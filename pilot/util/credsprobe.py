#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""TEMPORARY: probe the new PanDA credentials API at pilot start-up.

Why this module exists
----------------------
``ActiveMQ.get_credentials()`` in :mod:`pilot.util.activemq` still targets the
old dispatcher endpoint (``get_access_token`` under ``/server/panda/``) and
still assumes the old response shape. Three things must change:

1. the endpoint becomes ``api/v1/creds/get_user_secrets``. Note that the
   current code calls ``get_access_token`` while its own docstring says
   ``get_user_secrets``; the docstring was right, and under the new API
   ``get_access_token`` serves tokens rather than secrets;
2. the response becomes ``{"success": bool, "message": str, "data": {...}}``;
3. the transport becomes :func:`pilot.util.https.request2`. The current code
   calls :func:`pilot.util.https.request` (the curl fallback) and then indexes
   the result as ``res[0]``/``res[1]``, which cannot work: ``request()``
   returns a *dict*, so ``res[0]`` raises ``KeyError: 0``.

The endpoint is settled. Three points are not, and each is a dimension of the
probe:

- **Authentication.** Whether the call may simply carry
  ``{'get_json': True, 'keys': 'MB_USERNAME,MB_PASSWORD'}``, or whether it
  must additionally be authenticated with the OIDC token the way
  :func:`pilot.util.https.refresh_oidc_token` is (bearer token plus the
  ``client_name`` and ``token_key`` query parameters). Those two parameters
  belong to the token endpoint and are a long shot here, but they are cheap to
  probe and rule out the possibility that the new creds API applies one auth
  convention across all of its endpoints.
- **Method.** Whether the payload travels as query parameters on a ``GET`` or
  as a JSON body on a ``POST``.
- **Encoding.** ``request2()`` gzips bodies sent to ``api/v`` endpoints by
  default, which is a plausible rejection cause independent of authentication.

How it runs
-----------
:func:`run_probe` is called unconditionally from ``pilot.py``, once per pilot
process, immediately after the HTTPS setup. It walks the full matrix in a
single run, logs the outcome of every combination, and then lets the pilot
continue normally: no command-line option is involved, nothing is aborted and
no job is skipped. Deploy the pilot to a test queue, let one pilot run, and
read the matrix out of ``pilotlog.txt``.

Everything is written through the normal pilot logger, so the results come
back with the usual log upload.

Secret handling
---------------
``pilotlog.txt`` is uploaded to the log server, so nothing derived from a
response may reach it unmasked. Every logging site that touches the response
goes through :func:`safe_preview` or :func:`scrub_text` first, and the same
masking is applied inside :func:`parse_credentials_response` so that it stays
safe once it moves into :mod:`pilot.util.activemq`. Masking leaves the first
:data:`VISIBLE_PREFIX` characters visible - enough to confirm that the right
value came back - and preserves key names, since the shape of the response is
what the probe exists to discover. The only unmasked credential fact in the
log is the username and the password *length*, both reported explicitly on
success.

One leak path is outside this module: on an HTTP error, ``request2()`` logs
the server's response body. That body is an error document rather than a
successful secrets payload, so it should not carry credentials, but it is
worth a glance when reading the log.

Tuning
------
Because there are no command-line options, the constants below are the tuning
surface. Edit and redeploy:

- :data:`ENDPOINTS` - API paths to try; add ``api/v1/creds/get_access_token``
  back if the secrets turn out to be served from there after all
- :data:`VARIANTS` - request shapes to try
- :data:`KEYS` - secret names to request
- :data:`TIME_BUDGET` - wall-clock cap; remaining combinations are skipped
  once it is exceeded, so that an unreachable server cannot delay every pilot
  by the full per-request timeout multiplied by the size of the matrix

Caveats
-------
- The probe runs *before* :func:`pilot.util.https.update_local_oidc_token_info`,
  so it exercises the token the wrapper supplied. If every token variant fails
  with an authorisation error, check the token expiry before concluding that
  the endpoint rejects tokens.
- ``request2()`` disables OIDC entirely when ``PILOT_SITENAME`` contains
  ``CERN-PTEST``; the probe warns when that is the case, because the token
  variants would then silently run unauthenticated.
- The probe can never fail the pilot: :func:`run_probe` swallows every
  exception and returns ``None``.

Removal
-------
This module is diagnostic scaffolding. Once the correct combination is known,
:func:`parse_credentials_response` and :func:`get_credentials` move into
:mod:`pilot.util.activemq` and everything else goes away. To remove:

1. delete this file and ``pilot/test/test_credsprobe.py``;
2. delete the ``run_probe(args)`` call and its import in ``pilot.py``.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, NamedTuple

from pilot.util.https import (
    get_auth_token_content,
    get_local_oidc_token_info,
    get_server_command,
    request2,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# probe configuration (no command-line options - edit here and redeploy)
# --------------------------------------------------------------------------

# API paths to try, in order. Under the new API the message broker secrets are
# served by get_user_secrets; get_access_token serves tokens (see
# refresh_oidc_token()) and is deliberately not probed here.
ENDPOINTS = (
    "api/v1/creds/get_user_secrets",
)

# Secret names to request, username first.
KEYS = "MB_USERNAME,MB_PASSWORD"

# Value for the client_name query parameter, as used by refresh_oidc_token().
CLIENT_NAME = "pilot_server"

# Wall-clock cap in seconds for the whole matrix. Once exceeded, the remaining
# combinations are skipped so that an unreachable server cannot delay every
# pilot by the full per-request timeout multiplied by the size of the matrix.
TIME_BUDGET = 180

# Candidate names for the secrets container inside the new-style 'data' field.
SECRET_CONTAINER_KEYS = ("secrets", "user_secrets", "userSecrets", "values", "keys")

# Guard against a second invocation within the same pilot process.
_PROBE_HAS_RUN = False


class Variant(NamedTuple):
    """One request shape to probe against a credentials endpoint.

    Attributes:
        method: HTTP method.
        style: How the payload is carried: ``query`` or ``json``.
        use_token: Whether to authenticate with the OIDC token
            (``panda=True`` for ``request2()``).
        use_key: Whether to append ``client_name``/``token_key`` query
            parameters, as ``refresh_oidc_token()`` does.
        compressed: Whether to let ``request2()`` gzip the body. Irrelevant
            for the body-less ``GET`` variants.
        description: Human-readable summary written to the log.
    """

    method: str
    style: str
    use_token: bool
    use_key: bool
    compressed: bool
    description: str


VARIANTS: dict[str, Variant] = {
    "get-plain": Variant(
        "GET", "query", False, False, True,
        "GET, keys in query string, no token (simplest possible call)"),
    "get-token": Variant(
        "GET", "query", True, False, True,
        "GET, keys in query string, OIDC token"),
    "get-tokenkey": Variant(
        "GET", "query", True, True, True,
        "GET, keys + client_name/token_key in query, OIDC token (refresh_oidc_token() style)"),
    "post-plain": Variant(
        "POST", "json", False, False, True,
        "POST, keys in gzipped JSON body, no token"),
    "post-token": Variant(
        "POST", "json", True, False, True,
        "POST, keys in gzipped JSON body, OIDC token"),
    "post-tokenkey": Variant(
        "POST", "json", True, True, True,
        "POST, keys in gzipped JSON body, client_name/token_key in query, OIDC token"),
    "post-token-nogzip": Variant(
        "POST", "json", True, False, False,
        "POST, keys in plain JSON body, OIDC token (isolates gzip as a failure cause)"),
    "post-tokenkey-nogzip": Variant(
        "POST", "json", True, True, False,
        "POST, keys in plain JSON body, client_name/token_key in query, OIDC token"),
}


# --------------------------------------------------------------------------
# redaction - nothing derived from a response reaches the log unmasked
# --------------------------------------------------------------------------

# Substrings that mark a field name as carrying a secret. Matched
# case-insensitively against the whole field name, so MB_PASSWORD, userSecret
# and access_token are all caught.
SECRET_KEY_MARKERS = ("password", "passwd", "pwd", "secret", "token", "credential")

# Number of leading characters left visible when masking, enough to confirm
# that the right value came back without disclosing it.
VISIBLE_PREFIX = 2


def mask_secret(value: Any) -> str:
    """Mask a secret value, leaving only a short leading fragment visible.

    Args:
        value: The value to mask.

    Returns:
        The first :data:`VISIBLE_PREFIX` characters followed by an ellipsis,
        or a bare ellipsis when the value is too short for a prefix to be
        safe.
    """
    text = str(value)
    if len(text) <= VISIBLE_PREFIX + 1:
        return "......"

    return f"{text[:VISIBLE_PREFIX]}......"


def _is_secret_key(name: Any, password_key: str) -> bool:
    """Report whether a field name denotes a secret.

    Args:
        name: Field name from a response.
        password_key: The password field name currently being requested, which
            counts as a secret even if it contains none of the markers.

    Returns:
        ``True`` when the field value must be masked before logging.
    """
    lowered = str(name).lower()
    if password_key and lowered == password_key.lower():
        return True

    return any(marker in lowered for marker in SECRET_KEY_MARKERS)


def redact(value: Any, password_key: str = "", inside_secret: bool = False) -> Any:
    """Recursively mask secret values in a parsed response.

    A field is masked when its own name denotes a secret, and every scalar
    inside a secret-named container is masked regardless of its own name, so
    that a shape such as ``{"credentials": ["user", "pass"]}`` cannot leak.
    Key names are always preserved: the structure of the response is what the
    probe exists to discover, and only the values are sensitive.

    Also descends into JSON-encoded strings, since the ``data`` field may carry
    the secrets as a string rather than as a nested object.

    Args:
        value: Any part of a parsed response.
        password_key: The password field name currently being requested.
        inside_secret: ``True`` when the value sits inside a container whose
            name denotes a secret, in which case every scalar below it is
            masked.

    Returns:
        A copy of the value with every secret masked.
    """
    if isinstance(value, dict):
        redacted = {}
        for key, item in value.items():
            secret_here = inside_secret or _is_secret_key(key, password_key)
            redacted[key] = redact(item, password_key, secret_here)
        return redacted

    if isinstance(value, list):
        return [redact(item, password_key, inside_secret) for item in value]

    if isinstance(value, str):
        decoded = _coerce_mapping(value)
        if decoded:
            return json.dumps(redact(decoded, password_key, inside_secret))

    if inside_secret:
        return mask_secret(value)

    return value


def scrub_text(text: str, password_key: str = "") -> str:
    """Mask secrets in text that could not be parsed as JSON.

    Covers both ``"KEY": "value"`` and ``KEY=value`` forms, so an HTML error
    page or a query-string echo cannot leak a secret either.

    Args:
        text: Raw response text.
        password_key: The password field name currently being requested.

    Returns:
        The text with any secret-looking value masked.
    """
    markers = [re.escape(marker) for marker in SECRET_KEY_MARKERS]
    if password_key:
        markers.append(re.escape(password_key))
    pattern = "|".join(markers)

    text = re.sub(
        rf'("[^"]*(?:{pattern})[^"]*"\s*:\s*")([^"]*)(")',
        lambda match: match.group(1) + mask_secret(match.group(2)) + match.group(3),
        text,
        flags=re.IGNORECASE,
    )

    return re.sub(
        rf'([A-Za-z0-9_]*(?:{pattern})[A-Za-z0-9_]*=)([^&\s"]+)',
        lambda match: match.group(1) + mask_secret(match.group(2)),
        text,
        flags=re.IGNORECASE,
    )


def safe_preview(payload: Any, keys: str = "") -> str:
    """Render a response as text that is safe to write to the log.

    Args:
        payload: Response value, parsed or raw.
        keys: Comma-separated secret names, username first; the second name is
            treated as the password field.

    Returns:
        A loggable string with every secret masked.
    """
    _, _, password_key = keys.partition(",")
    password_key = password_key.strip()

    if isinstance(payload, (dict, list)):
        try:
            return json.dumps(redact(payload, password_key), default=str)
        except (TypeError, ValueError):
            return scrub_text(str(payload), password_key)

    return scrub_text(str(payload), password_key)


# --------------------------------------------------------------------------
# response parsing - this is the part destined for activemq.py
# --------------------------------------------------------------------------

def _coerce_mapping(value: Any) -> dict[str, Any]:
    """Coerce a value into a dictionary where possible.

    Handles the ``get_json`` case where the server may return the secrets as a
    JSON-encoded *string* rather than as a nested object.

    Args:
        value: Candidate value from the response payload.

    Returns:
        A dictionary, or an empty dictionary if the value cannot be coerced.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (ValueError, TypeError):
            return {}
        if isinstance(decoded, dict):
            return decoded

    return {}


def extract_secrets(data: Any) -> dict[str, Any]:
    """Locate the secrets mapping inside the ``data`` field of a response.

    The secrets may sit directly in ``data``, inside a nested container such
    as ``data['secrets']``, or be JSON-encoded as a string at either level.

    Args:
        data: The ``data`` value from a new-style response envelope.

    Returns:
        Mapping of secret name to value; empty when nothing usable is found.
    """
    mapping = _coerce_mapping(data)
    if not mapping:
        return {}

    for key in SECRET_CONTAINER_KEYS:
        if key in mapping:
            nested = _coerce_mapping(mapping[key])
            if nested:
                return nested

    return mapping


def _as_dict(payload: Any) -> dict[str, Any]:
    """Normalise a server response into a dictionary.

    ``request2()`` usually returns a dict already, but falls back to raw text
    when the response is not parseable, so both are accepted.

    Args:
        payload: Response bytes, text, or an already-parsed dictionary.

    Returns:
        The parsed response dictionary.

    Raises:
        ValueError: If the payload is not valid JSON or is not a dictionary.
    """
    if isinstance(payload, (bytes, str)):
        text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
        try:
            payload = json.loads(text)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"response is not valid JSON: {exc}: {scrub_text(text[:200])!r}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"unexpected response type: {type(payload).__name__}")

    return payload


def _unwrap_envelope(payload: dict[str, Any]) -> Any:
    """Strip the status envelope and return the value carrying the secrets.

    Recognises the new-style ``success``/``message``/``data`` envelope and the
    legacy ``StatusCode``/``ErrorDialog`` envelope; anything else is assumed to
    already be the secrets mapping.

    Args:
        payload: Parsed response dictionary.

    Returns:
        The inner value that should contain the secrets.

    Raises:
        ValueError: If the envelope reports a server-side failure.
    """
    if "success" in payload:
        if payload.get("success") is not True:
            raise ValueError(f"server returned success=False: {payload.get('message', 'no message')!r}")
        return payload.get("data")

    if "StatusCode" in payload:
        statuscode = payload.get("StatusCode", 0)
        if str(statuscode) != "0":
            raise ValueError(f"server returned StatusCode={statuscode}: {payload.get('ErrorDialog', '')!r}")
        return payload.get("data", payload)

    return payload


def parse_credentials_response(payload: Any, keys: str = KEYS) -> tuple[str, str]:
    """Extract the message broker username and password from a server response.

    Supports both response shapes:

    - New: ``{"success": true, "message": "...", "data": {"MB_USERNAME": ...}}``
    - Legacy: a bare ``{"MB_USERNAME": ..., "MB_PASSWORD": ...}`` dict, or one
      wrapped in ``{"StatusCode": 0, ...}``.

    Args:
        payload: Response as returned by :func:`pilot.util.https.request2`
            (normally a ``dict``), or raw response text.
        keys: Comma-separated secret names, username first.

    Returns:
        A two-element tuple ``(username, password)``.

    Raises:
        ValueError: If the response is unparseable, reports failure, or does
            not contain both requested keys.
    """
    envelope = _as_dict(payload)
    secrets = extract_secrets(_unwrap_envelope(envelope))

    if not secrets:
        raise ValueError(f"no secrets found in response: {safe_preview(envelope, keys)[:300]}")

    user_key, _, pass_key = keys.partition(",")
    try:
        username = str(secrets[user_key.strip()])
        password = str(secrets[pass_key.strip()])
    except KeyError as exc:
        raise ValueError(f"missing key {exc} in secrets (available: {sorted(secrets)})") from exc

    if not username or not password:
        raise ValueError("username and/or password is empty")

    return username, password


# --------------------------------------------------------------------------
# request construction
# --------------------------------------------------------------------------

def get_token_key() -> str:
    """Read the PanDA token key referenced by ``PANDA_AUTH_TOKEN_KEY``.

    Returns:
        The token key content, or an empty string when the variable is unset
        or the file cannot be read.
    """
    token_key_name = os.environ.get("PANDA_AUTH_TOKEN_KEY")
    if not token_key_name:
        return ""

    return get_auth_token_content(token_key_name, key=True)


def build_params(spec: Variant, keys: str, token_key: str) -> dict[str, str]:
    """Build the query parameters for a variant.

    Note the lowercase string ``'true'``: :func:`pilot.util.https._merge_query`
    renders query values with ``str()``, so a Python ``True`` would reach the
    server as ``"True"``.

    Args:
        spec: Variant specification.
        keys: Comma-separated secret names.
        token_key: PanDA token key content.

    Returns:
        Query parameter dictionary, possibly empty.
    """
    params: dict[str, str] = {}
    if spec.use_key:
        params["client_name"] = CLIENT_NAME
        params["token_key"] = token_key
    if spec.style == "query":
        params["get_json"] = "true"
        params["keys"] = keys

    return params


def build_body(spec: Variant, keys: str) -> dict[str, Any] | None:
    """Build the JSON body for a variant.

    Args:
        spec: Variant specification.
        keys: Comma-separated secret names.

    Returns:
        The body dictionary, or ``None`` for variants that carry no JSON body.
    """
    if spec.style != "json":
        return None

    return {"get_json": True, "keys": keys}


def check_prerequisites(spec: Variant, token: str, token_key: str) -> str:
    """Report why a variant cannot be run, if it cannot.

    Args:
        spec: Variant specification.
        token: OIDC token content.
        token_key: PanDA token key content.

    Returns:
        A reason string, or an empty string when the variant can run.
    """
    if spec.use_token and not token:
        return "requires an OIDC token but none could be read"
    if spec.use_key and not token_key:
        return "requires a token key but PANDA_AUTH_TOKEN_KEY is unset or unreadable"

    return ""


def _hide_key(params: dict[str, str]) -> dict[str, str]:
    """Return a copy of the query parameters with the token key redacted.

    Args:
        params: Query parameter dictionary.

    Returns:
        Copy that is safe to write to the log.
    """
    safe = dict(params)
    if "token_key" in safe:
        safe["token_key"] = "<hidden>"

    return safe


# --------------------------------------------------------------------------
# transport
# --------------------------------------------------------------------------

def send(spec: Variant, url: str, port: int, *, endpoint: str = ENDPOINTS[0],
         keys: str = KEYS) -> tuple[bool, Any]:
    """Send one variant through :func:`pilot.util.https.request2`.

    Args:
        spec: Variant specification.
        url: PanDA server URL.
        port: PanDA server port.
        endpoint: API path to call.
        keys: Comma-separated secret names.

    Returns:
        A two-element tuple ``(ok, payload)``. On failure *ok* is ``False`` and
        *payload* is a description of what went wrong.
    """
    cmd = get_server_command(url, port, cmd=endpoint)
    params = build_params(spec, keys, get_token_key())
    body = build_body(spec, keys)

    logger.info(
        f"request2({cmd!r}, params={_hide_key(params)}, json_body={body}, "
        f"panda={spec.use_token}, compressed={spec.compressed}, method={spec.method!r})"
    )

    try:
        res = request2(
            cmd,
            params=params or None,
            json_body=body,
            compressed=spec.compressed,
            panda=spec.use_token,
            method=spec.method,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return False, f"{type(exc).__name__}: {exc}"

    # request2() signals transport failures with a marker string, and an
    # unreadable token with an empty string
    if isinstance(res, str):
        if res.startswith("failed to send request"):
            return False, res
        if not res:
            return False, "request2() returned an empty string (token content unreadable?)"

    return True, res


# --------------------------------------------------------------------------
# candidate replacement for ActiveMQ.get_credentials()
# --------------------------------------------------------------------------

def get_credentials(url: str, port: int, variant: str = "get-token", *,
                    endpoint: str = ENDPOINTS[0], keys: str = KEYS) -> tuple[str, str]:
    """Fetch message broker credentials from the PanDA server.

    This is the candidate replacement for ``ActiveMQ.get_credentials()``. Once
    the probe has established which combination works, this function and
    :func:`parse_credentials_response` move into :mod:`pilot.util.activemq`
    and the surrounding probe machinery is deleted.

    Args:
        url: PanDA server URL.
        port: PanDA server port.
        variant: Request variant to use; see :data:`VARIANTS`.
        endpoint: API path to call.
        keys: Comma-separated secret names, username first.

    Returns:
        A two-element tuple ``(username, password)``; both empty on failure.
    """
    spec = VARIANTS.get(variant)
    if spec is None:
        logger.warning(f"unknown variant: {variant}")
        return "", ""

    ok, payload = send(spec, url, port, endpoint=endpoint, keys=keys)
    if not ok:
        logger.warning(f"failed to get credentials: {scrub_text(str(payload))}")
        return "", ""

    try:
        return parse_credentials_response(payload, keys=keys)
    except ValueError as exc:
        logger.warning(f"failed to extract credentials: {scrub_text(str(exc))}")
        return "", ""


# --------------------------------------------------------------------------
# probe driver
# --------------------------------------------------------------------------

def log_environment(token: str, origin: str | None, token_key: str) -> None:
    """Write the authentication context to the log before probing.

    Args:
        token: OIDC token content.
        origin: Token origin string.
        token_key: PanDA token key content.
    """
    auth_token_name, _ = get_local_oidc_token_info()
    logger.info("=" * 78)
    logger.info("credentials API probe - authentication context")
    logger.info(f"token name:  {auth_token_name or 'NOT SET'}")
    logger.info(f"token:       {'read, length ' + str(len(token)) if token else 'NOT AVAILABLE'}")
    logger.info(f"origin:      {origin or 'NOT SET'}")
    logger.info(f"token key:   {'read, length ' + str(len(token_key)) if token_key else 'NOT AVAILABLE'}")

    sitename = os.environ.get("PILOT_SITENAME", "")
    if "CERN-PTEST" in sitename:
        logger.warning(
            f"PILOT_SITENAME={sitename!r} contains CERN-PTEST: request2() disables OIDC tokens "
            "for this site, so the token variants will silently run unauthenticated"
        )


def probe_variant(name: str, url: str, port: int, *, endpoint: str,
                  token: str, token_key: str) -> tuple[bool, str]:
    """Run a single variant against a single endpoint and describe the outcome.

    Args:
        name: Variant name.
        url: PanDA server URL.
        port: PanDA server port.
        endpoint: API path to call.
        token: OIDC token content.
        token_key: PanDA token key content.

    Returns:
        A two-element tuple ``(success, summary)``.
    """
    spec = VARIANTS[name]
    logger.info("-" * 78)
    logger.info(f"variant {name}: {spec.description}")

    reason = check_prerequisites(spec, token, token_key)
    if reason:
        logger.warning(f"skipped: {reason}")
        return False, f"skipped ({reason})"

    ok, payload = send(spec, url, port, endpoint=endpoint, keys=KEYS)
    if not ok:
        detail = scrub_text(str(payload))
        logger.warning(f"failed: {detail}")
        return False, detail[:150]

    logger.info(f"response: {safe_preview(payload, KEYS)[:2000]}")

    try:
        username, password = parse_credentials_response(payload, keys=KEYS)
    except ValueError as exc:
        detail = scrub_text(str(exc))
        logger.warning(f"reached server but parse failed: {detail}")
        return False, f"parse failed: {detail}"

    logger.info(f"credentials extracted: username={username!r}, password length={len(password)}")

    return True, "OK"


def run_matrix(url: str, port: int, token: str, token_key: str) -> list[tuple[str, str, bool, str]]:
    """Walk the full endpoint/variant matrix within the time budget.

    Args:
        url: PanDA server URL.
        port: PanDA server port.
        token: OIDC token content.
        token_key: PanDA token key content.

    Returns:
        List of ``(endpoint, variant, success, summary)`` tuples, one per
        combination attempted or skipped.
    """
    results: list[tuple[str, str, bool, str]] = []
    deadline = time.time() + TIME_BUDGET

    for endpoint in ENDPOINTS:
        logger.info("=" * 78)
        logger.info(f"endpoint: {get_server_command(url, port, cmd=endpoint)}")
        for name in VARIANTS:
            if time.time() > deadline:
                logger.warning(f"time budget of {TIME_BUDGET}s exceeded - skipping {endpoint}/{name}")
                results.append((endpoint, name, False, "skipped (time budget exceeded)"))
                continue
            success, summary = probe_variant(
                name, url, port, endpoint=endpoint, token=token, token_key=token_key
            )
            results.append((endpoint, name, success, summary))

    return results


def log_summary(results: list[tuple[str, str, bool, str]]) -> None:
    """Write the PASS/FAIL matrix to the log.

    Args:
        results: Output of :func:`run_matrix`.
    """
    endpoint_width = max(len(endpoint) for endpoint, _, _, _ in results)
    variant_width = max(len(name) for _, name, _, _ in results)

    logger.info("=" * 78)
    logger.info("credentials API probe - summary")
    for endpoint, name, success, summary in results:
        logger.info(
            f"  {'PASS' if success else 'FAIL'}  {endpoint:<{endpoint_width}}  "
            f"{name:<{variant_width}}  {summary}"
        )
    logger.info("=" * 78)

    passes = [f"{endpoint} {name}" for endpoint, name, success, _ in results if success]
    if passes:
        logger.info(f"working combinations: {', '.join(passes)}")
    else:
        logger.warning("no combination succeeded - see the responses above for the server diagnostics")


def run_probe(args: Any) -> None:
    """Probe the credentials API across the full matrix and log the result.

    Called unconditionally from ``pilot.py`` at start-up. Runs once per pilot
    process, never raises, and never changes the pilot's behaviour: the pilot
    continues with normal operation afterwards.

    Args:
        args: The pilot arguments object. Only ``url`` and ``port`` are used.
    """
    global _PROBE_HAS_RUN  # pylint: disable=global-statement
    if _PROBE_HAS_RUN:
        logger.debug("credentials API probe has already run in this pilot process")
        return
    _PROBE_HAS_RUN = True

    try:
        auth_token_name, origin = get_local_oidc_token_info()
        token = get_auth_token_content(auth_token_name) if auth_token_name else ""
        token_key = get_token_key()

        log_environment(token, origin, token_key)
        logger.info(f"keys:        {KEYS}")
        logger.info(f"matrix:      {len(ENDPOINTS)} endpoint(s) x {len(VARIANTS)} variants "
                    f"= {len(ENDPOINTS) * len(VARIANTS)} requests, time budget {TIME_BUDGET}s")

        started = time.time()
        results = run_matrix(args.url, args.port, token, token_key)
        log_summary(results)
        logger.info(f"credentials API probe finished in {time.time() - started:.1f}s")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        # the probe is diagnostic scaffolding and must never affect the pilot
        logger.warning(f"credentials API probe failed unexpectedly (ignored): {exc}", exc_info=True)
