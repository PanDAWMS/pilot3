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

"""OIDC token lifecycle: refresh scheduling, lifetime gate before getJob and exit-code handling.

Background (PanDA ID 7367714395): the token downloaded at pilot startup had 891 s left, the pilot
took a job anyway, every server update after the token expired was rejected with 403, the hourly
refresh was rejected as well (it authenticated with the expired token), and the pilot exited 0
although the server never received the final update.

This module schedules the token refresh ahead of the token's actual expiry rather than on a fixed
hourly clock, prevents taking a job with a token that will not outlast the next heartbeat (unless
X.509 failover is available), and turns a final server update that failed because the token was
rejected into a non-zero pilot exit code. The X.509 failover itself is implemented in
:mod:`pilot.util.https`.
"""

from __future__ import annotations

import logging
from time import time

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
from pilot.util.config import config
from pilot.util.https import (
    get_token_expiry,
    update_local_oidc_token_info,
    x509_failover_available,
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()
pilot_cache = get_pilot_cache()

# defaults used when the [Token] section of the config file lacks the corresponding setting
DEFAULT_DOWNLOAD_CHECK = 3600  # maximum time between token refreshes (s)
DEFAULT_REFRESH_MARGIN = 900  # refresh this long before the token expires (s)
DEFAULT_REFRESH_RETRY = 300  # retry interval when no sufficiently long-lived token was obtained (s)
DEFAULT_HEARTBEAT = 1800  # heartbeat interval (s), see [Pilot] heartbeat

# a heartbeat is not sent at exactly the heartbeat interval (e.g. 1810 s after getJob for
# PanDA ID 7367714395), so the token must outlast the interval by this much
HEARTBEAT_GRACE = 120


def _get_config_int(section: str, name: str, default: int) -> int:
    """Return an integer setting from the pilot config, or a default.

    Args:
        section: Config section name (e.g. ``'Token'``).
        name: Setting name.
        default: Value to use when the setting is missing or invalid.

    Returns:
        The setting as an int, or *default*.
    """
    try:
        return int(getattr(getattr(config, section), name))
    except (AttributeError, ValueError, TypeError):
        return default


def compute_next_refresh(now: float, expiry: int | None, interval: int, margin: int, retry: int) -> float:
    """Return the epoch time at which the token should next be refreshed.

    The token is refreshed at the latest *interval* seconds from now, and earlier if it expires
    within *interval* + *margin* seconds, namely *margin* seconds before it expires. A token that
    already expires within *margin* seconds (e.g. because the server returned a token with only
    891 s left) is retried every *retry* seconds.

    Args:
        now: Current epoch time.
        expiry: Token expiry (epoch seconds), or ``None`` if unknown.
        interval: Maximum time between refreshes (s).
        margin: Refresh this long before the token expires (s).
        retry: Retry interval when the token expires within *margin* (s).

    Returns:
        Epoch time of the next refresh.
    """
    if not expiry:
        return now + interval

    deadline = expiry - margin
    if deadline > now:
        return min(now + interval, deadline)

    return now + retry


def schedule_next_refresh(expiry: int | None, now: float | None = None) -> float:
    """Schedule the next token refresh based on the token expiry and the [Token] settings.

    Args:
        expiry: Token expiry (epoch seconds), or ``None`` if unknown.
        now: Current epoch time (defaults to the current time).

    Returns:
        Epoch time of the next refresh (also stored in the pilot cache).
    """
    now = time() if now is None else now
    next_refresh = compute_next_refresh(
        now,
        expiry,
        _get_config_int('Token', 'download_check', DEFAULT_DOWNLOAD_CHECK),
        _get_config_int('Token', 'refresh_margin', DEFAULT_REFRESH_MARGIN),
        _get_config_int('Token', 'refresh_retry', DEFAULT_REFRESH_RETRY),
    )
    pilot_cache.token_next_refresh = next_refresh
    logger.info(f"next token refresh in {int(next_refresh - now)} s")

    return next_refresh


def is_refresh_due(last_check: float, interval: int, now: float | None = None) -> bool:
    """Return ``True`` if the token should be refreshed now.

    Uses the schedule set by :func:`schedule_next_refresh`; until a refresh has been scheduled
    (e.g. when token renewal is disabled), falls back to a refresh every *interval* seconds.

    Args:
        last_check: Epoch time of the last refresh check.
        interval: Fallback interval between refresh checks (s).
        now: Current epoch time (defaults to the current time).

    Returns:
        ``True`` if a refresh is due.
    """
    now = time() if now is None else now
    if pilot_cache.token_next_refresh:
        return now >= pilot_cache.token_next_refresh

    return now - last_check > interval


def refresh_token(url: str, port: int) -> bool:
    """Refresh the OIDC token and schedule the next refresh from its expiry.

    A refresh that returns a token which does not expire later than the current one is reported
    as such (the server may hand out a cached token close to expiry); the schedule then retries
    sooner. When a newer token is obtained while X.509 authentication is in use after an earlier
    token rejection, the pilot switches back to token authentication.

    Args:
        url: PanDA server base URL.
        port: PanDA server port.

    Returns:
        ``True`` if a token was downloaded, ``False`` otherwise.
    """
    previous = get_token_expiry()
    status = False
    try:
        status = update_local_oidc_token_info(url, port)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f"failed to update local OIDC token: {exc}")

    expiry = get_token_expiry()
    if status and expiry:
        if previous and expiry <= previous:
            logger.warning(f"the server did not return a newer token (expiry unchanged: {expiry}) - "
                           f"token remaining lifetime: {expiry - int(time())} s")
        elif pilot_cache.token_x509_mode and expiry > time():
            logger.info("a newer token has been downloaded - will use token authentication again")
            pilot_cache.token_x509_mode = False

    # always reschedule, so that a failed refresh is not retried every monitoring cycle
    schedule_next_refresh(expiry)

    return status


def get_token_remaining_lifetime() -> int | None:
    """Return the remaining lifetime of the local OIDC token.

    Returns:
        Remaining lifetime in seconds (negative once expired), or ``None`` if no token is
        configured or its expiry is unknown.
    """
    expiry = get_token_expiry()
    if expiry is None:
        return None

    return expiry - int(time())


def get_required_token_lifetime() -> int:
    """Return the minimum token lifetime needed to take a new job.

    The token must outlast the next heartbeat, i.e. the first server update after getJob.

    Returns:
        Required remaining lifetime in seconds.
    """
    return _get_config_int('Pilot', 'heartbeat', DEFAULT_HEARTBEAT) + HEARTBEAT_GRACE


def check_token_for_new_job(url: str, port: int, renewal_allowed: bool) -> tuple[bool, str]:
    """Decide whether the token allows the pilot to take a new job.

    If the token will not outlast the next heartbeat, a refresh is attempted first (when renewal
    is allowed). If the token is still too short, the pilot may proceed only if X.509 failover is
    available for at least as long.

    Args:
        url: PanDA server base URL.
        port: PanDA server port.
        renewal_allowed: ``False`` if the pilot must not renew the token (``no_token_renewal``).

    Returns:
        A tuple ``(proceed, diagnostics)`` where *diagnostics* explains a refusal.
    """
    remaining = get_token_remaining_lifetime()
    if remaining is None:
        logger.debug("no token lifetime available - token lifetime check skipped")
        return True, ""

    required = get_required_token_lifetime()
    if remaining >= required:
        logger.info(f"token remaining lifetime ({remaining} s) is sufficient for a new job (required: {required} s)")
        return True, ""

    logger.warning(f"token remaining lifetime ({remaining} s) is shorter than required for a new job ({required} s)")
    if renewal_allowed:
        refresh_token(url, port)
        remaining = get_token_remaining_lifetime()
        if remaining is not None and remaining >= required:
            logger.info(f"token remaining lifetime after refresh: {remaining} s - sufficient for a new job")
            return True, ""

    if x509_failover_available(required):
        logger.warning(f"token remaining lifetime ({remaining} s) is too short, but X.509 failover is available "
                       f"- will proceed with a new job")
        return True, ""

    return False, (f"token remaining lifetime ({remaining} s) is shorter than required ({required} s) and "
                   f"no X.509 failover is available - will not fetch a new job")


def record_final_update(jobid: str, ok: bool) -> None:
    """Keep track of final server updates that failed because the server rejected the authentication.

    Args:
        jobid: PanDA job id.
        ok: ``True`` if the final update was accepted.
    """
    if ok:
        pilot_cache.token_failed_final_updates.discard(str(jobid))
    elif pilot_cache.token_rejected:
        logger.warning(f"the final server update for job {jobid} failed since the server rejected the authentication "
                       f"- the pilot will exit with error code {errors.TOKENTOOSHORT}")
        pilot_cache.token_failed_final_updates.add(str(jobid))


def apply_token_exit_code(exitcode: int) -> int:
    """Return the pilot exit code, accounting for final server updates lost to a rejected token.

    A pilot that otherwise succeeded must not exit with 0 if the server never received a final
    update because it rejected the authentication.

    Args:
        exitcode: Pilot exit code determined so far.

    Returns:
        :data:`ErrorCodes.TOKENTOOSHORT` if *exitcode* is 0 and a final update was lost to a
        rejected token, otherwise *exitcode* unchanged.
    """
    if exitcode == 0 and pilot_cache.token_failed_final_updates:
        jobids = ", ".join(sorted(pilot_cache.token_failed_final_updates))
        logger.warning(f"final server update failed for job(s) {jobids} since the token was rejected - "
                       f"exit code set to {errors.TOKENTOOSHORT}")
        return errors.TOKENTOOSHORT

    return exitcode
