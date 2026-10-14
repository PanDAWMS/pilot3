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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-26
# - Alexander Bogdanchikov, alexander.bogdanchikov@cern.ch, 2020

"""Functions related to proxy handling for ATLAS."""

from __future__ import annotations
import logging
import os
import threading

from time import time
from typing import (
    Any,
    Optional,
)

# from pilot.user.atlas.setup import get_file_system_root_path
from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
from pilot.util.config import config
from pilot.util.container import (
    execute,
    execute_nothreads
)
from pilot.util.filehandling import remove
from pilot.util.proxy import (
    JOB_PROXY_TYPES,
    get_job_proxy_path,
    get_proxy
)

errors = ErrorCodes()
logger = logging.getLogger(__name__)
pilot_cache = get_pilot_cache()

# TEST-ONLY: when True, the validity of a job proxy (payload proxy or unified dispatch user proxy) is clamped
# to JOB_PROXY_RENEWAL_THRESHOLD + TEST_JOB_PROXY_MARGIN from the moment it is verified, so that the renewal
# mechanism is triggered TEST_JOB_PROXY_MARGIN after each download within a short test job (use together with
# a short proxy_verification_time in pilot/util/default.cfg). Must be False in any release (guarded by
# test_job_proxy_renewal.py).
_TEST_JOB_PROXY_RENEWAL = False
TEST_JOB_PROXY_MARGIN = 300

# a job proxy is renewed once its remaining validity drops below this (s)
JOB_PROXY_RENEWAL_THRESHOLD = 24 * 3600

# a job proxy that cannot be renewed only fails the job once its remaining validity drops below this (s)
JOB_PROXY_HARD_FLOOR = 600

# minimum remaining validity of a proxy served by the PanDA server (s) and the grace allowed for it
JOB_PROXY_EXPECTED_LIFETIME = 72 * 3600
JOB_PROXY_LIFETIME_GRACE = 20 * 60

# after a renewal that did not solve the problem (the new proxy did not last longer than the current one,
# or is still below the renewal threshold), wait this long before the next attempt (s) - except during the
# last backoff interval before JOB_PROXY_HARD_FLOOR is reached, when every check attempts a renewal
JOB_PROXY_RENEWAL_BACKOFF = 3600

# earliest time (epoch seconds) of the next renewal attempt, per job proxy id
_renewal_backoff: dict[str, int] = {}

# serialises job proxy renewals between the job monitor and the stage-out threads
_renewal_lock = threading.Lock()


def get_voms_role(role: str = 'production') -> str:
    """Return the proper voms role.

    Args:
        role: proxy role, 'production' or 'user'.

    Returns:
        str: voms role.
    """
    return 'atlas:/atlas/Role=production' if role == 'production' else 'atlas'


def get_and_verify_proxy(x509: str, voms_role: str = '', proxy_type: str = '', workdir: str = '') -> tuple[int, str, str]:
    """Download a payload proxy from the server and verify it.

    Args:
        x509: X509_USER_PROXY.
        voms_role: role, e.g. 'atlas' for user jobs in unified dispatch, 'atlas:/atlas/Role=production' for production jobs.
        proxy_type: proxy type ('unified' on unified dispatch queues, otherwise blank).
        workdir: payload work directory.

    Returns:
        tuple[int, str, str]: exit code, diagnostics, updated x509.
    """
    exit_code = 0
    diagnostics = ""

    x509_payload = get_job_proxy_path(x509, proxy_type, workdir=workdir) if proxy_type else x509

    # Job proxies are verified under their own cache entry, so that the periodic checks during the job
    # can be served from the cache without executing arcproxy again (see check_job_proxy()). Any entry
    # left from a previous job or an earlier download must be cleared first, since verify_arcproxy()
    # would otherwise judge the new proxy by the validity of the old one.
    proxy_id = proxy_type if proxy_type in JOB_PROXY_TYPES else None
    if proxy_id:
        invalidate_proxy_cache(proxy_id)
        clear_renewal_backoff(proxy_id)

    # try to receive payload proxy and update x509
    logger.info(f"download proxy from server (type=\'{proxy_type}\', x509_payload={x509_payload})")
    res, x509_payload = get_proxy(x509_payload, voms_role)  # note that x509_payload might be updated
    logger.debug(f'get_proxy() returned {x509_payload}')
    if not res:
        diagnostics = f"failed to download proxy from server for role='{voms_role}'"
        logger.warning(diagnostics)
        return errors.PAYLOADPROXYDOWNLOADFAILURE, diagnostics, x509

    logger.debug("server returned proxy (verifying)")
    exit_code, diagnostics = verify_proxy(x509=x509_payload, proxy_id=proxy_id, test=False)

    # verify_arcproxy() returns -1 when arcproxy itself is unavailable on the queue. That says
    # nothing about the downloaded proxy, so it must not be reported as an error code (-1 has no
    # entry in ErrorCodes._error_messages) and must not fail the job. The proxy was downloaded
    # successfully, so use it unverified.
    if exit_code == -1:
        logger.warning(f"cannot verify downloaded proxy ({diagnostics}) - will use it unverified")
        return 0, "", x509_payload

    # If all verifications fail, verify_proxy() returns exit_code=0 with the last failure in
    # diagnostics. That must still be treated as a failure: previously exit_code=0 was returned
    # while x509 was left unchanged, so the caller silently continued with the pilot's own proxy.
    if exit_code != 0 or diagnostics != "":
        logger.warning(diagnostics)
        logger.info(f"proxy verification failed (proxy type=\'{proxy_type}\')")
        return exit_code if exit_code != 0 else errors.NOVOMSPROXY, diagnostics, x509

    logger.info(f"proxy verified (proxy type=\'{proxy_type}\')")
    if proxy_id:
        check_downloaded_lifetime(proxy_id)

    return 0, "", x509_payload


def requires_payload_proxy(job: Any) -> bool:
    """Determine whether a payload proxy should be downloaded for the given job.

    The payload proxy is only ever applied to the container setup command built by
    ``alrb_wrapper()``, so the conditions here must match those under which that command is
    built - otherwise jobs that would never use a payload proxy would be failed for not being
    able to download one.

    Args:
        job: job object.

    Returns:
        bool: True if a payload proxy is required.
    """
    x509 = os.environ.get("X509_UNIFIED_DISPATCH") or os.environ.get("X509_USER_PROXY", "")
    if not x509:
        logger.debug("no X509_USER_PROXY set - no payload proxy required")
        return False

    proxy_verification = (os.environ.get("PILOT_PROXY_VERIFICATION") == "True" and
                          os.environ.get("PILOT_PAYLOAD_PROXY_VERIFICATION") == "True")
    if not (proxy_verification and config.Pilot.payload_proxy_from_server):
        logger.debug(f"no payload proxy required (proxy_verification={proxy_verification}, "
                     f"payload_proxy_from_server={config.Pilot.payload_proxy_from_server})")
        return False

    if not job.is_analysis():
        logger.debug("no payload proxy required for production jobs")
        return False

    queuedata = job.infosys.queuedata
    if queuedata.type == "unified":
        # on unified dispatch queues the user proxy is downloaded by handle_proxy() instead
        logger.debug("no payload proxy required on unified dispatch queues")
        return False

    if not queuedata.container_type.get("pilot"):
        # without a pilot container there is no container setup command to add the proxy to
        logger.debug("no payload proxy required since the queue does not use a pilot container")
        return False

    return True


def handle_payload_proxy(job: Any) -> tuple[int, str]:
    """Download and verify the payload proxy for the given job, if one is required.

    On success the resolved proxy path is stored in the pilot cache, where
    ``update_for_user_proxy()`` picks it up when the container setup command is built.

    This is deliberately done once per job during job validation rather than inside
    ``alrb_wrapper()``: that function is a command-string builder invoked from
    ``pilot.util.container.execute()``, so it ran the download twice per job (once for the setup
    verification and once for the payload) and was reached too late for the job to be failed
    cleanly - the payload used to run with the pilot's own proxy instead.

    Args:
        job: job object.

    Returns:
        tuple[int, str]: exit code (0 on success or if no payload proxy is required), diagnostics.
    """
    # never let a payload proxy from a previous job be used by this one
    pilot_cache.payload_proxy = None

    if not requires_payload_proxy(job):
        return 0, ""

    # the payload proxy is stored in the work directory, which is visible as /srv inside the payload
    # container - required for a renewed proxy to be seen by the running payload (see check_job_proxy())
    x509 = os.environ.get("X509_UNIFIED_DISPATCH") or os.environ.get("X509_USER_PROXY", "")
    voms_role = get_voms_role(role="user")
    exit_code, diagnostics, x509_payload = get_and_verify_proxy(
        x509, voms_role=voms_role, proxy_type="payload", workdir=job.workdir
    )
    if exit_code:
        return exit_code, diagnostics

    pilot_cache.payload_proxy = x509_payload
    logger.info(f"payload proxy is ready: {x509_payload}")

    return 0, ""


def verify_proxy(limit: int = None, x509: bool = None, proxy_id: str = "pilot", test: bool = False, pilotstartup: bool = False) -> tuple[int, str]:
    """Check for a valid voms/grid proxy longer than N hours.

    Use `limit` to set required time limit.

    Args:
        limit: time limit in hours.
        x509: points to the proxy file. If not set (=None) - get proxy file from X509_USER_PROXY environment.
        proxy_id: proxy id.
        test: free Boolean test parameter.
        pilotstartup: free Boolean pilotstartup parameter.

    Returns:
        tuple[int, str]: exit code (NOPROXY or NOVOMSPROXY), diagnostics (error diagnostics string).
    """
    if pilotstartup:
        limit = 72  # 3 days
    if limit is None:
        limit = 1

    # add setup for arcproxy if it exists
    if x509 is None:
        x509 = os.environ.get('X509_USER_PROXY', '')
    if x509 != '':
        envsetup = f'export X509_USER_PROXY={x509};'
    else:
        envsetup = ''

    return verify_arcproxy(envsetup, limit, proxy_id=proxy_id, test=test)  # exit_code, diagnostics


def verify_arcproxy(envsetup: str, limit: int, proxy_id: str = "pilot", test: bool = False) -> tuple[int, str]:  # noqa: C901
    """Verify the proxy using arcproxy.

    Args:
        envsetup: general setup string for proxy commands.
        limit: time limit in hours.
        proxy_id: proxy unique id name. The verification result will be cached for this id. If None the result will not be cached.
        test: free Boolean test parameter.

    Returns:
        tuple[int, str]: exit code, error diagnostics.
    """
    exit_code = 0
    diagnostics = ""
    proxies = ['cert', 'proxy']

    if test:
        return errors.VOMSPROXYABOUTTOEXPIRE, 'dummy test'

    if proxy_id is not None:
        if not hasattr(verify_arcproxy, "cache"):
            verify_arcproxy.cache = {}

        if proxy_id in verify_arcproxy.cache:  # if exists, then calculate result from current cache
            validity_end_cert = verify_arcproxy.cache[proxy_id][0]
            validity_end = verify_arcproxy.cache[proxy_id][1]
            if validity_end < 0:  # previous validity check failed, do not try to re-check
                exit_code = -1
                diagnostics = "arcproxy verification failed (cached result)"
            else:
                #
                validities = [validity_end_cert, validity_end]
                for proxyname, validity in list(zip(proxies, validities)):
                    exit_code, diagnostics = check_time_left(proxyname, validity, limit)
                    if exit_code == errors.VOMSPROXYABOUTTOEXPIRE:
                        # remove the proxy_id from the dictionary to trigger a new entry after a new proxy has been downloaded
                        del verify_arcproxy.cache[proxy_id]

            return exit_code, diagnostics

    # options and options' sequence are important for parsing, do not change it
    # -i validityEnd -i validityLeft: time left for the certificate
    # -i vomsACvalidityEnd -i vomsACvalidityLeft: time left for the proxy
    #   validityEnd - timestamp when proxy validity ends.
    #   validityLeft - duration of proxy validity left in seconds.
    #   vomsACvalidityEnd - timestamp when VOMS attribute validity ends.
    #   vomsACvalidityLeft - duration of VOMS attribute validity left in seconds.
    cmd = f"{envsetup}arcproxy -i validityEnd -i validityLeft -i vomsACvalidityEnd -i vomsACvalidityLeft"
    _exit_code, stdout, stderr = execute_nothreads(cmd, shell=True)  # , usecontainer=True, copytool=True)
    if stdout is not None:
        if 'command not found' in stdout:
            logger.warning(f"arcproxy is not available on this queue,"
                           f"this can lead to memory issues with voms-proxy-info on SL6: {stdout}")
            exit_code = -1
        else:
            exit_code, diagnostics, validity_end_cert, validity_end = interpret_proxy_info(_exit_code, stdout, stderr, limit)
            # validity_end = int(time()) + 71 * 3600  # 71 hours test

            if proxy_id == 'pilot' and validity_end:
                # Record when the pilot's own proxy expires, so that the rest of the pilot can work
                # out how much proxy time is left (see pilot.control.job.get_remaining_time()).
                #
                # This has to happen here rather than deeper down in extract_time_left(), which has
                # no way of telling which proxy it was given: get_and_verify_proxy() verifies the
                # *payload* proxy with proxy_id=None, and would otherwise overwrite the pilot's
                # validity with the payload proxy's.
                #
                # An absolute epoch time is stored rather than a relative lifetime, since the branch
                # above serves subsequent calls from verify_arcproxy.cache and this code is normally
                # only reached once per pilot -- a relative value would be frozen at its start-up
                # reading and never decrease. It is stored regardless of exit_code, as the validity
                # is a parsed property of the proxy independent of whether it passed the limit check.
                pilot_cache.proxy_validity_end = validity_end

            validity_end_cert, validity_end = apply_test_lifetime(proxy_id, validity_end_cert, validity_end)
            if proxy_id and validity_end:  # setup cache if requested
                if exit_code == 0:
                    logger.info(f"caching the validity ends from arcproxy: cache[\'{proxy_id}\'] = [{validity_end_cert}, {validity_end}]")
                    verify_arcproxy.cache[proxy_id] = [validity_end_cert, validity_end]
                else:
                    logger.warning('cannot store validity ends from arcproxy in cache')
                    verify_arcproxy.cache[proxy_id] = [-1, -1]  # -1 in cache means any error in prev validation
            if exit_code == 0:
                endtimes = [validity_end_cert, validity_end] if not proxy_id else verify_arcproxy.cache[proxy_id]
                for proxyname, validity in list(zip(proxies, endtimes)):
                    exit_code, diagnostics = check_time_left(proxyname, validity, limit)
                    if exit_code == errors.VOMSPROXYABOUTTOEXPIRE:
                        # remove the proxy_id from the dictionary to trigger a new entry after a new proxy has been downloaded
                        if proxy_id:
                            del verify_arcproxy.cache[proxy_id]
                    if exit_code == errors.CERTIFICATEHASEXPIRED:
                        logger.debug('certificate has expired')
                        break
                    if exit_code == errors.PROXYTOOSHORT:
                        # logger.debug('proxy is too short - aborting')
                        break
            if exit_code == errors.ARCPROXYLIBFAILURE:
                logger.warning("currenly ignoring arcproxy library failure")
                exit_code = 0
                diagnostics = ""
    else:
        logger.warning('command execution failed')

    return exit_code, diagnostics


def check_time_left(proxyname: str, validity: int, limit: int) -> tuple[int, str]:
    """Check the time left for the proxy.

    Args:
        proxyname: cert or proxy.
        validity: validity time.
        limit: time limit in hours.

    Returns:
        tuple[int, str]: exit code, diagnostics.
    """
    exit_code = 0
    diagnostics = ''
    tnow = int(time() + 0.5)  # round to seconds
    seconds_left = validity - tnow

    # test bad proxy
    #if proxyname == 'proxy':
    #    seconds_left = 1000
    logger.info(f"cache: check {proxyname} validity: wanted={limit}h ({limit * 3600 - 20 * 60}s with grace) "
                f"left={float(seconds_left) / 3600:.2f}h (now={tnow} validity={validity} left={seconds_left}s)")

    # special case for limit=72h (3 days) for pilot startup
    if limit == 72 and seconds_left < limit * 3600 - 20 * 60:
        diagnostics = f'proxy is too short for pilot startup: {float(seconds_left) / 3600:.2f}h'
        logger.warning(diagnostics)
        exit_code = errors.PROXYTOOSHORT
    elif seconds_left < limit * 3600 - 20 * 60:
        diagnostics = f'cert/proxy is about to expire: {float(seconds_left) / 3600:.2f}h'
        logger.warning(diagnostics)
        exit_code = errors.CERTIFICATEHASEXPIRED if proxyname == 'cert' else errors.VOMSPROXYABOUTTOEXPIRE
    else:
        logger.info(f"{proxyname} validity time is verified")

    return exit_code, diagnostics


def get_test_lifetime() -> int:
    """Return the validity (s) that job proxies are clamped to in test mode.

    It is defined relative to the renewal threshold, so that a renewal is triggered
    TEST_JOB_PROXY_MARGIN seconds after each download regardless of the threshold.

    Returns:
        int: clamped validity in seconds.
    """
    return JOB_PROXY_RENEWAL_THRESHOLD + TEST_JOB_PROXY_MARGIN


def apply_test_lifetime(proxy_id: str | None, validity_end_cert: int | None,
                        validity_end: int | None) -> tuple[int | None, int | None]:
    """Clamp the validity of a job proxy in test mode (see _TEST_JOB_PROXY_RENEWAL).

    The pilot's own proxy is never affected.

    Args:
        proxy_id: proxy id used for the arcproxy cache.
        validity_end_cert: certificate validity end (epoch seconds).
        validity_end: VOMS attribute validity end (epoch seconds).

    Returns:
        tuple[int | None, int | None]: possibly clamped validity_end_cert, validity_end.
    """
    if not _TEST_JOB_PROXY_RENEWAL or proxy_id not in JOB_PROXY_TYPES:
        return validity_end_cert, validity_end

    lifetime = get_test_lifetime()
    latest = int(time()) + lifetime
    logger.warning(f"TEST MODE: clamping the validity of the {proxy_id} proxy to {lifetime} s "
                   f"(set _TEST_JOB_PROXY_RENEWAL=False in pilot/user/atlas/proxy.py before release)")
    if validity_end_cert:
        validity_end_cert = min(validity_end_cert, latest)
    if validity_end:
        validity_end = min(validity_end, latest)

    return validity_end_cert, validity_end


def get_cache_entry(proxy_id: str) -> list | None:
    """Return the arcproxy cache entry for the given proxy id.

    Args:
        proxy_id: proxy id used for the arcproxy cache.

    Returns:
        list | None: [validity end cert, validity end], or None if there is no entry.
    """
    return getattr(verify_arcproxy, "cache", {}).get(proxy_id)


def set_cache_entry(proxy_id: str, entry: list | None) -> None:
    """Set (or remove, if entry is None) the arcproxy cache entry for the given proxy id.

    Args:
        proxy_id: proxy id used for the arcproxy cache.
        entry: [validity end cert, validity end], or None to remove the entry.
    """
    if not hasattr(verify_arcproxy, "cache"):
        verify_arcproxy.cache = {}
    if entry is None:
        verify_arcproxy.cache.pop(proxy_id, None)
    else:
        verify_arcproxy.cache[proxy_id] = entry


def invalidate_proxy_cache(proxy_id: str) -> None:
    """Remove the arcproxy cache entry for the given proxy id.

    Args:
        proxy_id: proxy id used for the arcproxy cache.
    """
    set_cache_entry(proxy_id, None)


def get_cached_validity_end(proxy_id: str) -> int | None:
    """Return the earliest cached validity end of the given proxy.

    Both the certificate and the VOMS attribute validity ends are considered, since the proxy
    is unusable as soon as either of them has passed.

    Args:
        proxy_id: proxy id used for the arcproxy cache.

    Returns:
        int | None: validity end (epoch seconds), -1 if the earlier verification failed,
            or None if there is no cache entry.
    """
    entry = get_cache_entry(proxy_id)
    if not entry:
        return None
    ends = [end for end in entry if end is not None]

    return min(ends) if ends else None


def check_downloaded_lifetime(proxy_id: str) -> bool:
    """Check that a job proxy just downloaded from the PanDA server has the expected remaining validity.

    The server only serves proxies with at least JOB_PROXY_EXPECTED_LIFETIME left. A shorter proxy
    indicates a server-side problem and is reported, but it is still used: it may well be long enough
    for the job, and the job would otherwise be failed over a problem that may not affect it. In test
    mode the validity is clamped (see apply_test_lifetime()), so the check is skipped.

    Args:
        proxy_id: job proxy id ('payload' or 'unified').

    Returns:
        bool: False if the proxy is shorter than expected, True otherwise (including unknown validity).
    """
    if _TEST_JOB_PROXY_RENEWAL:
        logger.debug(f"TEST MODE: not checking the remaining validity of the downloaded {proxy_id} proxy")
        return True

    validity_end = get_cached_validity_end(proxy_id)
    if validity_end is None or validity_end < 0:
        return True

    seconds_left = validity_end - int(time())
    if seconds_left < JOB_PROXY_EXPECTED_LIFETIME - JOB_PROXY_LIFETIME_GRACE:
        logger.warning(f"the PanDA server returned a {proxy_id} proxy with only {seconds_left / 3600:.2f}h left "
                       f"(expected at least {JOB_PROXY_EXPECTED_LIFETIME / 3600:.0f}h) - using it anyway")
        return False

    return True


def is_renewal_backed_off(proxy_id: str, seconds_left: int) -> bool:
    """Check whether the renewal of the given job proxy should be postponed.

    The backoff never applies during the last backoff interval before JOB_PROXY_HARD_FLOOR, so that
    every check makes a last attempt before the job is failed.

    Args:
        proxy_id: job proxy id ('payload' or 'unified').
        seconds_left: remaining validity of the current proxy (s).

    Returns:
        bool: True if the renewal should be postponed.
    """
    if seconds_left < JOB_PROXY_HARD_FLOOR + JOB_PROXY_RENEWAL_BACKOFF:
        return False

    return int(time()) < _renewal_backoff.get(proxy_id, 0)


def set_renewal_backoff(proxy_id: str) -> None:
    """Postpone the next renewal attempt of the given job proxy by JOB_PROXY_RENEWAL_BACKOFF.

    Args:
        proxy_id: job proxy id ('payload' or 'unified').
    """
    _renewal_backoff[proxy_id] = int(time()) + JOB_PROXY_RENEWAL_BACKOFF
    logger.info(f"next renewal attempt of the {proxy_id} proxy in {JOB_PROXY_RENEWAL_BACKOFF} s at the earliest")


def clear_renewal_backoff(proxy_id: str) -> None:
    """Allow the next renewal attempt of the given job proxy at the next check.

    Args:
        proxy_id: job proxy id ('payload' or 'unified').
    """
    _renewal_backoff.pop(proxy_id, None)


def get_job_proxies() -> list[tuple[str, str]]:
    """Return the job proxies currently in use.

    Returns:
        list[tuple[str, str]]: list of (proxy id, path) for each existing job proxy.
    """
    proxies = []
    unified = os.environ.get("X509_UNIFIED_DISPATCH", "")
    if unified and os.path.exists(unified):
        proxies.append(("unified", unified))
    payload = pilot_cache.payload_proxy
    if payload and os.path.exists(payload):
        proxies.append(("payload", payload))

    return proxies


def is_current_job_proxy(proxy_id: str, path: str) -> bool:
    """Check whether the given path is still the job proxy of the given type.

    Args:
        proxy_id: job proxy id ('payload' or 'unified').
        path: path to the proxy.

    Returns:
        bool: True if the proxy is still in use.
    """
    current = os.environ.get("X509_UNIFIED_DISPATCH", "") if proxy_id == "unified" else pilot_cache.payload_proxy

    return current == path


def verify_job_proxies(proxy_ids: tuple = JOB_PROXY_TYPES) -> tuple[int, str]:
    """Verify the job proxies and renew any proxy that is about to expire.

    Job proxies are the payload proxy (non-unified analysis queues) and the user proxy on
    unified dispatch queues. Unlike the pilot's own proxy, they are renewed here directly,
    since the generic renewal in the job monitor downloads a production proxy.

    Args:
        proxy_ids: job proxy ids to consider.

    Returns:
        tuple[int, str]: exit code (0, or PAYLOADPROXYDOWNLOADFAILURE if a proxy could not be
            renewed before it expired), diagnostics.
    """
    for proxy_id, path in get_job_proxies():
        if proxy_id not in proxy_ids:
            continue
        exit_code, diagnostics = check_job_proxy(proxy_id, path)
        if exit_code:
            return exit_code, diagnostics

    return 0, ""


def check_job_proxy(proxy_id: str, path: str) -> tuple[int, str]:
    """Check the validity of a job proxy using the arcproxy cache, and renew it if necessary.

    arcproxy is only executed when a proxy has been downloaded (see get_and_verify_proxy()),
    the checks in between are served from the cache. A proxy without a cached validity (arcproxy
    unavailable) cannot be verified and is left alone.

    The proxy is renewed once less than JOB_PROXY_RENEWAL_THRESHOLD remains. A failed download or
    verification is retried at the next check. A renewal that did not solve the problem (see
    _renew_job_proxy()) is retried after JOB_PROXY_RENEWAL_BACKOFF. The job is only failed once the
    proxy is about to expire for real (see JOB_PROXY_HARD_FLOOR).

    Args:
        proxy_id: job proxy id ('payload' or 'unified').
        path: path to the proxy.

    Returns:
        tuple[int, str]: exit code (0 or PAYLOADPROXYDOWNLOADFAILURE), diagnostics.
    """
    validity_end = get_cached_validity_end(proxy_id)
    if validity_end is None or validity_end < 0:
        if validity_end is None:
            # remember this, so that arcproxy is not executed and the warning not repeated at every check
            logger.warning(f"the validity of the {proxy_id} proxy is unknown - it cannot be verified or renewed")
            set_cache_entry(proxy_id, [-1, -1])
        return 0, ""

    seconds_left = validity_end - int(time())
    logger.info(f"the {proxy_id} proxy has {seconds_left / 3600:.2f}h left "
                f"(renewal below {JOB_PROXY_RENEWAL_THRESHOLD / 3600:.0f}h)")
    if seconds_left >= JOB_PROXY_RENEWAL_THRESHOLD:
        return 0, ""

    if is_renewal_backed_off(proxy_id, seconds_left):
        logger.info(f"the renewal of the {proxy_id} proxy is postponed after an earlier attempt")
        return 0, ""

    exit_code, diagnostics = renew_job_proxy(proxy_id, path)
    if exit_code == 0:
        return 0, ""

    seconds_left = validity_end - int(time())
    if seconds_left < JOB_PROXY_HARD_FLOOR:
        diagnostics = f"the {proxy_id} proxy could not be renewed and expires in {seconds_left} s: {diagnostics}"
        logger.warning(diagnostics)
        return errors.PAYLOADPROXYDOWNLOADFAILURE, diagnostics

    logger.warning(f"the {proxy_id} proxy was not renewed (will try again later): {diagnostics}")

    return 0, ""


def renew_job_proxy(proxy_id: str, path: str) -> tuple[int, str]:
    """Download, verify and install a new job proxy.

    Only one renewal runs at a time. If another thread renewed the proxy, or attempted to, while
    this one was waiting, nothing is done.

    Args:
        proxy_id: job proxy id ('payload' or 'unified').
        path: path to the proxy.

    Returns:
        tuple[int, str]: exit code (0 or PAYLOADPROXYDOWNLOADFAILURE), diagnostics.
    """
    with _renewal_lock:
        validity_end = get_cached_validity_end(proxy_id)
        if validity_end and validity_end > 0:
            seconds_left = validity_end - int(time())
            if seconds_left >= JOB_PROXY_RENEWAL_THRESHOLD:
                logger.info(f"the {proxy_id} proxy has already been renewed")
                return 0, ""
            if is_renewal_backed_off(proxy_id, seconds_left):
                logger.info(f"the renewal of the {proxy_id} proxy has just been attempted")
                return 0, ""

        return _renew_job_proxy(proxy_id, path)


def _renew_job_proxy(proxy_id: str, path: str) -> tuple[int, str]:
    """Download, verify and install a new job proxy (to be called with _renewal_lock held).

    The new proxy is downloaded to a temporary file next to the proxy in use, verified, and only
    then moved over it. The proxy in use is therefore never replaced by one that failed to
    download or verify, and a reader never sees a partially written file. The file is replaced
    within the same directory, which is visible to the running payload container as /srv.

    The new proxy is only installed if it is valid longer than the current one, since the server
    may return the same (cached) proxy. If it is not, or if it is still below the renewal threshold,
    the next attempt is postponed (see JOB_PROXY_RENEWAL_BACKOFF).

    Args:
        proxy_id: job proxy id ('payload' or 'unified').
        path: path to the proxy.

    Returns:
        tuple[int, str]: exit code (0 or PAYLOADPROXYDOWNLOADFAILURE), diagnostics.
    """
    logger.info(f"the {proxy_id} proxy is about to expire - downloading a new one for {path}")
    tmp_path = f"{path}.tmp"
    old_entry = get_cache_entry(proxy_id)
    old_end = get_cached_validity_end(proxy_id)

    exit_code, diagnostics = _download_job_proxy(proxy_id, tmp_path)
    if exit_code:
        set_cache_entry(proxy_id, old_entry)
        return exit_code, diagnostics

    new_end = get_cached_validity_end(proxy_id)
    check_downloaded_lifetime(proxy_id)
    if new_end is None or (old_end is not None and new_end <= old_end):
        _remove_if_exists(tmp_path)
        set_cache_entry(proxy_id, old_entry)
        set_renewal_backoff(proxy_id)
        return errors.PAYLOADPROXYDOWNLOADFAILURE, (f"the new {proxy_id} proxy (valid until {new_end}) is not valid "
                                                    f"longer than the current one (valid until {old_end}) - keeping "
                                                    f"the current one")

    try:
        os.replace(tmp_path, path)
    except OSError as exc:
        _remove_if_exists(tmp_path)
        set_cache_entry(proxy_id, old_entry)
        return errors.PAYLOADPROXYDOWNLOADFAILURE, f"failed to install the new {proxy_id} proxy: {exc}"

    if not is_current_job_proxy(proxy_id, path):
        # the job has moved on while the proxy was being renewed (e.g. stage-out removed it)
        logger.info(f"the {proxy_id} proxy is no longer in use - removing the renewed proxy")
        _remove_if_exists(path)
        invalidate_proxy_cache(proxy_id)
        return 0, ""

    if new_end - int(time()) < JOB_PROXY_RENEWAL_THRESHOLD:
        set_renewal_backoff(proxy_id)
    else:
        clear_renewal_backoff(proxy_id)
    logger.info(f"renewed the {proxy_id} proxy: {path} (valid until {new_end})")

    return 0, ""


def _download_job_proxy(proxy_id: str, tmp_path: str) -> tuple[int, str]:
    """Download a new job proxy to a temporary file and verify it (to be called with _renewal_lock held).

    On success, the arcproxy cache entry of the job proxy holds the validity of the new proxy. On
    failure, the temporary file is removed, and the caller must restore the cache entry.

    Args:
        proxy_id: job proxy id ('payload' or 'unified').
        tmp_path: path of the temporary file.

    Returns:
        tuple[int, str]: exit code (0 or PAYLOADPROXYDOWNLOADFAILURE), diagnostics.
    """
    x509 = os.environ.get("X509_USER_PROXY", "")
    res, written = get_proxy(tmp_path, get_voms_role(role="user"))
    if written != tmp_path:
        # get_proxy() fell back to another location (read-only file system) and redirected
        # X509_USER_PROXY there - undo that, the pilot's own proxy must not be replaced
        os.environ["X509_USER_PROXY"] = x509
        _remove_if_exists(written)
        return errors.PAYLOADPROXYDOWNLOADFAILURE, f"the new {proxy_id} proxy could not be written to {tmp_path}"
    if not res:
        _remove_if_exists(tmp_path)
        return errors.PAYLOADPROXYDOWNLOADFAILURE, f"failed to download a new {proxy_id} proxy"

    invalidate_proxy_cache(proxy_id)
    exit_code, diagnostics = verify_proxy(x509=tmp_path, proxy_id=proxy_id)
    if exit_code != 0 or diagnostics:
        _remove_if_exists(tmp_path)
        return errors.PAYLOADPROXYDOWNLOADFAILURE, f"the new {proxy_id} proxy failed verification: {exit_code}, {diagnostics}"

    return 0, ""


def _remove_if_exists(path: str) -> None:
    """Remove the given file if it exists.

    Args:
        path: path to the file.
    """
    if path and os.path.exists(path):
        remove(path)


def verify_vomsproxy(envsetup: str, limit: int) -> tuple[int, str]:
    """Verify proxy using voms-proxy-info command.

    Args:
        envsetup: general setup string for proxy commands.
        limit: time limit in hours.

    Returns:
        tuple[int, str]: exit code, error diagnostics.
    """
    exit_code = 0
    diagnostics = ""

    if os.environ.get('X509_USER_PROXY', '') != '':
        cmd = f"{envsetup}voms-proxy-info -actimeleft --timeleft --file $X509_USER_PROXY"
        logger.info(f'executing command: {cmd}')
        _exit_code, stdout, stderr = execute_nothreads(cmd, shell=True)
        if stdout is not None:
            if "command not found" in stdout:
                logger.info("skipping voms proxy check since command is not available")
            else:
                exit_code, diagnostics, _, _ = interpret_proxy_info(_exit_code, stdout, stderr, limit)
                if exit_code == 0:
                    logger.info("voms proxy verified using voms-proxy-info")
                    return 0, diagnostics
        else:
            logger.warning('command execution failed')
    else:
        logger.warning('X509_USER_PROXY is not set')

    return exit_code, diagnostics


def verify_gridproxy(envsetup: str, limit: int) -> tuple[int, str]:
    """Verify proxy using grid-proxy-info command.

    Args:
        envsetup: general setup string for proxy commands.
        limit: time limit in hours.

    Returns:
        tuple[int, str]: exit code, error diagnostics.
    """
    ec = 0
    diagnostics = ""

    if limit:
        # next clause had problems: grid-proxy-info -exists -valid 0.166666666667:00
        # more accurate calculation of HH:MM
        limit_hours = int(limit * 60) / 60
        limit_minutes = int(limit * 60 + .999) - limit_hours * 60
        cmd = f"{envsetup}grid-proxy-info -exists -valid {limit_hours}:{limit_minutes:02}"
    else:
        cmd = f"{envsetup}grid-proxy-info -exists -valid 24:00"

    logger.info(f'executing command: {cmd}')
    exit_code, stdout, _ = execute(cmd, shell=True)
    if stdout is not None:
        if exit_code != 0:
            if stdout.find("command not found") > 0:
                logger.info("skipping grid proxy check since command is not available")
            else:
                # Analyze exit code / stdout
                diagnostics = f"grid proxy certificate does not exist or is too short: {exit_code}, {stdout}"
                logger.warning(diagnostics)
                return errors.NOPROXY, diagnostics
        else:
            logger.info("grid proxy verified")
    else:
        logger.warning('command execution failed')

    return ec, diagnostics


def interpret_proxy_info(proxy_ec: Any, stdout: str, stderr: str, limit: int) -> tuple[int, str, Optional[int], Optional[int]]:
    """Interpret the output from arcproxy.

    Args:
        proxy_ec: exit code from proxy command.
        stdout: stdout from proxy command.
        stderr: stderr from proxy command.
        limit: time limit in hours.

    Returns:
        tuple[int, str, Optional[int], Optional[int]]: exit code, diagnostics, validity end cert, validity end in seconds if detected or None if not detected.
    """
    exitcode = 0
    diagnostics = ""
    validity_end = None  # not detected
    validity_end_cert = None  # not detected

    logger.debug(f'stdout = {stdout}')
    logger.debug(f'stderr = {stderr}')

    if proxy_ec != 0:
        if "Unable to verify signature! Server certificate possibly not installed" in stdout:
            logger.warning(f"skipping voms proxy check: {stdout}")
        # test for command errors
        elif "arcproxy: error while loading shared libraries" in stderr:
            diagnostics = stderr
            logger.warning(diagnostics)
            exitcode = errors.ARCPROXYLIBFAILURE
        elif "arcproxy:" in stdout:
            diagnostics = f"arcproxy failed: {stdout}"
            logger.warning(diagnostics)
            exitcode = errors.ARCPROXYFAILURE
        else:
            # Analyze exit code / output
            diagnostics = f"voms proxy certificate check failure: {proxy_ec}, {stdout}"
            logger.warning(diagnostics)
            exitcode = errors.NOVOMSPROXY
    else:
        if "\n" in stdout:
            # try to extract the time left from the command output
            validity_end_cert, validity_end, stdout = extract_time_left(stdout)
            if validity_end:
                return exitcode, diagnostics, validity_end_cert, validity_end

            diagnostics = f"arcproxy failed: {stdout}"
            logger.warning(diagnostics)
            exitcode = errors.GENERALERROR

            return exitcode, diagnostics, validity_end_cert, validity_end

        # test for command errors
        if "arcproxy:" in stdout:
            diagnostics = f"arcproxy failed: {stdout}"
            logger.warning(diagnostics)
            exitcode = errors.GENERALERROR
        else:
            # on EMI-3 the time output is different (HH:MM:SS as compared to SS on EMI-2)
            if ":" in stdout:
                ftr = [3600, 60, 1]
                stdout = sum(a * b for a, b in zip(ftr, [int(x) for x in stdout.split(':')]))
            try:
                validity = int(stdout)
                if validity >= limit * 3600:
                    logger.info(f"voms proxy verified ({validity} s)")
                else:
                    diagnostics = f"voms proxy certificate does not exist or is too short (lifetime {validity} s)"
                    logger.warning(diagnostics)
                    exitcode = errors.NOVOMSPROXY
            except ValueError as exc:
                diagnostics = f"failed to evaluate command stdout: {stdout}, stderr: {stderr}, exc={exc}"
                logger.warning(diagnostics)
                exitcode = errors.GENERALERROR

    return exitcode, diagnostics, validity_end_cert, validity_end


def extract_time_left(stdout: str) -> tuple[Optional[int], Optional[int], str]:
    """Extract the time left for the cert and proxy from the proxy command.

    Some processing on the stdout is done.

    Args:
        stdout: stdout.

    Returns:
        tuple[Optional[int], Optional[int], str]: validity_end_cert, validity_end, stdout.
    """
    validity_end_cert = None
    validity_end = None

    # remove the last \n in case there is one
    if stdout[-1] == '\n':
        stdout = stdout[:-1]
    stdout_split = stdout.split('\n')
    # give up if there are not four entries
    if len(stdout_split) != 4:
        print(f'cannot extract validity_end from: {stdout}')
        return None, None, stdout

    try:
        validity_end_cert = int(stdout_split[-4])
        validity_end = int(stdout_split[-2])
    except (ValueError, TypeError):
        # try to get validity_end in penultimate line
        try:
            validity_end_cert = None  # unknown in this case
            validity_end_str = stdout_split[-1]  # may raise exception IndexError if stdout is too short
            logger.debug(f"try to get validity_end from the line: \"{validity_end_str}\"")
            validity_end = int(
                validity_end_str)  # may raise ValueError if not string
        except (IndexError, ValueError) as exc:
            logger.warning(f"validity_end not found in stdout: {exc}")

    if validity_end_cert:
        logger.info(f"validity_end_cert = {validity_end_cert}")
    if validity_end:
        logger.info(f"validity_end = {validity_end}")

    return validity_end_cert, validity_end, stdout


def extract_time_left_old(stdout: str) -> tuple[Optional[int], str]:
    """Extract the time left from the proxy command.

    Some processing on the stdout is done.

    Args:
        stdout: stdout.

    Returns:
        tuple[Optional[int], str]: validity_end, stdout.
    """
    validity_end = None

    # remove the last \n in case there is one
    if stdout[-1] == '\n':
        stdout = stdout[:-1]
    stdout_split = stdout.split('\n')
    try:
        validity_end = int(stdout_split[-2])
    except (ValueError, TypeError):
        # try to get validity_end in penultimate line
        try:
            validity_end_str = stdout_split[-1]  # may raise exception IndexError if stdout is too short
            logger.debug(f"try to get validity_end from the line: \"{validity_end_str}\"")
            validity_end = int(validity_end_str)  # may raise ValueError if not string
        except (IndexError, ValueError) as exc:
            logger.info(f"validity_end not found in stdout: {exc}")
        #validity_end = None

    if validity_end:
        logger.info(f"validity_end = {validity_end}")

    return validity_end, stdout


def getproxy_dictionary(voms_role: str) -> dict:
    """Prepare the dictionary with the VOMS role and DN for the getProxy call.

    Args:
        voms_role: VOMS role.

    Returns:
        dict: getProxy dictionary.
    """
    return {'role': voms_role, 'dn': 'atlpilo2'} if voms_role == 'atlas' else {'role': voms_role}
