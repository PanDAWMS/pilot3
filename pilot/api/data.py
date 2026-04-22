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
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2026
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2018-2024

"""
API for data transfers.

This module provides a high-level API for managing data transfers (stage-in and stage-out)
within the Pilot framework. It serves as an abstraction layer over various underlying transfer
protocols and tools, collectively known as "copytools." The primary goal is to provide a
unified interface for staging data, regardless of the specific technology used for the transfer.

Core Classes:
- `StagingClient`: This is the base class that provides the common framework for data staging.
  It handles the dynamic selection of copytools based on site configuration and the type of
  activity (e.g., 'read_lan', 'write_wan'). It includes methods for resolving file replicas
  from catalogs like Rucio, sorting them based on priority (e.g., LAN vs. WAN), and
  orchestrating the transfer process through the `transfer` method. It also manages tracing
  and logging for transfers.

- `StageInClient`: This class inherits from `StagingClient` and specializes in handling the
  stage-in of input files. It contains logic to resolve the best replica for input files,
  considering factors like direct access modes (LAN/WAN), allowed schemas (e.g., 'root', 'https'),
  and site-specific storage configurations. It is also responsible for checking available
  disk space and verifying that input file sizes are within configured limits.

- `StageOutClient`: This class, also inheriting from `StagingClient`, is responsible for
  staging out output files. Its key functionality includes preparing destinations by resolving
  the correct output storage element (RSE) based on the activity. It constructs the final
  destination SURL (Storage URL) for the output files, calculates checksums for verification,
  and ensures that output files exist and have a non-zero size before initiating the transfer.

Key Concepts:
- Copytools: The actual file transfers are delegated to specific "copytool" modules, which
  are located in the `pilot/copytool/` directory (e.g., `rucio`, `xrdcp`, `gfal`). The
  `StagingClient` dynamically imports and uses the appropriate copytool based on the
  `acopytools` configuration for a given activity. This design makes the system extensible
  to new transfer protocols.

- Replica Resolution: For stage-in, the client queries a catalog (like Rucio) to find all
  available replicas (copies) of a file. These replicas are then sorted by priority
  (e.g., network proximity, site preference) to select the most efficient source for the
  transfer.

- Protocol and Destination Resolution: For stage-out, the client determines the correct
  destination storage and the protocol to use for the transfer based on site and experiment
  configurations stored in `ddmconf` and `astorages`.

- Direct Access: The `StageInClient` supports a "direct access" or "remote I/O" mode, where
  files are not physically copied to the worker node but are accessed remotely by the payload.
  The client identifies when this mode is applicable and sets the file status accordingly,
  providing the payload with the correct remote TURL (Transport URL).

Workflow:
1. A `StageInClient` or `StageOutClient` is instantiated with site-specific information.
2. The `transfer` method is called with a list of `FileSpec` objects that represent the
   files to be transferred.
3. The client determines the appropriate copytool(s) for the given activity.
4. For each copytool, the client prepares the files:
   - Stage-in: Resolves replicas, selects the best source URL, and checks for direct
     access possibilities.
   - Stage-out: Resolves the destination URL and prepares the source file by checking for
     its existence and calculating its checksum.
5. The client calls the `copy_in` or `copy_out` function of the selected copytool module,
   passing the list of files to be transferred.
6. The copytool executes the transfer.
7. The client updates the status of the `FileSpec` objects and handles any errors. If one
   copytool fails, it can try the next one in the configured list.
"""

import os
import hashlib
import logging
import time
from functools import reduce
from typing import Any, Optional, Union
try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

from pilot.info import infosys
from pilot.common.exception import (
    PilotException,
    ErrorCodes,
    SizeTooLarge,
    NoLocalSpace,
    ReplicasNotFound,
)
from pilot.util.config import config
from pilot.util.filehandling import (
    calculate_checksum,
    write_json,
)
from pilot.util.math import convert_mb_to_b
from pilot.util.parameters import get_maximum_input_sizes
from pilot.util.workernode import get_local_disk_space
from pilot.util.auxiliary import TimeoutException
from pilot.util.tracereport import TraceReport

# Chunk size chosen to stay well within Rucio's documented limits.
_RUCIO_LIST_REPLICAS_CHUNK_SIZE = 1000
_RUCIO_MAX_ATTEMPTS = 3
_RUCIO_BACKOFF_BASE_SECONDS = 2

# Transfer types that imply direct I/O (remote_io) rather than copy-to-scratch.
# 'file' is intentionally excluded — it means Rucio copy via POSIX link.
_DIRECTIO_TRANSFER_TYPES = frozenset({'direct', 'root', 'davs'})


def is_directio_transfertype(transfertype: str) -> bool:
    """Return True if *transfertype* implies direct I/O (remote_io) access.

    A single keyword (e.g. ``"direct"``, ``"root"``, ``"davs"``) or a
    comma-separated list of those keywords (e.g. ``"davs,root"``) all return
    ``True``.  An empty/``None`` value or any string containing ``"file"``
    returns ``False``.

    Args:
        transfertype: Value of ``job.transfertype`` from the server.

    Returns:
        bool: ``True`` when all tokens are recognised direct-I/O types.
    """
    if not transfertype:
        return False
    tokens = [t.strip() for t in transfertype.lower().split(',') if t.strip()]
    return bool(tokens) and all(t in _DIRECTIO_TRANSFER_TYPES for t in tokens)


def get_directio_preferred_schemas(transfertype: str, default_schemas: list) -> list:
    """Return an ordered schema list for direct I/O, honouring *transfertype*.

    When *transfertype* is ``"direct"`` or empty the *default_schemas* list is
    returned unchanged (preserving existing behaviour).  For any other
    recognised keyword(s) the requested protocols are moved to the front of the
    list and the remaining entries from *default_schemas* follow in their
    original order.

    Examples::

        get_directio_preferred_schemas("davs",      ["root", "https"])
        # -> ["davs", "root", "https"]

        get_directio_preferred_schemas("davs,root", ["root", "https"])
        # -> ["davs", "root", "https"]

        get_directio_preferred_schemas("direct",    ["root", "https"])
        # -> ["root", "https"]   (unchanged)

    Args:
        transfertype: Value of ``job.transfertype`` from the server.
        default_schemas: The schema list that would be used without any
            *transfertype* override (e.g. ``direct_localinput_allowed_schemas``).

    Returns:
        list: Re-ordered schema list with requested protocols first.
    """
    if not transfertype:
        return default_schemas
    tokens = [t.strip() for t in transfertype.lower().split(',') if t.strip() in _DIRECTIO_TRANSFER_TYPES]
    # 'direct' is the legacy keyword meaning "use default order"
    explicit = [t for t in tokens if t != 'direct']
    if not explicit:
        return default_schemas
    remainder = [s for s in default_schemas if s not in explicit]
    return explicit + remainder


class StagingClient:
    """Base class for stage-in and stage-out clients.

    Provides the common framework for data staging, including dynamic copytool
    selection, replica resolution, protocol negotiation, and transfer orchestration.
    Subclasses (:class:`StageInClient`, :class:`StageOutClient`) implement the
    direction-specific logic by overriding :meth:`transfer_files` and
    :meth:`resolve_surl`.

    Class-level attributes:
        copytool_modules (dict): Registry mapping copytool names to their module names.
        direct_remoteinput_allowed_schemas (list): URL schemas permitted for direct
            (remote-I/O) access from WAN replicas.
        direct_localinput_allowed_schemas (list): URL schemas permitted for direct
            access from LAN replicas.
        remoteinput_allowed_schemas (list): URL schemas permitted for copied transfers
            from remote (WAN) replicas.
    """

    ipv = "IPv6"
    workdir = ''
    mode = ""  # stage-in/out, set by the inheritor of the class
    copytool_modules = {'rucio': {'module_name': 'rucio'},
                        'gfal': {'module_name': 'gfal'},
                        'gfalcopy': {'module_name': 'gfal'},
                        'xrdcp': {'module_name': 'xrdcp'},
                        'mv': {'module_name': 'mv'},
                        'objectstore': {'module_name': 'objectstore'},
                        's3': {'module_name': 's3'},
                        'gs': {'module_name': 'gs'},
                        'lsm': {'module_name': 'lsm'}
                        }

    # list of allowed schemas to be used for direct acccess mode from REMOTE replicas
    direct_remoteinput_allowed_schemas = ['root', 'https']
    # list of schemas to be used for direct acccess mode from LOCAL replicas
    direct_localinput_allowed_schemas = ['root', 'dcache', 'dcap', 'file', 'https']
    # list of allowed schemas to be used for transfers from REMOTE sites
    remoteinput_allowed_schemas = ['root', 'gsiftp', 'dcap', 'srm', 'storm', 'https']

    def __init__(self,
                 infosys_instance: Any = None,
                 acopytools: dict = None,
                 logger: Any = None,
                 default_copytools: str = 'rucio',
                 trace_report: dict = None,
                 ipv: str = 'IPv6',
                 workdir: str = "") -> None:
        """Initialize common staging client state.

        If ``acopytools`` is not provided it is resolved automatically from
        ``infosys.queuedata``.  When using auto-resolution the ``infosys``
        instance must already be initialized.

        Args:
            infosys_instance: Info service instance used for queue/storage data
                resolution.  Defaults to the module-level ``infosys`` singleton.
            acopytools: Mapping of activity name to ordered list of copytool
                names, e.g. ``{'read_lan': ['rucio'], 'default': ['gfal']}``.
                Also accepts a plain list or a single string, which are wrapped
                into ``{'default': [...]}`` automatically.
            logger: Logger instance to use.  If ``None`` a disabled logger is
                created so all log calls become no-ops.
            default_copytools: Copytool name(s) to fall back to when the
                requested activity is not found in ``acopytools``.  Accepts
                either a list or a single string.
            trace_report: Pre-initialized trace report object.  A new
                :class:`~pilot.util.tracereport.TraceReport` is created when
                ``None``.
            ipv: Internet protocol version string passed to the trace report.
                Defaults to ``"IPv6"``.
            workdir: Working directory passed to the trace report.

        Raises:
            PilotException: If no copytool configuration can be resolved.
        """
        super().__init__()

        if not logger:
            logger = logging.getLogger(__name__ + '.null')
            logger.disabled = True

        self.logger = logger
        self.infosys = infosys_instance or infosys
        self.ipv = ipv
        self.workdir = workdir

        if isinstance(acopytools, str):
            acopytools = {'default': [acopytools]} if acopytools else {}

        if isinstance(acopytools, (list, tuple)):
            acopytools = {'default': acopytools} if acopytools else {}

        self.acopytools = acopytools or {}

        if self.infosys.queuedata:
            self.set_acopytools()

        if not self.acopytools.get('default'):
            self.acopytools['default'] = self.get_default_copytools(default_copytools)

        # get an initialized trace report (has to be updated for get/put if not defined before)
        self.trace_report = trace_report if trace_report else TraceReport(pq=os.environ.get('PILOT_SITENAME', ''), ipv=self.ipv, workdir=self.workdir)

        if not self.acopytools:
            msg = f'failed to initialize StagingClient: no acopytools options found, acopytools={self.acopytools}'
            logger.error(msg)
            self.trace_report.update(clientState='BAD_COPYTOOL', stateReason=msg)
            self.trace_report.send()
            raise PilotException("failed to resolve acopytools settings")
        logger.info('configured copytools per activity: acopytools=%s', self.acopytools)

    def allow_mvfinaldest(self, catchall: str) -> bool:
        """Check whether the catchall string enables mv-to-final-destination mode.

        Args:
            catchall: The ``catchall`` string from queuedata.

        Returns:
            bool: ``True`` if ``"mv_final_destination"`` is present in
                ``catchall``, ``False`` otherwise.
        """
        return catchall and 'mv_final_destination' in catchall

    def set_acopytools(self) -> None:
        """Resolve and set ``acopytools`` from queuedata if not already configured.

        Tries ``queuedata.acopytools`` first; falls back to the keys of
        ``queuedata.copytools`` wrapped in a ``"default"`` activity.
        """
        if not self.acopytools:  # resolve from queuedata.acopytools using infosys
            self.acopytools = (self.infosys.queuedata.acopytools or {}).copy()
        if not self.acopytools:  # resolve from queuedata.copytools using infosys
            self.acopytools = {"default": list((self.infosys.queuedata.copytools or {}).keys())}

    @staticmethod
    def get_default_copytools(default_copytools: str) -> list:
        """Return the default copytools as a list.

        Args:
            default_copytools: A single copytool name string, or an already-
                converted list.  An empty string returns an empty list.

        Returns:
            list: The copytool name(s) as a list.
        """
        if isinstance(default_copytools, str):
            default_copytools = [default_copytools] if default_copytools else []

        return default_copytools

    @classmethod
    def get_preferred_replica(cls, replicas: list, allowed_schemas: list) -> Optional[Any]:
        """Return the first replica whose PFN matches one of the allowed schemas.

        Args:
            replicas: Ordered list of replica info dicts, each containing at
                least a ``"pfn"`` key.
            allowed_schemas: URL schemas to match against (e.g. ``["root",
                "https"]``).  An empty schema string matches any PFN.

        Returns:
            Optional[Any]: The first matching replica dict, or ``None`` if no
                replica matches any of the allowed schemas.
        """
        for replica in replicas:
            pfn = replica.get('pfn')
            for schema in allowed_schemas:
                if pfn and (not schema or pfn.startswith(f'{schema}://')):
                    return replica
        return None

    def prepare_sources(self, files: list, activities: Any = None) -> None:
        """Prepare source file metadata before transfer (base implementation, no-op).

        Subclasses may override this to customize source data for each entry in
        ``files``, optionally using ``activities`` to resolve storage endpoints.

        Args:
            files: List of :class:`~pilot.info.filespec.FileSpec` objects to
                be prepared.
            activities: Activity name or ordered list of activity names used to
                resolve ``astorages``.  Unused in this base implementation.
        """
        return None

    def prepare_inputddms(self, files: list, activities: list = None) -> None:
        """Populate ``fspec.inputddms`` for each file from the site's associated storages.

        Resolves the preferred input DDM endpoints for each activity in
        ``activities`` using ``queuedata.astorages``.  Falls back to
        ``fspec.ddmendpoint`` when no storages are configured.

        Args:
            files: List of :class:`~pilot.info.filespec.FileSpec` objects whose
                ``inputddms`` attribute will be populated.
            activities: Ordered list of activity names used to resolve
                ``astorages``.  Defaults to ``["read_lan"]``.
        """
        astorages = self.infosys.queuedata.astorages if self.infosys and self.infosys.queuedata else {}
        activities = activities or ['read_lan']

        storages = next((astorages.get(a) for a in activities if astorages.get(a)), None) or []

        #if not storages:  ## ignore empty astorages
        #    activity = activities[0]
        #    raise PilotException("Failed to resolve input sources: no associated storages defined for activity=%s (%s)"
        #                         % (activity, ','.join(activities)), code=ErrorCodes.NOSTORAGE, state='NO_ASTORAGES_DEFINED')

        for fdat in files:
            if not fdat.inputddms:
                fdat.inputddms = storages
            if not fdat.inputddms and fdat.ddmendpoint:
                fdat.inputddms = [fdat.ddmendpoint]

    def print_replicas(self, replicas: list, label: str = 'unsorted') -> None:
        """Log a labelled summary of replica PFNs for debugging.

        Prints at most 10 replicas at DEBUG level.

        Args:
            replicas: List of ``(pfn, replica_data)`` tuples to log.
            label: Descriptive label shown in the log header.  Defaults to
                ``"unsorted"``.
        """
        number = 1
        maxnumber = 10
        self.logger.debug(f'{label} list of replicas: (max {maxnumber})')
        for pfn, xdat in replicas:
            self.logger.debug(f"{number}. "
                              f"lfn={pfn}, "
                              f"rse={xdat.get('ddmendpoint')}, "
                              f"domain={xdat.get('domain')}")
            number += 1
            if number > maxnumber:
                break

    @classmethod
    def sort_replicas(cls, replicas: list, inputddms: list) -> list:
        """Sort replicas so that preferred DDM endpoints appear first.

        Replicas whose RSE is listed in ``inputddms`` are moved to the front
        while preserving their relative order within each group.  Replicas not
        in ``inputddms`` are appended in their original order.

        Args:
            replicas: Prioritized list of ``(pfn, replica_data)`` tuples as
                returned by Rucio.
            inputddms: Ordered list of preferred DDM endpoint names (LAN-local
                endpoints first).

        Returns:
            list: Re-ordered list of ``(pfn, replica_data)`` tuples.
        """
        if not inputddms:
            return replicas

        # group replicas by ddmendpoint to properly consider priority of inputddms
        ddmreplicas = {}
        for pfn, xdat in replicas:
            ddmreplicas.setdefault(xdat.get('rse'), []).append((pfn, xdat))

        # process LAN first (keep fspec.inputddms priorities)
        xreplicas = []
        for ddm in inputddms:
            xreplicas.extend(ddmreplicas.get(ddm) or [])

        for pfn, xdat in replicas:
            if (pfn, xdat) in xreplicas:
                continue
            xreplicas.append((pfn, xdat))

        return xreplicas

    def resolve_replicas(self, files: list, use_vp: bool = False) -> list:
        """Populate ``fspec.replicas`` for each file by querying the replica catalog.

        After the call each file's ``replicas`` attribute contains a list of
        dicts with the structure::

            {'ddmendpoint': str, 'pfn': str, 'domain': str}

        File size and checksum values are cross-checked against the catalog and
        a warning is logged on any mismatch.

        Args:
            files: List of :class:`~pilot.info.filespec.FileSpec` objects to
                resolve.
            use_vp: Set to ``True`` for VP (Virtual Placement) jobs, which
                restricts replicas to ``root://`` schema and non-tape RSEs.

        Raises:
            Exception: Re-raises any exception from :meth:`list_replicas`.

        Returns:
            list: The same ``files`` list with ``replicas`` populated.
        """
        logger = self.logger
        xfiles = list(files)

        if not xfiles:  # no files for replica look-up
            return files

        # get the list of replicas
        replicas = self.list_replicas(xfiles, use_vp)

        files_lfn = dict(((e.scope, e.lfn), e) for e in xfiles)
        for replica in replicas:
            k = replica['scope'], replica['name']
            fdat = files_lfn.get(k)
            if not fdat:  # not requested replica
                continue

            # add the replicas to the fdat structure
            fdat = self.add_replicas(fdat, replica)

            # verify filesize and checksum values
            self.trace_report.update(validateStart=time.time())
            status = True
            if fdat.filesize != replica['bytes']:
                logger.warning("Filesize of input file=%s mismatched with value from Rucio replica: filesize=%s, replica.filesize=%s, fdat=%s",
                               fdat.lfn, fdat.filesize, replica['bytes'], fdat)
                status = False

            if not fdat.filesize:
                fdat.filesize = replica['bytes']
                logger.warning("Filesize value for input file=%s is not defined, assigning info from Rucio replica: filesize=%s", fdat.lfn, replica['bytes'])

            for ctype in ('adler32', 'md5'):
                if fdat.checksum.get(ctype) != replica[ctype] and replica[ctype]:
                    logger.warning("Checksum value of input file=%s mismatched with info got from Rucio replica: checksum=%s, replica.checksum=%s, fdat=%s",
                                   fdat.lfn, fdat.checksum, replica[ctype], fdat)
                    status = False

                if not fdat.checksum.get(ctype) and replica[ctype]:
                    fdat.checksum[ctype] = replica[ctype]

            if not status:
                logger.warning("filesize and/or checksum verification failed")
                self.trace_report.update(clientState="DONE")

        logger.info('Number of resolved replicas:\n' +
                    '\n'.join([f"lfn={f.lfn}: nr replicas={len(f.replicas or [])}, is_directaccess={f.is_directaccess(ensure_replica=False)}" for f in files]))

        return files

    def _list_replicas_chunk(self, rucio_client, query: dict, chunk_idx: int,
                             n_chunks: int, retriable_exceptions: tuple) -> list:
        """
        Call rucio.list_replicas() for a single chunk with retry logic.

        Args:
            rucio_client: Rucio Client instance.
            query: Complete query dict including the 'dids' key for this chunk.
            chunk_idx: 1-based index of this chunk (for log messages).
            n_chunks: Total number of chunks (for log messages).
            retriable_exceptions: Tuple of exception types that trigger a retry.

        Raises:
            PilotException: If all retry attempts are exhausted or an unexpected
                exception is raised.

        Returns:
            list: Raw replica dicts for this chunk.
        """
        for attempt in range(1, _RUCIO_MAX_ATTEMPTS + 1):
            try:
                chunk_replicas = list(rucio_client.list_replicas(**query))
                self.logger.debug(
                    'chunk %d/%d: received %d replica(s) from Rucio',
                    chunk_idx, n_chunks, len(chunk_replicas),
                )
                return chunk_replicas

            except retriable_exceptions as exc:
                self.logger.warning(
                    'Chunk %d/%d, attempt %d/%d: transient error while listing replicas: %s',
                    chunk_idx, n_chunks, attempt, _RUCIO_MAX_ATTEMPTS, exc,
                )
                if attempt < _RUCIO_MAX_ATTEMPTS:
                    sleep_for = _RUCIO_BACKOFF_BASE_SECONDS ** (attempt - 1)
                    self.logger.info('Retrying after %s seconds...', sleep_for)
                    time.sleep(sleep_for)
                else:
                    raise PilotException(
                        f'Failed to get replicas from Rucio after {_RUCIO_MAX_ATTEMPTS} attempts '
                        f'(chunk {chunk_idx}/{n_chunks}): {exc}',
                        code=ErrorCodes.RUCIOLISTREPLICASFAILED,
                    ) from exc

            except Exception as exc:
                self.logger.exception(
                    'Unexpected exception while listing replicas from Rucio (chunk %d/%d).',
                    chunk_idx, n_chunks,
                )
                raise PilotException(
                    f'Failed to get replicas from Rucio (chunk {chunk_idx}/{n_chunks}): {exc}',
                    code=ErrorCodes.RUCIOLISTREPLICASFAILED,
                ) from exc

    def list_replicas(self, xfiles: list, use_vp: bool) -> list:
        """Query Rucio for all available replicas of the given files.

        Wraps ``rucio.client.Client.list_replicas()``.  The query requests
        geo-IP sorted results and a 24-hour signature lifetime.  For VP jobs
        the schema is restricted to ``root://`` and availability checks are
        enforced.

        Large file lists are automatically split into chunks of
        _RUCIO_LIST_REPLICAS_CHUNK_SIZE to avoid Rucio server-side limits.
        Results from all chunks are merged before returning.

        Args:
            xfiles: List of :class:`~pilot.info.filespec.FileSpec` objects to
                look up.
            use_vp: Set to ``True`` for VP jobs to apply VP-specific query
                parameters.

        Raises:
            PilotException: If the Rucio call fails.

        Returns:
            list: Raw replica dicts as returned by the Rucio client.
        """
        from rucio.client import Client

        # Optional imports (do NOT require requests to exist)
        retriable_exceptions = []
        try:
            import urllib3
            retriable_exceptions.append(urllib3.exceptions.ProtocolError)
        except Exception:
            pass

        try:
            from requests.exceptions import ChunkedEncodingError, RequestException
            retriable_exceptions.extend([ChunkedEncodingError, RequestException])
        except Exception:
            pass

        try:
            retriable_exceptions.append(ConnectionError)
            retriable_exceptions.append(TimeoutError)
        except Exception:
            pass

        retriable_exceptions = tuple(retriable_exceptions) if retriable_exceptions else (Exception,)
        rucio_client = Client()

        location, diagnostics = self.detect_client_location(use_vp=use_vp)
        if diagnostics:
            self.logger.warning(f'failed to get client location for rucio: {diagnostics}')

        base_query = {
            'schemes': ['srm', 'root', 'davs', 'gsiftp', 'https', 'storm', 'file'],
            'sort': 'geoip',
            'client_location': location,
            'signature_lifetime': 24 * 3600,
        }
        if use_vp:
            base_query['schemes'] = ['root']
            base_query['rse_expression'] = 'istape=False\\type=SPECIAL'
            base_query['ignore_availability'] = False

        # Split into chunks to avoid Rucio server-side limits with large input lists
        chunks = [xfiles[i:i + _RUCIO_LIST_REPLICAS_CHUNK_SIZE]
                  for i in range(0, len(xfiles), _RUCIO_LIST_REPLICAS_CHUNK_SIZE)]
        n_chunks = len(chunks)
        if n_chunks > 1:
            self.logger.info(f'splitting {len(xfiles)} files into {n_chunks} '
                             f'chunks of up to {_RUCIO_LIST_REPLICAS_CHUNK_SIZE} '
                             f'for rucio.list_replicas()')

        all_replicas = []
        for chunk_idx, chunk in enumerate(chunks, start=1):
            query = dict(base_query)
            query['dids'] = [{'scope': e.scope, 'name': e.lfn} for e in chunk]
            if n_chunks > 1:
                self.logger.info(
                    'calling rucio.list_replicas() for chunk %d/%d (%d files)',
                    chunk_idx, n_chunks, len(chunk),
                )
            else:
                self.logger.info('calling rucio.list_replicas() with query=%s', query)
            all_replicas.extend(
                self._list_replicas_chunk(rucio_client, query, chunk_idx, n_chunks, retriable_exceptions)
            )

        self.logger.debug('rucio.list_replicas() total replicas received: %d', len(all_replicas))
        return all_replicas

    def add_replicas(self, fdat: Any, replica: Any) -> Any:
        """Populate ``fdat.replicas`` from a raw Rucio replica record.

        Only DISK-type replicas are kept.  The ``domain`` field is overridden
        to ``"lan"`` when the replica's RSE is in ``fdat.inputddms``,
        otherwise ``"wan"``.  Replicas that conflict with the file's
        ``allow_lan`` / ``allow_wan`` settings are skipped.

        Args:
            fdat: :class:`~pilot.info.filespec.FileSpec` object to update.
            replica: Raw Rucio replica dict containing ``"pfns"``, ``"scope"``,
                ``"name"``, and checksum fields.

        Returns:
            Any: The updated ``fdat`` object.
        """
        fdat.replicas = []  # reset replicas list

        # sort replicas by priority value
        sorted_replicas = sorted(iter(list(replica.get('pfns', {}).items())), key=lambda x: x[1]['priority'])

        # prefer replicas from inputddms first
        #self.print_replicas(sorted_replicas)
        xreplicas = self.sort_replicas(sorted_replicas, fdat.inputddms)
        self.print_replicas(xreplicas)

        for pfn, xdat in xreplicas:

            if xdat.get('type') != 'DISK':  # consider only DISK replicas
                continue

            rinfo = {'pfn': pfn, 'ddmendpoint': xdat.get('rse'), 'domain': xdat.get('domain')}

            ## (TEMPORARY?) consider fspec.inputddms as a primary source for local/lan source list definition
            ## backward compartible logic -- FIX ME LATER if NEED
            ## in case we should rely on domain value from Rucio, just remove the overwrite line below
            rinfo['domain'] = 'lan' if rinfo['ddmendpoint'] in fdat.inputddms else 'wan'

            if not fdat.allow_lan and rinfo['domain'] == 'lan':
                continue
            if not fdat.allow_wan and rinfo['domain'] == 'wan':
                continue

            fdat.replicas.append(rinfo)

        if not fdat.replicas:
            name = replica.get('name', '[unknown lfn]')
            self.logger.warning(
                f'{name}: no replicas were selected (verify replica type, read_lan/wan, allow_lan/wan and domain values)'
            )
            self.logger.warning('e.g. check that read_lan is set for the relevant RSE in CRIC')

        return fdat

    def detect_client_location(self, use_vp: bool = False) -> tuple:
        """Detect the worker node's network location for Rucio geo-sorting.

        Opens a UDP socket toward the public internet to discover the local IP
        address.  For VP sites, geographic coordinates are obtained from the
        ``RUCIO_LATITUDE`` / ``RUCIO_LONGITUDE`` environment variables or by
        querying an external geo-IP service.

        Args:
            use_vp: Set to ``True`` for VP (Virtual Placement) sites to also
                resolve geographic coordinates.

        Returns:
            tuple: A two-element tuple ``(client_location, diagnostics)`` where
                ``client_location`` is a dict with at least ``"ip"`` and
                ``"site"`` keys (and optionally ``"latitude"`` / ``"longitude"``
                for VP sites), and ``diagnostics`` is an error string (empty
                when no errors occurred).
        """
        diagnostics = ''
        client_location = {}

        ip = '0.0.0.0'
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                s.connect(("8.8.8.8", 80))
                ip = s.getsockname()[0]
            finally:
                s.close()
        except socket.gaierror as e:
            diagnostics = f'failed to get socket info due to address-related error: {e}'
            self.logger.warning(diagnostics)
        except socket.timeout as e:
            diagnostics = f'failed to get socket info due to timeout: {e}'
            self.logger.warning(diagnostics)
        except socket.error as e:
            diagnostics = f'failed to get socket info due to general socket error: {e}'
            self.logger.warning(diagnostics)

        client_location['ip'] = ip
        site = os.environ.get('PILOT_RUCIO_SITENAME', 'unknown')
        client_location['site'] = site

        if use_vp:
            latitude = os.environ.get('RUCIO_LATITUDE')
            longitude = os.environ.get('RUCIO_LONGITUDE')
            if latitude and longitude:
                try:
                    client_location['latitude'] = float(latitude)
                    client_location['longitude'] = float(longitude)
                except ValueError:
                    diagnostics = f'client set latitude (\"{latitude}\") and longitude (\"{longitude}\") are not valid'
                    self.logger.warning(diagnostics)
            elif requests is not None:
                try:
                    response = requests.post('https://location.cern.workers.dev',
                                             json={"site": site},
                                             timeout=10)
                    if response.status_code == 200 and 'application/json' in response.headers.get('Content-Type', ''):
                        client_location = response.json()
                        # put back the site
                        client_location['site'] = site
                except requests.exceptions.Timeout as exc:
                    diagnostics = f'requests.post timed out: {exc}'
                    self.logger.warning(diagnostics)
                except requests.exceptions.RequestException as exc:
                    diagnostics = f'requests.post failed with general exception: {exc}'
                    self.logger.warning(diagnostics)
            else:
                self.logger.warning('requests module is not available: cannot determine VP client location')

        self.logger.debug(f'will use client_location={client_location}')
        return client_location, diagnostics

    def resolve_surl(self, fspec: Any, protocol: dict, ddmconf: dict, **kwargs: Any) -> dict:
        """Resolve the final storage URL (SURL) for a file transfer.

        This is an abstract method that must be implemented by subclasses.
        The concrete implementation lives in :class:`StageOutClient`.

        Args:
            fspec: :class:`~pilot.info.filespec.FileSpec` object for the file
                being transferred.
            protocol: Protocol dict (endpoint, path, flavour) selected for the
                transfer.
            ddmconf: Full DDM configuration mapping endpoint names to storage
                data objects.
            **kwargs: Additional keyword arguments forwarded by the caller.

        Raises:
            NotImplementedError: Always, in this base implementation.

        Returns:
            dict: Dict with at least a ``"surl"`` key and optionally a
                ``"ddmendpoint"`` key.
        """
        raise NotImplementedError()

    def transfer_files(self, copytool: Any, files: list, activity: list, **kwargs: Any) -> list:
        """Execute the actual file transfer using the given copytool.

        This is an abstract method that must be implemented by subclasses
        (:meth:`StageInClient.transfer_files`,
        :meth:`StageOutClient.transfer_files`).

        Args:
            copytool: Imported copytool module (e.g. ``pilot.copytool.rucio``).
            files: List of :class:`~pilot.info.filespec.FileSpec` objects to
                transfer.
            activity: Ordered list of activity names used to select the
                appropriate copytool.
            **kwargs: Extra keyword arguments forwarded to the copytool handler.

        Raises:
            NotImplementedError: Always, in this base implementation.
            PilotException: Subclass implementations raise this on controlled
                transfer errors.
        """
        raise NotImplementedError()

    def transfer(self, files: list, activity: Union[list, str] = 'default', raise_exception: bool = True, **kwargs: Any) -> list:  # noqa: C901
        """Stage files using the copytools configured for the given activity.

        Resolves the copytool list for ``activity`` (falling back to
        ``"default"``), prepares input DDMs and file activities, then iterates
        through the copytools until one succeeds or all fail.

        Args:
            files: List of :class:`~pilot.info.filespec.FileSpec` objects to
                transfer.
            activity: Activity name or ordered list of activity names used to
                select the copytool(s).  ``"default"`` is always appended as a
                final fallback.
            raise_exception: When ``True`` a :exc:`PilotException` is raised if
                any files remain un-transferred after all copytools are
                exhausted.  Set to ``False`` to suppress the exception and
                inspect ``fspec.status`` instead.
            **kwargs: Extra keyword arguments forwarded to the copytool handler.

        Raises:
            PilotException: If no copytool can be resolved for the activity, or
                if ``raise_exception`` is ``True`` and files remain
                un-transferred.

        Returns:
            list: The same ``files`` list with updated ``status`` attributes.
        """
        self.trace_report.update(relativeStart=time.time(), transferStart=time.time())

        if isinstance(activity, str):
            activity = [activity]
        if 'default' not in activity:
            activity.append('default')

        copytools = None
        for aname in activity:
            copytools = self.acopytools.get(aname)
            if copytools:
                break

        if not copytools:
            raise PilotException(f'failed to resolve copytool by preferred activities={activity}, acopytools={self.acopytools}',
                                 code=ErrorCodes.UNKNOWNCOPYTOOL)

        # populate inputddms if needed
        self.prepare_inputddms(files)

        # initialize ddm_activity name for requested files if not set
        for fspec in files:
            if fspec.ddm_activity:  # skip already initialized data
                continue
            if self.mode == 'stage-in':
                if os.environ.get('PILOT_ES_EXECUTOR_TYPE', 'generic') == 'raythena':
                    fspec.status = 'no_transfer'

                fspec.ddm_activity = [_f for _f in
                                      ['read_lan' if fspec.ddmendpoint in fspec.inputddms else None, 'read_wan'] if _f]
            else:
                fspec.ddm_activity = [_f for _f in
                                      ['write_lan' if fspec.ddmendpoint in fspec.inputddms else None, 'write_wan'] if _f]
        caught_errors = []

        for name in copytools:

            # get remain files that need to be transferred by copytool
            remain_files = [e for e in files if e.status not in ['remote_io', 'transferred', 'no_transfer']]

            if not remain_files:
                break

            try:
                if name not in self.copytool_modules:
                    raise PilotException(f'passed unknown copytool with name={name} .. skipped',
                                         code=ErrorCodes.UNKNOWNCOPYTOOL)

                module = self.copytool_modules[name]['module_name']
                self.logger.info(f'trying to use copytool={name} for activity={activity}')
                copytool = __import__(f'pilot.copytool.{module}', globals(), locals(), [module], 0)
                #self.trace_report.update(protocol=name)

            except PilotException as exc:
                caught_errors.append(exc)
                self.logger.debug(f'error: {exc}')
                continue
            except Exception as exc:
                self.logger.warning(f'failed to import copytool module={module}, error={exc}')
                continue

            try:
                result = self.transfer_files(copytool, remain_files, activity, **kwargs)
                self.logger.debug(f'transfer_files() using copytool={copytool} completed with result={result}')
                break
            except PilotException as exc:
                self.logger.warning(f'failed to transfer_files() using copytool={copytool} .. skipped; error={exc}')
                caught_errors.append(exc)
            except TimeoutException as exc:
                self.logger.warning(f'function timed out: {exc}')
                caught_errors.append(exc)
            except Exception as exc:
                self.logger.warning(f'failed to transfer files using copytool={copytool} .. skipped; error={exc}')
                caught_errors.append(exc)
                import traceback
                self.logger.error(traceback.format_exc())

            if caught_errors and isinstance(caught_errors[-1], PilotException) and \
                    caught_errors[-1].get_error_code() == ErrorCodes.MISSINGOUTPUTFILE:
                raise caught_errors[-1]

        remain_files = [fspec for fspec in files if fspec.status not in ['remote_io', 'transferred', 'no_transfer']]

        if remain_files:  # failed or incomplete transfer
            # propagate message from first error back up
            # errmsg = str(caught_errors[0]) if caught_errors else ''
            if caught_errors and isinstance(caught_errors[0], PilotException):
                code = caught_errors[0].get_error_code()
                # errmsg = caught_errors[0].get_last_error()
            elif caught_errors and isinstance(caught_errors[0], TimeoutException):
                code = ErrorCodes.STAGEINTIMEOUT if self.mode == 'stage-in' else ErrorCodes.STAGEOUTTIMEOUT  # is it stage-in/out?
                self.logger.warning(f'caught time-out exception: {caught_errors[0]}')
            elif caught_errors and "Cannot authenticate" in str(caught_errors):
                code = ErrorCodes.STAGEINAUTHENTICATIONFAILURE if self.mode == 'stage-in' else ErrorCodes.STAGEOUTAUTHENTICATIONFAILURE  # is it stage-in/out?
            elif caught_errors and "bad queue configuration" in str(caught_errors):
                code = ErrorCodes.BADQUEUECONFIGURATION
            else:
                code = ErrorCodes.STAGEINFAILED if self.mode == 'stage-in' else ErrorCodes.STAGEOUTFAILED  # is it stage-in/out?
            details = str(caught_errors) + ":" + f'failed to transfer files using copytools={copytools}'
            self.logger.fatal(details)
            if raise_exception:
                raise PilotException(details, code=code)

        return files

    def require_protocols(self, files: list, copytool: Any, activity: Union[list, str], local_dir: str = '') -> None:
        """Resolve and assign transfer protocols and TURLs for each file.

        Populates ``fspec.protocols`` and ``fspec.turl`` for each entry in
        ``files`` according to the preferred ``fspec.ddm_activity``.  When
        ``local_dir`` is provided, a synthetic local-file protocol is used
        instead of querying the storage configuration.

        Args:
            files: List of :class:`~pilot.info.filespec.FileSpec` objects to
                update.
            copytool: Imported copytool module whose ``allowed_schemas``
                attribute (if present) constrains protocol selection.
            activity: Activity name or ordered list of activity names used to
                resolve allowed schemas from queuedata.
            local_dir: If non-empty, all files are assigned this directory as
                their protocol endpoint instead of using DDM configuration.
        """
        allowed_schemas = getattr(copytool, 'allowed_schemas', None)

        if self.infosys and self.infosys.queuedata:
            copytool_name = copytool.__name__.rsplit('.', 1)[-1]
            allowed_schemas = self.infosys.queuedata.resolve_allowed_schemas(activity, copytool_name) or allowed_schemas

        if local_dir:
            if not local_dir.endswith('/'):
                local_dir += '/'
            for fdat in files:
                fdat.protocols = [{'endpoint': local_dir, 'flavour': '', 'id': 0, 'path': ''}]
        else:
            files = self.resolve_protocols(files)

        ddmconf = self.infosys.resolve_storage_data()

        for fspec in files:

            protocols = self.resolve_protocol(fspec, allowed_schemas)
            if not protocols and 'mv' not in self.infosys.queuedata.copytools:  # no protocols found
                error = f'Failed to resolve protocol for file={fspec.lfn}, allowed_schemas={allowed_schemas}, fspec={fspec}'
                self.logger.error(f"resolve_protocol: {error}")
                raise PilotException(error, code=ErrorCodes.NOSTORAGEPROTOCOL)

            # take first available protocol for copytool: FIX ME LATER if need (do iterate over all allowed protocols?)
            protocol = protocols[0]

            self.logger.info(f"Resolved protocol to be used for transfer: \'{protocol}\': lfn=\'{fspec.lfn}\'")

            resolve_surl = getattr(copytool, 'resolve_surl', None)
            if not callable(resolve_surl):
                resolve_surl = self.resolve_surl

            r = resolve_surl(fspec, protocol, ddmconf, local_dir=local_dir)  # pass ddmconf for possible custom look up at the level of copytool
            if r.get('surl'):
                fspec.turl = r['surl']

            if r.get('ddmendpoint'):
                fspec.ddmendpoint = r['ddmendpoint']

    def resolve_protocols(self, files: list) -> list:
        """Populate ``fspec.protocols`` for each file from the DDM configuration.

        Resolves the list of available transfer protocols for each file based on
        its ``ddm_activity`` and the ``arprotocols`` defined in the DDM storage
        configuration.  The result is stored as::

            fdat.protocols = [{'endpoint': str, 'path': str, 'flavour': str}, ...]

        Args:
            files: List of :class:`~pilot.info.filespec.FileSpec` objects whose
                ``protocols`` attribute will be populated.

        Raises:
            PilotException: If the file's DDM endpoint cannot be found in the
                storage configuration.

        Returns:
            list: The same ``files`` list with ``protocols`` populated.
        """
        ddmconf = self.infosys.resolve_storage_data()

        for fdat in files:
            ddm = ddmconf.get(fdat.ddmendpoint)
            if not ddm:
                error = f'Failed to resolve output ddmendpoint by name={fdat.ddmendpoint} (from PanDA), please check configuration.'
                self.logger.error(f"resolve_protocols: {error}, fspec={fdat}")
                raise PilotException(error, code=ErrorCodes.NOSTORAGE)

            protocols = []
            for aname in fdat.ddm_activity:
                protocols = ddm.arprotocols.get(aname)
                if protocols:
                    break

            fdat.protocols = protocols

        return files

    @classmethod
    def resolve_protocol(cls, fspec: Any, allowed_schemas: Any = None) -> list:
        """Filter ``fspec.protocols`` to those matching the allowed schemas.

        Args:
            fspec: :class:`~pilot.info.filespec.FileSpec` instance whose
                ``protocols`` list is filtered.
            allowed_schemas: List of URL schema strings (e.g. ``["root",
                "https"]``) to accept.  ``None`` accepts all schemas.

        Returns:
            list: Subset of ``fspec.protocols`` whose endpoints start with one
                of the allowed schemas, or all protocols when ``allowed_schemas``
                is ``None``.
        """
        if not fspec.protocols:
            return []

        protocols = []

        allowed_schemas = allowed_schemas or [None]
        for schema in allowed_schemas:
            for pdat in fspec.protocols:
                if schema is None or pdat.get('endpoint', '').startswith(f"{schema}://"):
                    protocols.append(pdat)

        return protocols


class StageInClient(StagingClient):
    """Stage-in client for copying input files to the worker node.

    Extends :class:`StagingClient` with replica resolution logic that selects
    the best available source URL for each input file, taking into account LAN
    vs. WAN proximity, direct-access (remote I/O) eligibility, allowed URL
    schemas, and VP-job constraints.  Files approved for direct access are
    marked with ``status="remote_io"`` and are not physically copied.
    """

    mode = "stage-in"

    def resolve_replica(self, fspec: Any, primary_schemas: Any = None, allowed_schemas: Any = None, domain: Any = None) -> Optional[dict]:
        """Select the best available replica for a single input file.

        First attempts to find a replica matching ``primary_schemas`` within the
        requested ``domain`` (priority is ignored for primary schemas, making
        this suitable for direct-access URL selection).  If not found, falls
        back to any replica in ``allowed_schemas`` within the same domain.

        Args:
            fspec: :class:`~pilot.info.filespec.FileSpec` object whose
                ``replicas`` list is searched.
            primary_schemas: Preferred URL schemas tried first (ignoring Rucio
                priority order).  ``None`` skips the primary lookup.
            allowed_schemas: Fallback URL schemas.  ``None`` accepts any schema.
            domain: Domain to restrict the search to (``"lan"`` or ``"wan"``).

        Returns:
            Optional[dict]: Dict with keys ``"surl"``, ``"ddmendpoint"``,
                ``"pfn"``, and ``"domain"`` for the chosen replica, or ``None``
                if no suitable replica was found.
        """
        if not fspec.replicas:
            self.logger.warning('resolve_replica() received no fspec.replicas')
            return None

        allowed_schemas = allowed_schemas or [None]
        primary_replica, replica = None, None

        # group by ddmendpoint to look up related surl/srm value
        replicas = {}

        for rinfo in fspec.replicas:

            replicas.setdefault(rinfo['ddmendpoint'], []).append(rinfo)

            if rinfo['domain'] != domain:
                continue
            if primary_schemas and not primary_replica:  # look up primary schemas if requested
                primary_replica = self.get_preferred_replica([rinfo], primary_schemas)
            if not replica:
                replica = self.get_preferred_replica([rinfo], allowed_schemas)

            if replica and primary_replica:
                break

        replica = primary_replica or replica

        if not replica:  # replica not found
            schemas = 'any' if not allowed_schemas[0] else ','.join(allowed_schemas)
            pschemas = 'any' if primary_schemas and not primary_schemas[0] else ','.join(primary_schemas or [])

            error = f'Failed to find replica for file={fspec.lfn}, domain={domain}, allowed_schemas={schemas}, pschemas={pschemas}, fspec={fspec}'
            self.logger.info("resolve_replica: %s", error)
            return None

        # prefer SRM protocol for surl -- to be verified, can it be deprecated?
        rse_replicas = replicas.get(replica['ddmendpoint'], [])
        surl = self.get_preferred_replica(rse_replicas, ['srm']) or rse_replicas[0]
        self.logger.info(f"[stage-in] surl (srm replica) from Rucio: pfn={surl['pfn']}, ddmendpoint={surl['ddmendpoint']}")

        return {'surl': surl['pfn'], 'ddmendpoint': replica['ddmendpoint'], 'pfn': replica['pfn'], 'domain': replica['domain']}

    def resolve_surl(self, fspec: Any, protocol: dict, ddmconf: dict, **kwargs: Any) -> dict:
        """Resolve the source URL for a stage-in transfer (not applicable).

        SURL resolution is only meaningful for stage-out.  This override exists
        solely to satisfy the abstract interface and always raises
        :exc:`NotImplementedError`.

        Args:
            fspec: :class:`~pilot.info.filespec.FileSpec` object.
            protocol: Protocol dict selected for the transfer.
            ddmconf: Full DDM configuration dict.
            **kwargs: Additional keyword arguments (ignored).

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError()

    def get_direct_access_variables(self, job: Any) -> tuple:
        """Return the direct-access (remote I/O) settings for the current queue.

        Checks ``queuedata.direct_access_lan`` and
        ``queuedata.direct_access_wan``.  Direct access is disabled for
        production jobs unless ``job.transfertype`` implies remote I/O.

        Transfer types that enable direct access for production jobs:

        * ``"direct"`` — default remote I/O; uses the queue's default
          protocol priority (``root`` first).
        * ``"root"`` — remote I/O with ``root://`` preferred.
        * ``"davs"`` — remote I/O with ``davs://`` preferred.
        * Comma-separated combinations of the above (e.g. ``"davs,root"``) —
          remote I/O tried in the listed protocol order.

        ``"file"`` (Rucio copy via POSIX link) and ``None``/empty trigger
        copy-to-scratch and do **not** enable direct access for production jobs.

        Args:
            job: Job object.  May be ``None`` when no job context is available.

        Returns:
            tuple: A two-element tuple ``(allow_direct_access, direct_access_type)``
                where ``allow_direct_access`` is a ``bool`` and
                ``direct_access_type`` is ``"LAN"``, ``"WAN"``, or ``""``
                (empty string when direct access is disabled).
        """
        allow_direct_access, direct_access_type = False, ''
        if self.infosys.queuedata:  # infosys is initialized
            allow_direct_access = self.infosys.queuedata.direct_access_lan or self.infosys.queuedata.direct_access_wan
            if self.infosys.queuedata.direct_access_lan:
                direct_access_type = 'LAN'
            if self.infosys.queuedata.direct_access_wan:
                direct_access_type = 'WAN'
        else:
            self.logger.info('infosys.queuedata is not initialized: direct access mode will be DISABLED by default')

        if job and not job.is_analysis() and not is_directio_transfertype(job.transfertype):
            allow_direct_access = False
            self.logger.info(f'switched off direct access mode for production job since transfertype={job.transfertype}')

        return allow_direct_access, direct_access_type

    def transfer_files(self, copytool: Any, files: list, activity: list = None, **kwargs: Any) -> list:  # noqa: C901
        """Stage in files using the given copytool module.

        Orchestrates the full stage-in pipeline for a single copytool:

        1. Resolve Rucio replicas (if the copytool requires them).
        2. Select the best replica per file considering ``transfertype``,
           LAN/WAN preference, and direct-access eligibility.
        3. Resolve input protocols (if required by the copytool).
        4. Mark direct-access files with ``status="remote_io"``.
        5. Check available disk space against the total input size.
        6. Delegate physical transfers to ``copytool.copy_in()``.

        Args:
            copytool: Imported copytool module.
            files: List of :class:`~pilot.info.filespec.FileSpec` objects to
                stage in.
            activity: Ordered list of activity names used to resolve allowed
                schemas.
            **kwargs: Extra keyword arguments forwarded to the copytool.
                Recognized keys include ``use_vp``, ``job``, ``workdir``,
                ``input_dir``, ``args``, and ``catchall``.

        Raises:
            ReplicasNotFound: If no suitable replica is found for a file.
            PilotException: On validation failures or copytool errors.

        Returns:
            list: The updated ``files`` list.
        """
        if getattr(copytool, 'require_replicas', False) and files:
            if files[0].replicas is None:  # look up replicas only once
                files = self.resolve_replicas(files, use_vp=kwargs.get('use_vp', False))

            allowed_schemas = getattr(copytool, 'allowed_schemas', None)

            if self.infosys and self.infosys.queuedata:
                copytool_name = copytool.__name__.rsplit('.', 1)[-1]
                allowed_schemas = self.infosys.queuedata.resolve_allowed_schemas(activity,
                                                                                 copytool_name) or allowed_schemas

            # overwrite allowed_schemas for VP jobs
            if kwargs.get('use_vp', False):
                allowed_schemas = ['root']
                self.logger.debug('overwrote allowed_schemas for VP job: %s', str(allowed_schemas))

            for fspec in files:
                resolve_replica = getattr(copytool, 'resolve_replica', None)
                resolve_replica = self.resolve_replica if not callable(resolve_replica) else resolve_replica

                replica = None

                # --- NEW: prefer schema based on job.transfertype for copy-to-scratch ---
                job = kwargs.get('job')
                ttype = (getattr(job, 'transfertype', '') or '').lower()
                prefer = [ttype] if ttype in ('file', 'root', 'davs') else None

                # don’t interfere with direct I/O behavior
                doing_direct = fspec.is_directaccess(ensure_replica=False) and (fspec.direct_access_lan or fspec.direct_access_wan)

                if prefer and not doing_direct:
                    # try LAN first (if allowed)
                    if fspec.allow_lan:
                        preferred_replica = resolve_replica(fspec, prefer, allowed_schemas, domain='lan')
                        if preferred_replica:
                            replica = preferred_replica
                            self.logger.info('lan replica resolved with preferred schema=%s: %s', prefer, replica)

                    # then WAN (respecting existing WAN schema restriction logic)
                    if not replica and fspec.allow_wan:
                        xschemas = self.remoteinput_allowed_schemas
                        wan_allowed_schemas = [s for s in allowed_schemas if
                                               s in xschemas] if allowed_schemas else xschemas
                        preferred_replica = resolve_replica(fspec, prefer, wan_allowed_schemas, domain='wan')
                        if preferred_replica:
                            replica = preferred_replica
                            self.logger.info('wan replica resolved with preferred schema=%s: %s', prefer, replica)
                # --- end NEW block ---

                # process direct access logic  ## TODO move to upper level, should not be dependent on copytool (anisyonk)

                # check local replicas first
                if fspec.allow_lan:
                    if not replica:
                        # Determine primary schemas for direct-access LAN, honouring transfertype
                        # protocol preference (e.g. 'davs' moves davs:// to the front).
                        if fspec.direct_access_lan and fspec.is_directaccess(ensure_replica=False):
                            primary_schemas = get_directio_preferred_schemas(
                                ttype, self.direct_localinput_allowed_schemas
                            )
                        else:
                            primary_schemas = None
                        replica = resolve_replica(fspec, primary_schemas, allowed_schemas, domain='lan')

                    if not replica:
                        self.logger.info(
                            "[stage-in] No LAN replica found for lfn=%s, primary_schemas=%s, allowed_schemas=%s",
                            fspec.lfn, primary_schemas, allowed_schemas
                        )
                else:
                    self.logger.info(
                        "[stage-in] LAN access is DISABLED for lfn=%s (fspec.allow_lan=%s)",
                        fspec.lfn, fspec.allow_lan
                    )

                # check remote replicas
                if not replica and fspec.allow_wan:
                    # Determine primary schemas for direct-access WAN, honouring transfertype
                    # protocol preference (e.g. 'davs' moves davs:// to the front).
                    if fspec.direct_access_wan and fspec.is_directaccess(ensure_replica=False):
                        primary_schemas = get_directio_preferred_schemas(
                            ttype, self.direct_remoteinput_allowed_schemas
                        )
                    else:
                        primary_schemas = None

                    xschemas = self.remoteinput_allowed_schemas
                    wan_allowed_schemas = [s for s in allowed_schemas if s in xschemas] if allowed_schemas else xschemas

                    replica = resolve_replica(fspec, primary_schemas, wan_allowed_schemas, domain='wan')

                    if not replica:
                        self.logger.info(
                            "[stage-in] No WAN replica found for lfn=%s, primary_schemas=%s, allowed_schemas=%s",
                            fspec.lfn, primary_schemas, wan_allowed_schemas
                        )

                if not replica:
                    raise ReplicasNotFound(
                        f'No replica found for lfn={fspec.lfn} (allow_lan={fspec.allow_lan}, allow_wan={fspec.allow_wan})'
                    )

                if replica.get('pfn'):
                    fspec.turl = replica['pfn']
                if replica.get('surl'):
                    fspec.surl = replica['surl']  # TO BE CLARIFIED if it's still used and need
                if replica.get('ddmendpoint'):
                    fspec.ddmendpoint = replica['ddmendpoint']
                if replica.get('domain'):
                    fspec.domain = replica['domain']

                self.logger.info(
                    "[stage-in] found replica to be used for lfn=%s: ddmendpoint=%s, pfn=%s",
                    fspec.lfn, fspec.ddmendpoint, fspec.turl
                )

        # prepare files (resolve protocol/transfer url)
        if getattr(copytool, 'require_input_protocols', False) and files:
            args = kwargs.get('args')
            input_dir = kwargs.get('input_dir') if not args else args.input_dir
            self.require_protocols(files, copytool, activity, local_dir=input_dir)

        # mark direct access files with status=remote_io
        self.set_status_for_direct_access(files, kwargs.get('workdir', ''))

        # get remain files that need to be transferred by copytool
        remain_files = [e for e in files if e.status not in ['direct', 'remote_io', 'transferred', 'no_transfer']]

        if not remain_files:
            return files

        if not copytool.is_valid_for_copy_in(remain_files):
            msg = f'input is not valid for transfers using copytool={copytool}'
            self.logger.warning(msg)
            self.logger.debug('input: %s', remain_files)
            self.trace_report.update(clientState='NO_REPLICA', stateReason=msg)
            self.trace_report.send()
            raise PilotException('invalid input data for transfer operation')

        if self.infosys:
            if self.infosys.queuedata:
                kwargs['copytools'] = self.infosys.queuedata.copytools
            kwargs['ddmconf'] = self.infosys.resolve_storage_data()
        kwargs['activity'] = activity

        # verify file sizes and available space for stage-in
        if getattr(copytool, 'check_availablespace', True):
            if self.infosys.queuedata.maxinputsize != -1:
                self.check_availablespace(remain_files)
            else:
                self.logger.info('skipping input file size check since maxinputsize=-1')

        # add the trace report
        kwargs['trace_report'] = self.trace_report
        self.logger.info('ready to transfer (stage-in) files: %s', remain_files)

        # is there an override in catchall to allow mv to final destination (relevant for mv copytool only)
        kwargs['mvfinaldest'] = self.allow_mvfinaldest(kwargs.get('catchall', ''))

        # use bulk downloads if necessary
        # if kwargs['use_bulk_transfer']
        # return copytool.copy_in_bulk(remain_files, **kwargs)
        return copytool.copy_in(remain_files, **kwargs)

    def set_status_for_direct_access(self, files: list, workdir: str) -> None:
        """Mark files eligible for direct access with ``status="remote_io"``.

        For each file, evaluates LAN and WAN direct-access conditions.  When
        both ``domain`` and ``direct_access_{lan,wan}`` match, the file status
        is set to ``"remote_io"``, an ``ALRB_XCACHE_PROXY`` prefix is applied
        to the TURL if configured, and a trace report is sent.

        Should be called only once per transfer because it emits trace events.

        Args:
            files: List of :class:`~pilot.info.filespec.FileSpec` objects to
                evaluate and potentially update.
            workdir: Working directory used to locate the base trace report
                file when remote file verification is enabled.
        """
        for fspec in files:
            direct_lan = (fspec.domain == 'lan' and fspec.direct_access_lan and
                          fspec.is_directaccess(ensure_replica=True, allowed_replica_schemas=self.direct_localinput_allowed_schemas))
            direct_wan = (fspec.domain == 'wan' and fspec.direct_access_wan and
                          fspec.is_directaccess(ensure_replica=True, allowed_replica_schemas=self.direct_remoteinput_allowed_schemas))

            # testing direct acess
            #if 'CYFRONET' in os.environ.get('PILOT_SITENAME', ''):
            #    if '.root.' in fspec.lfn:
            #        direct_lan = True

            if not direct_lan and not direct_wan:
                self.logger.debug('direct lan/wan transfer will not be used for lfn=%s', fspec.lfn)
            self.logger.debug('lfn=%s, direct_lan=%s, direct_wan=%s, direct_access_lan=%s, direct_access_wan=%s, '
                              'direct_localinput_allowed_schemas=%s, remoteinput_allowed_schemas=%s, domain=%s',
                              fspec.lfn, direct_lan, direct_wan, fspec.direct_access_lan, fspec.direct_access_wan,
                              str(self.direct_localinput_allowed_schemas), str(self.direct_remoteinput_allowed_schemas), fspec.domain)

            if direct_lan or direct_wan:
                fspec.status_code = 0
                fspec.status = 'remote_io'

                alrb_xcache_proxy = os.environ.get('ALRB_XCACHE_PROXY', None)
                if alrb_xcache_proxy and direct_lan:  #fspec.is_directaccess(ensure_replica=False):
                    fspec.turl = '${ALRB_XCACHE_PROXY}' + fspec.turl

                self.logger.info('stage-in: direct access (remote i/o) will be used for lfn=%s (direct_lan=%s, direct_wan=%s), turl=%s',
                                 fspec.lfn, direct_lan, direct_wan, fspec.turl)

                # send trace
                localsite = os.environ.get('RUCIO_LOCAL_SITE_ID')
                localsite = localsite or fspec.ddmendpoint
                self.trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
                self.trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
                self.trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
                self.trace_report.update(url=fspec.turl, clientState='FOUND_ROOT', stateReason='direct_access')

                # do not send the trace report at this point if remote file verification is to be done
                # note also that we can't verify the files at this point since root will not be available from inside
                # the rucio container
                if config.Pilot.remotefileverification_log:
                    # store the trace report for later use (the trace report class inherits from dict, so just write it as JSON)
                    # outside of the container, it will be available in the normal work dir
                    # use the normal work dir if we are not in a container
                    _workdir = workdir if os.path.exists(workdir) else '.'
                    path = os.path.join(_workdir, config.Pilot.base_trace_report)
                    if not os.path.exists(_workdir):
                        path = os.path.join('/srv', config.Pilot.base_trace_report)
                    if not os.path.exists(path):
                        self.logger.debug(f'writing base trace report to: {path}')
                        write_json(path, self.trace_report)
                else:
                    self.trace_report.send()

    def check_availablespace(self, files: list) -> None:
        """Verify that total input size fits within configured limits and available disk.

        Compares the sum of all file sizes against ``maxinputsize`` and against
        the currently available local disk space (from ``df``).

        Args:
            files: List of :class:`~pilot.info.filespec.FileSpec` objects whose
                ``filesize`` values are summed.

        Raises:
            SizeTooLarge: If the total input size exceeds ``maxinputsize``.
            NoLocalSpace: If the total input size exceeds available local disk
                space.
        """
        for f in files:
            self.logger.debug(f'lfn={f.lfn} filesize={f.filesize} accessmode={f.accessmode}')

        maxinputsize = convert_mb_to_b(get_maximum_input_sizes())
        totalsize = reduce(lambda x, y: x + y.filesize, files, 0)

        # verify total filesize
        if maxinputsize and totalsize > maxinputsize:
            error = f"too many/too large input files ({len(files)}). total file size={totalsize} B > maxinputsize={maxinputsize} B"
            raise SizeTooLarge(error)

        self.logger.info(f"total input file size={totalsize} B within allowed limit={maxinputsize} B (zero value means unlimited)")

        # get available space
        try:
            disk_space = get_local_disk_space(os.getcwd())
        except PilotException as exc:
            diagnostics = exc.get_detail()
            self.logger.warning(f'exception caught while executing df: {diagnostics} (ignoring)')
        else:
            if disk_space:
                available_space = convert_mb_to_b(disk_space)
                self.logger.info(f"locally available space: {available_space} B")

                # are we within the limit?
                if totalsize > available_space:
                    error = f"not enough local space for staging input files and run the job (need {totalsize} B, but only have {available_space} B)"
                    raise NoLocalSpace(error)
            else:
                self.logger.warning('get_local_disk_space() returned None')


class StageOutClient(StagingClient):
    """Stage-out client for uploading output files from the worker node.

    Extends :class:`StagingClient` with destination resolution logic that maps
    each output file to the correct RSE (Rucio Storage Element) for the
    requested activity.  Before transfer, the client verifies that each output
    file exists on disk, is non-zero in size, and computes its checksum.
    """

    mode = "stage-out"

    def prepare_destinations(self, files: list, activities: Union[list, str], alt_exclude: list = None) -> list:
        """Resolve the output RSE (``fspec.ddmendpoint``) for each file.

        Determines the primary and alternative DDM endpoints by matching the
        ``activities`` list against ``queuedata.astorages``.  Files that
        already have an endpoint set are re-mapped to the default endpoint if
        their current endpoint is not in the allowed storage list; the original
        value is then used as the alternative destination.

        Args:
            files: List of :class:`~pilot.info.filespec.FileSpec` objects whose
                ``ddmendpoint`` and ``ddmendpoint_alt`` will be set.
            activities: Activity name or ordered list of activity names used to
                look up associated output storages in ``astorages``.
            alt_exclude: Global list of endpoint names that must not be used as
                alternative destinations (e.g. endpoints that have already
                failed).

        Raises:
            PilotException: If ``activities`` is empty, or if no associated
                storages are defined and the ``mv`` copytool is not configured.

        Returns:
            list: The same ``files`` list with ``ddmendpoint`` and
                ``ddmendpoint_alt`` populated.
        """

        alt_exclude = list(alt_exclude or [])

        if not self.infosys.queuedata:  # infosys is not initialized: not able to fix destination if need, nothing to do
            return files

        if isinstance(activities, str):
            activities = [activities]

        if not activities:
            raise PilotException("Failed to resolve destination: passed empty activity list. Internal error.",
                                 code=ErrorCodes.INTERNALPILOTPROBLEM, state='INTERNAL_ERROR')

        astorages = self.infosys.queuedata.astorages or {}
        storages = None
        activity = activities[0]
        for a in activities:
            storages = astorages.get(a, {})
            if storages:
                break

        if not storages:
            if 'mv' in self.infosys.queuedata.copytools:
                return files

            act = ','.join(activities)
            raise PilotException(f"Failed to resolve destination: no associated storages defined for activity={activity} ({act})",
                                 code=ErrorCodes.NOSTORAGE, state='NO_ASTORAGES_DEFINED')

        def resolve_alt_destination(primary, exclude=None):
            """Return the next storage entry after ``primary`` that is not in ``exclude``."""

            cur = storages.index(primary) if primary in storages else 0
            exclude = set([primary] + list(exclude or []))
            alt = None
            for attempt in range(len(exclude) or 1):  # apply several tries to jump exclude entries (in case of dublicated data will stack)
                inext = (cur + 1) % len(storages)  # cycle storages, start from the beginning when reach end
                if storages[inext] not in exclude:
                    alt = storages[inext]
                    break
                cur += 1
            return alt

        # default destination
        ddm = storages[0]  # take the fist choice for now, extend the logic later if need
        ddm_alt = resolve_alt_destination(ddm, exclude=alt_exclude)

        self.logger.info(f"[prepare_destinations][{activity}]: allowed (local) destinations: {storages}, alt_exclude={alt_exclude}")
        self.logger.info(f"[prepare_destinations][{activity}]: resolved default destination: ddm={ddm}, ddm_alt={ddm_alt}")

        for e in files:
            if not e.ddmendpoint:  # no preferences => use default destination
                self.logger.info("[prepare_destinations][%s]: fspec.ddmendpoint is not set for lfn=%s"
                                 " .. will use default ddm=%s as (local) destination; ddm_alt=%s", activity, e.lfn, ddm, ddm_alt)
                e.ddmendpoint = ddm
                e.ddmendpoint_alt = ddm_alt
            #elif e.ddmendpoint not in storages and is_unified:  ## customize nucleus logic if need
            #   pass
            elif e.ddmendpoint not in storages:  # fspec.ddmendpoint is not in associated storages => use it as (non local) alternative destination
                self.logger.info("[prepare_destinations][%s]: Requested fspec.ddmendpoint=%s is not in the list of allowed (local) destinations"
                                 " .. will consider default ddm=%s as primary and set %s as alt. location", activity, e.ddmendpoint, ddm, e.ddmendpoint)
                e.ddmendpoint_alt = e.ddmendpoint if e.ddmendpoint not in alt_exclude else None
                e.ddmendpoint = ddm  # use default destination, check/verify nucleus case
            else:  # set corresponding ddmendpoint_alt if exist (next entry in cycled storages list)
                e.ddmendpoint_alt = resolve_alt_destination(e.ddmendpoint, exclude=alt_exclude)

            self.logger.info("[prepare_destinations][%s]: use ddmendpoint_alt=%s for fspec.ddmendpoint=%s",
                             activity, e.ddmendpoint_alt, e.ddmendpoint)

        return files

    @classmethod
    def get_path(cls, scope: str, lfn: str) -> str:
        """Construct a deterministic Rucio path fragment from scope and LFN.

        The path follows the standard Rucio deterministic layout::

            <scope_parts>/md5(<scope>:<lfn>)[0:2]/md5(<scope>:<lfn>)[2:4]/<lfn>

        The ``rucio`` prefix is omitted; it should be prepended by the caller
        via the protocol endpoint and path.

        Args:
            scope: Rucio scope of the replica (e.g. ``"data18_13TeV"``).
            lfn: Logical file name of the replica.

        Returns:
            str: The deterministic path fragment (without leading slash).
        """
        s = f'{scope}:{lfn}'
        hash_hex = hashlib.md5(s.encode('utf-8')).hexdigest()

        # exclude prefix from the path: this should be properly considered in protocol/AGIS for today
        paths = scope.split('.') + [hash_hex[0:2], hash_hex[2:4], lfn]
        paths = [_f for _f in paths if _f]  # remove empty parts to avoid double /-chars

        return '/'.join(paths)

    def resolve_surl(self, fspec: Any, protocol: dict, ddmconf: dict, **kwargs: Any) -> dict:
        """Construct the final destination SURL for a stage-out file.

        Uses the deterministic Rucio path layout (see :meth:`get_path`) unless
        a ``local_dir`` override is supplied.  Only deterministic DDM endpoints
        are supported; non-deterministic endpoints raise an exception.

        Can be replaced at the copytool level by providing a ``resolve_surl``
        callable on the copytool module.

        Args:
            fspec: :class:`~pilot.info.filespec.FileSpec` object for the output
                file.
            protocol: Protocol dict (``endpoint``, ``path``, ``flavour``) to
                use for building the SURL.
            ddmconf: Full DDM storage configuration dict.
            **kwargs: Recognized keys: ``local_dir`` — when non-empty the SURL
                is built from this directory instead of the DDM configuration.

        Raises:
            PilotException: If the DDM endpoint cannot be resolved or is
                non-deterministic.

        Returns:
            dict: Dict with key ``"surl"`` containing the constructed storage URL.
        """
        local_dir = kwargs.get('local_dir', '')
        if not local_dir:
            # consider only deterministic sites (output destination) - unless local input/output
            ddm = ddmconf.get(fspec.ddmendpoint)
            if not ddm:
                raise PilotException(f'Failed to resolve ddmendpoint by name={fspec.ddmendpoint}')

            # path = protocol.get('path', '').rstrip('/')
            # if not (ddm.is_deterministic or (path and path.endswith('/rucio'))):
            if not ddm.is_deterministic:
                raise PilotException(f'resolve_surl(): Failed to construct SURL for non deterministic '
                                     f'ddm={fspec.ddmendpoint}: NOT IMPLEMENTED', code=ErrorCodes.NONDETERMINISTICDDM)

        surl = protocol.get('endpoint', '') + os.path.join(protocol.get('path', ''), self.get_path(fspec.scope, fspec.lfn))

        return {'surl': surl}

    def transfer_files(self, copytool: Any, files: list, activity: list, **kwargs: Any) -> list:
        """Stage out files using the given copytool module.

        Orchestrates the full stage-out pipeline for a single copytool:

        1. Verify each output file exists on disk and is readable.
        2. Populate ``fspec.filesize`` from the filesystem when not already set.
        3. Reject zero-size output files.
        4. Compute the file checksum if not already present.
        5. Resolve output protocols and TURLs (``require_protocols``).
        6. Validate input via ``copytool.is_valid_for_copy_out()``.
        7. Delegate to ``copytool.copy_out()``.

        Args:
            copytool: Imported copytool module.
            files: List of :class:`~pilot.info.filespec.FileSpec` objects to
                stage out.
            activity: Ordered list of activity names used to resolve SE
                protocols.
            **kwargs: Extra keyword arguments forwarded to the copytool.
                Recognized keys include ``workdir``, ``output_dir``, and
                ``catchall``.

        Raises:
            PilotException: If an output file is missing, zero-size, has no
                RSE defined, or if the copytool reports invalid input.

        Returns:
            list: Return value of ``copytool.copy_out()``.
        """
        # check if files exist before actual processing
        # populate filesize if needed, calculate checksum
        for fspec in files:
            if not fspec.ddmendpoint:  # ensure that output destination is properly set
                if 'mv' not in self.infosys.queuedata.copytools:
                    msg = f'no output RSE defined for file={fspec.lfn}'
                    self.logger.error(msg)
                    raise PilotException(msg, code=ErrorCodes.NOSTORAGE, state='NO_OUTPUTSTORAGE_DEFINED')

            pfn = fspec.surl or getattr(fspec, 'pfn', None) or os.path.join(kwargs.get('workdir', ''), fspec.lfn) or \
                os.path.join(os.path.join(kwargs.get('workdir', ''), '..'), fspec.lfn)
            if not os.path.exists(pfn) or not os.access(pfn, os.R_OK):
                msg = f"output pfn file/directory does not exist: {pfn}"
                self.logger.error(msg)
                self.trace_report.update(clientState='MISSINGOUTPUTFILE', stateReason=msg, filename=fspec.lfn)
                self.trace_report.send()
                raise PilotException(msg, code=ErrorCodes.MISSINGOUTPUTFILE, state="FILE_INFO_FAIL")
            if not fspec.filesize:
                fspec.filesize = os.path.getsize(pfn)

            if not fspec.filesize:
                msg = f'output file has size zero: {fspec.lfn}'
                self.logger.fatal(msg)
                raise PilotException(msg, code=ErrorCodes.ZEROFILESIZE, state="ZERO_FILE_SIZE")

            fspec.surl = pfn
            fspec.activity = activity
            if os.path.isfile(pfn) and not fspec.checksum.get(config.File.checksum_type):
                fspec.checksum[config.File.checksum_type] = calculate_checksum(pfn,
                                                                               algorithm=config.File.checksum_type)

        # prepare files (resolve protocol/transfer url)
        if getattr(copytool, 'require_protocols', True) and files:
            output_dir = kwargs.get('output_dir', '')
            self.require_protocols(files, copytool, activity, local_dir=output_dir)

        if not copytool.is_valid_for_copy_out(files):
            self.logger.warning(f'input is not valid for transfers using copytool={copytool}')
            self.logger.debug(f'input: {files}')
            raise PilotException('invalid input for transfer operation')

        self.logger.info(f'ready to transfer (stage-out) files: {files}')

        if self.infosys:
            kwargs['copytools'] = self.infosys.queuedata.copytools

            # some copytools will need to know endpoint specifics (e.g. the space token) stored in ddmconf, add it
            kwargs['ddmconf'] = self.infosys.resolve_storage_data()

        if not files:
            msg = 'nothing to stage-out - an internal Pilot error has occurred'
            self.logger.fatal(msg)
            raise PilotException(msg, code=ErrorCodes.INTERNALPILOTPROBLEM)

        # add the trace report
        kwargs['trace_report'] = self.trace_report

        # is there an override in catchall to allow mv to final destination (relevant for mv copytool only)
        kwargs['mvfinaldest'] = self.allow_mvfinaldest(kwargs.get('catchall', ''))

        return copytool.copy_out(files, **kwargs)

#class StageInClientAsync(object):
#
#    def __init__(self, site):
#        raise NotImplementedError
#
#    def queue(self, files):
#        raise NotImplementedError
#
#    def is_transferring(self):
#        raise NotImplementedError
#
#    def start(self):
#        raise NotImplementedError
#
#    def finish(self):
#        raise NotImplementedError
#
#    def status(self):
#        raise NotImplementedError
#
#
#class StageOutClientAsync(object):
#
#    def __init__(self, site):
#        raise NotImplementedError
#
#    def queue(self, files):
#        raise NotImplementedError
#
#    def is_transferring(self):
#        raise NotImplementedError
#
#    def start(self):
#        raise NotImplementedError
#
#    def finish(self):
#        raise NotImplementedError
#
#    def status(self):
#        raise NotImplementedError
