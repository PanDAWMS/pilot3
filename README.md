![Vericode Pylint Check](https://github.com/PalNilsson/pilot3/actions/workflows/vericode-check.yml/badge.svg)

# PanDA Pilot 3

**Version:** 3.12.1.79
**License:** Apache License 2.0
**Python:** 3.9, 3.11, 3.12

PanDA Pilot 3 is a dependency-free Python application that manages distributed job execution on the ATLAS/PanDA computing grid. It runs on worker nodes, fetches jobs from the PanDA server, stages input data in from distributed storage, executes experiment payloads, and stages output data back out. It is the successor to Pilot 2 (Pilot 1's successor) and is designed for reliability, extensibility, and zero mandatory runtime dependencies.

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Architecture](#architecture)
4. [Entry Point: `pilot.py`](#entry-point-pilotpy)
5. [Argument Parsing: `arguments.py`](#argument-parsing-argumentspy)
6. [Workflow Layer: `pilot/workflow/`](#workflow-layer-pilotworkflow)
7. [Control Threads: `pilot/control/`](#control-threads-pilotcontrol)
8. [Data API: `pilot/api/`](#data-api-pilotapi)
9. [Copytools: `pilot/copytool/`](#copytools-pilotcopytool)
10. [Info Service: `pilot/info/`](#info-service-pilotinfo)
11. [User Plugins: `pilot/user/`](#user-plugins-pilotuser)
12. [Event Service: `pilot/eventservice/`](#event-service-piloteventservice)
13. [Common Utilities: `pilot/common/`](#common-utilities-pilotcommon)
14. [Utilities: `pilot/util/`](#utilities-pilotutil)
15. [Configuration: `pilot/util/default.cfg`](#configuration-pilotutildefaultcfg)
16. [Error Codes: `pilot/common/errorcodes.py`](#error-codes-pilotcommonerrorcodespy)
17. [Shared State: `pilot/common/pilotcache.py`](#shared-state-pilotcommonpilotcachepy)
18. [Testing: `pilot/test/`](#testing-pilottest)
19. [CI/CD](#cicd)
20. [Threading Model](#threading-model)
21. [Signal Handling and Graceful Shutdown](#signal-handling-and-graceful-shutdown)
22. [Harvester Integration](#harvester-integration)
23. [Key Design Decisions](#key-design-decisions)
24. [Contributing](#contributing)
25. [Building Documentation](#building-documentation)

---

## Overview

The PanDA (Production and Distributed Analysis) system coordinates large-scale distributed computing across hundreds of sites for high-energy physics experiments (primarily ATLAS at CERN, but also ePIC, DarkSide, Rubin, SKA, sPHENIX). The **Pilot** is the agent that runs on each individual worker node: it acts as the bridge between the central PanDA server and the local compute resource.

A typical pilot lifetime:
1. Starts on a worker node (launched by a batch system or Harvester).
2. Validates the environment (CVMFS, disk space, proxies).
3. Fetches a job description from the PanDA server.
4. Stages in input files via a distributed storage protocol (Rucio, xrdcp, etc.).
5. Executes the experiment payload (Athena, ROOT transform, etc.) as a subprocess.
6. Monitors the payload for memory, CPU, looping, and other failure modes.
7. Stages out output files and logs.
8. Reports final job status to the PanDA server.
9. Optionally loops to fetch another job (multi-job mode).

---

## Quick Start

```bash
# Minimum invocation (generic workflow, ATLAS queue):
./pilot.py --pilot-user ATLAS -q AGLT2_TEST-condor

# With debug logging:
./pilot.py --pilot-user ATLAS -q AGLT2_TEST-condor -d

# Disable server updates (useful for local testing):
./pilot.py --pilot-user ATLAS -q AGLT2_TEST-condor -z

# Run tests:
python3 -m unittest

# Run a single test:
python3 -m unittest -v pilot/test/test_copytools_mv.py

# Lint:
flake8 pilot.py pilot/
pylint pilot/
```

### Required argument

| Flag | Description |
|---|---|
| `-q` / `--queue` | **Mandatory.** PanDA queue name (e.g. `AGLT2_TEST-condor`) |
| `--pilot-user` | Experiment plugin to load (e.g. `ATLAS`, `generic`, `ePIC`) |

---

## Architecture

The codebase is organized as follows:

```
pilot3/
├── pilot.py              # Entry point
├── arguments.py          # CLI argument parser
├── PILOTVERSION          # Version string (e.g. 3.12.1.79)
├── pilot/
│   ├── workflow/         # Workflow orchestrators (generic, HPC, analysis, etc.)
│   ├── control/          # Four core control threads (job, data, payload, monitor)
│   ├── api/              # High-level data transfer API (StageInClient, StageOutClient)
│   ├── copytool/         # Transfer protocol plugins (rucio, xrdcp, gfal, mv, s3, ...)
│   ├── info/             # InfoService: site/queue/storage metadata aggregation
│   ├── user/             # Experiment-specific plugins (atlas, epic, generic, ...)
│   ├── eventservice/     # Event Service (fine-grained event processing) support
│   ├── common/           # Shared foundations: errors, exceptions, cache, plugin factory
│   ├── util/             # General-purpose utilities (HTTPS, config, timing, processes, ...)
│   └── test/             # Unit tests
└── doc/                  # Sphinx documentation source
```

### High-level data flow

```
pilot.py
  └─ arguments.py          (parse CLI)
  └─ infosys.init()        (fetch queue/site/storage metadata)
  └─ workflow.run(args)
       └─ generic.py       (set up queues + launch 5 ExcThreads)
            ├─ job.control         → fetches jobs → queues.jobs
            ├─ data.control        → stage-in via StageInClient → queues.data_out
            │                       stage-out via StageOutClient
            ├─ payload.control     → executes payload subprocess
            ├─ monitor.control     → monitors pilot + job health
            └─ monitor.cgroup_control → optional cgroup resource enforcement
```

---

## Entry Point: `pilot.py`

`pilot.py` is the top-level script. Its `__main__` block:

1. Calls `get_args()` from `arguments.py` to parse command-line options.
2. Sets up the pilot timing dictionary and records `PILOT_START_TIME`.
3. Creates the main work directory (via `create_main_work_dir()`).
4. Establishes logging (via `establish_logging()`).
5. Sets environment variables (`PILOT_USER`, `PILOT_VERSION`, `PILOT_HOME`, etc.).
6. Calls `main()`.

Inside `main()`:

- Registers threading events (`graceful_stop`, `abort_job`, `job_aborted`).
- Optionally verifies CVMFS availability.
- Calls `infosys.init(args.queue)` to download and validate queue metadata.
- Checks queue state is `ACTIVE` and not `offline`.
- Renews OIDC tokens if needed.
- Sends worker-node hardware map to the PanDA server.
- Dynamically imports the requested workflow module: `pilot.workflow.<args.workflow>`.
- Calls `workflow.run(args)` and returns the `traces` object.

After `main()` returns, `wrap_up()` cleans up the work directory, writes a Harvester kill-worker signal if needed, and converts the pilot error code to a shell exit code.

### Why the work directory is created before logging is set up

The pilot creates its work directory (`PanDA_Pilot3_<pid>_<timestamp>`) before establishing logging because the log file (`pilotlog.txt`) is placed inside that directory. Any failures during work-directory creation are printed to stderr.

---

## Argument Parsing: `arguments.py`

All CLI arguments are defined in `arguments.py` using Python's `argparse`. Arguments are grouped into thematic functions:

| Function | Arguments |
|---|---|
| `add_main_args` | `--pilot-user`, `-a` workdir, `--cleanup`, `-i` version tag, `-z` disable server updates |
| `add_logging_args` | `--no-pilot-log`, `-d` debug, `--redirect-stdout` |
| `add_job_args` | `-j` job label, `-v` getjob requests, `-x` getjob failures, `--job-type` |
| `add_workflow_args` | `-w` workflow (`generic`, `analysis`, `production`, `*_hpc`, `stager`, ...) |
| `add_lifetime_args` | `-l` lifetime (default 324000 s), `-L` leasetime |
| `add_queue_args` | `-q` queue (required), `--resource-type` (`MCORE`, `SCORE`, ...), `-b` nocvmfs |
| `add_harvester_args` | Harvester workdir, datadir, event status dump, worker attributes, submit mode |
| `add_hpc_args` | `--hpc-resource`, `--hpc-mode`, `--es-executor-type`, `--pod` |
| `add_proxy_args` | `-t` disable proxy verification, `--cacert`, `--capath`, `--no-token-renewal` |
| `add_server_args` | `--url`, `-p` port (25443), `--queuedata-url`, `--subscribe-to-msgsvc`, `--use-https` |
| `add_realtimelogging_args` | `--use-realtime-logging`, `--realtime-logging-server`, `--realtime-logname` |
| `add_rucio_args` | `--use-rucio-traces`, `--rucio-host` |

`resource_type` is validated against the pattern `SCORE`, `MCORE`, `SCORE_*`, `MCORE_*` so that invalid resource types are caught early.

---

## Workflow Layer: `pilot/workflow/`

Workflows are the top-level orchestrators. `pilot.py` imports one dynamically at runtime:

```python
workflow = __import__(f'pilot.workflow.{args.workflow}', ...)
exitcode = workflow.run(args)
```

| File | Purpose |
|---|---|
| `generic.py` | Default workflow for standard production and analysis jobs |
| `analysis.py` | Analysis-specific workflow |
| `production.py` | Production-specific workflow |
| `generic_hpc.py` | Generic HPC variant |
| `analysis_hpc.py` | Analysis HPC variant |
| `production_hpc.py` | Production HPC variant |
| `eventservice_hpc.py` | Event Service on HPC |
| `stager.py` | Stage-only workflow (no payload) |

### `generic.py` in detail

This is the reference workflow. `run(args)` does:

1. **Signal registration** — registers `SIGINT`, `SIGTERM`, `SIGQUIT`, `SIGSEGV`, `SIGXCPU`, `SIGUSR1`, `SIGBUS` to the `interrupt()` handler.
2. **Queue setup** — creates ~20 `queue.Queue` objects for every state transition a job can pass through:
   - `jobs`, `payloads`, `data_in`, `data_out`
   - `validated_jobs`, `validated_payloads`, `monitored_payloads`
   - `finished_*`, `failed_*`, `completed_jobs`, `completed_jobids`
   - `realtimelog_payloads`, `messages`
3. **Sanity check** — calls the user plugin's `sanity_check()` to validate the environment before doing any work.
4. **Thread launch** — creates five `ExcThread` instances targeting `job.control`, `payload.control`, `data.control`, `monitor.control`, and `monitor.cgroup_control`.
5. **Main loop** — spins polling for thread exceptions (via each thread's `bucket` queue) and waiting for all threads to finish.

The `Traces` namedtuple (`pilot` dict with `state`, `nr_jobs`, `error_code`, `command`) is returned at the end and propagated back to `pilot.py` for the final exit code calculation.

---

## Control Threads: `pilot/control/`

Each of the four main control threads has a `control(queues, traces, args)` entry point called by the workflow.

### `job.py` — Job Controller

Responsibilities:
- Fetches job descriptions from the PanDA server via HTTPS (`getJob` API endpoint).
- Validates the job (sanity checks, proxy verification, disk space).
- Creates per-job work directories.
- Puts validated `JobData` objects on `queues.jobs` and then `queues.data_in`.
- Sends heartbeat updates and state changes (`starting`, `running`, `failed`, `finished`) to the PanDA server (`updateJob` API endpoint).
- Manages multi-job looping (re-requests a new job after the previous one finishes).
- Handles Harvester PUSH mode (reads job from `pandaJobData.out` instead of fetching).

The `send_state()` function in `job.py` is called throughout the codebase to report job status updates.

### `data.py` — Data Controller

Responsibilities:
- Monitors `queues.data_in`: for each job, invokes `StageInClient.transfer()` to download input files.
- On success, moves jobs to `queues.payloads`.
- Monitors `queues.data_out`: after payload finishes, invokes `StageOutClient.transfer()` to upload output files and logs.
- Handles staged-in file validation (checksums, sizes).
- Supports middleware containerization (runs stage-in/out scripts inside a container image).

Three internal sub-threads:
- `copytool_in` — reads from `data_in`, calls stage-in, pushes to `validated_jobs`.
- `copytool_out` — reads from `data_out`, calls stage-out.
- `queue_monitoring` — monitors for stuck or failed data transfers.

### `payload.py` — Payload Controller

Responsibilities:
- Reads jobs from `queues.payloads`.
- Prepares the execution environment (calls user plugin `setup()`).
- Executes the payload command as a subprocess using `pilot/util/container.py`.
- Streams stdout/stderr to `payload.stdout` / `payload.stderr`.
- Starts optional utility processes (e.g. `prmon` memory monitor) alongside the payload.
- Waits for the subprocess to finish and reads its exit code.
- Parses `jobReport.json` produced by the payload.
- Puts the finished job on `queues.data_out`.

Payload types are handled via `pilot/control/payloads/`:
- `generic.py` — standard subprocess execution
- `eventservice.py` — event service mode (fine-grained, reads events from a message queue)
- `eventservicemerge.py` — merging of event service output

### `monitor.py` — Monitor Controller

Responsibilities:
- Monitors **pilot-level** health (not per-job health, which is done by a sub-thread inside `job.py`):
  - Thread health: checks all `ExcThread` instances are still alive.
  - CVMFS / disk space / proxy validity at configurable intervals.
  - Machine features and job features (HEPScore, etc.).
  - Pilot heartbeat file (`pilot_heartbeat.json`).
  - OIDC token freshness.
- `cgroup_control()` — a second monitor thread that tracks cgroup memory limits when running under HTCondor with cgroup support.

---

## Data API: `pilot/api/`

### `data.py` — `StagingClient`, `StageInClient`, `StageOutClient`

This is the high-level abstraction over all file-transfer backends.

**`StagingClient`** (base class):
- Dynamically loads copytool plugins based on the `acopytools` site configuration (e.g., `{'pr': ['rucio'], 'pw': ['gfal']}`).
- Iterates over configured copytools in priority order until a transfer succeeds.
- Manages transfer tracing (Rucio trace reports sent back to the monitoring system).

**`StageInClient`**:
- Queries Rucio (or the catalog specified per copytool) for all replicas of each input file.
- Sorts replicas by proximity: LAN replicas first, WAN replicas second.
- Implements **direct access** (remote I/O) mode: if the site configuration allows, files are not copied locally but accessed remotely by the payload via xroot/HTTPS. The `FileSpec` object is marked as `direct_access` in this case.
- Checks that total input file size is within limits.
- Verifies checksums after download.

**`StageOutClient`**:
- Resolves the correct output RSE (Rucio Storage Element) and destination path from `ddmconf`/`astorages`.
- Calculates checksums of output files before upload.
- Verifies output files exist and are non-zero size.
- Constructs the SURL for each output file.

### `es_data.py` — `StageInESClient`

A specialized stage-in client for Event Service jobs, which handle individual events rather than full files.

### `memorymonitor.py`, `analytics.py`, `services.py`

API wrappers for resource monitoring and analytics data collection.

---

## Copytools: `pilot/copytool/`

Each file in `pilot/copytool/` is a plugin implementing the actual file transfer for a specific protocol. All copytools must implement:
- `copy_in(files, **kwargs)` — download files to the worker node.
- `copy_out(files, **kwargs)` — upload files from the worker node.

| File | Protocol |
|---|---|
| `rucio.py` | Rucio (the ATLAS distributed data management system) — the primary tool |
| `xrdcp.py` | XRootD (`xrdcp` command-line tool) |
| `gfal.py` | GFAL2 (`gfal-copy`, supports SRM, WebDAV, gsiftp, etc.) |
| `gs.py` | Google Cloud Storage |
| `s3.py` | S3-compatible object stores |
| `lsm.py` | LSM (Local Storage Manager, used at some HPC sites) |
| `mv.py` | Local filesystem move (used in testing and Harvester PUSH mode) |
| `objectstore.py` | Generic object store abstraction |

The selection of which copytool to use for a given job is driven by the PanDA queue's `acopytools` configuration, which maps activities (`pr` = production read, `pw` = production write, `pl` = production log) to ordered lists of copytool names.

---

## Info Service: `pilot/info/`

The Info Service is responsible for obtaining and caching all site-specific metadata that the pilot needs to operate.

| File | Role |
|---|---|
| `infoservice.py` | `InfoService` class — the singleton `infosys` used throughout the codebase |
| `extinfo.py` | `ExtInfoProvider` — fetches data from external sources (CRIC/AGIS APIs) |
| `configinfo.py` | `PilotConfigProvider` — provides data from the local `default.cfg` |
| `dataloader.py` | `merge_dict_data()` utility for merging multiple info provider outputs |
| `queuedata.py` | `QueueData` — structured representation of PanDA queue configuration |
| `storagedata.py` | `StorageData` — structured representation of DDM endpoints and storage |
| `filespec.py` | `FileSpec` — represents a single file in a stage-in or stage-out operation |
| `jobdata.py` | `JobData` — structured representation of a PanDA job |
| `jobinfo.py` | `JobInfoProvider` — job-level info provider |
| `basedata.py` | Base class for all data models |

`infosys` is initialized once at startup with `infosys.init(queue_name)`. It fetches:
1. Queue configuration from `pandaserver.cern.ch` (or the CRIC/AGIS API).
2. Storage endpoint definitions from CRIC.
3. Any overrides from the local `default.cfg` or command-line arguments.

It exposes `infosys.queuedata` (a `QueueData` object) and `infosys.storages` (a dict of `StorageData`).

The `@require_init` decorator on `InfoService` methods ensures that code cannot accidentally call info-service methods before `infosys.init()` has been called.

---

## User Plugins: `pilot/user/`

The user plugin system allows experiments to customize pilot behavior without forking the codebase. Each experiment has a subdirectory:

| Directory | Experiment |
|---|---|
| `atlas/` | ATLAS (the most complete implementation, reference for all others) |
| `epic/` | ePIC experiment at EIC |
| `darkside/` | DarkSide dark matter experiment |
| `rubin/` | Rubin Observatory / LSST |
| `ska/` | Square Kilometre Array |
| `sphenix/` | sPHENIX experiment at RHIC |
| `generic/` | Fallback/baseline plugin |

Each plugin directory typically contains:
- `common.py` — `sanity_check()`, `allow_send_workernode_map()`, and other shared hooks.
- `setup.py` — Environment setup, software release configuration, container setup.
- `container.py` — Container-specific customizations.
- `copytool_definitions.py` — Experiment-specific default copytools.
- `monitoring.py` — Extra monitoring (e.g. memory monitor integration for ATLAS uses `prmon`).
- `metadata.py` — Job report and metadata handling.
- `diagnose.py` — Error diagnosis (mapping payload exit codes to pilot error codes).
- `proxy.py` — Proxy/credential handling.
- `jobdata.py` — Experiment-specific job data fields.
- `memory.py`, `cpu.py` — Resource usage parsing.

The pilot selects the plugin at runtime based on the `PILOT_USER` environment variable (set from `--pilot-user`):

```python
user = __import__(f'pilot.user.{pilot_user}.common', ...)
user.sanity_check()
```

The **ATLAS plugin** is the most feature-complete and serves as the reference implementation. It handles ATLAS-specific concerns such as:
- ALRB (ATLAS Local Root Base) environment setup.
- Athena/transform execution.
- `prmon` memory monitoring integration.
- NordugridARC site-specific handling.
- Rucio trace reporting.
- X509 proxy and VOMS proxy management.

---

## Event Service: `pilot/eventservice/`

The Event Service (ES) is a fine-grained execution model where jobs process individual physics events rather than fixed input files. This allows unused compute to be reclaimed if a job is preempted mid-execution.

| File | Role |
|---|---|
| `yoda.py` | Yoda — the ES coordinator that orchestrates event processing |
| `droid.py` | Droid — the per-worker execution engine |
| `esprocess/` | Subprocess management for ES payload execution |
| `workexecutor/` | Execution backends (generic, Raythena) |
| `communicationmanager/` | Communication with the ES message broker (iDDS/ActiveMQ) |

In ES mode, the payload is given individual events (or ranges of events) via a message queue rather than a full input file. The pilot reports event-level status back to PanDA, allowing fine-grained accounting and recovery.

---

## Common Utilities: `pilot/common/`

| File | Role |
|---|---|
| `errorcodes.py` | `ErrorCodes` class — all pilot error codes as integer constants |
| `exception.py` | `PilotException` hierarchy + `ExcThread` |
| `pilotcache.py` | `get_pilot_cache()` — global in-memory singleton |
| `pluginfactory.py` | Factory pattern for loading plugins dynamically |

### `errorcodes.py`

Defines ~100 error codes as class constants on `ErrorCodes`. The numeric values match the legacy Pilot 1 codes because the PanDA server and ATLAS dashboards parse these codes to classify job failures. Adding a new code requires a corresponding entry in `get_error_message()`.

Every module that needs to report an error instantiates `ErrorCodes()` at module level:
```python
errors = ErrorCodes()
errors.STAGEINFAILED  # → 1099
```

### `exception.py`

`PilotException` is the base class. It carries:
- `_error_code` — an integer from `ErrorCodes`.
- `_message` — a human-readable string looked up from `ErrorCodes`.
- `_stack_trace` — the traceback at the point of raise.

Subclasses like `StageInFailure`, `SetupFailure`, `RunPayloadFailure`, etc. each hardcode their error code.

`ExcThread` is a `threading.Thread` subclass with a `bucket` (`queue.Queue`). Any unhandled exception in the thread body is caught in `run()` and placed in the bucket. The workflow's main loop drains buckets to detect thread failures.

### `pilotcache.py`

A process-wide singleton implemented with `@lru_cache(maxsize=1)`. Returns a single `PilotCache` object shared by all modules. Used to share state that would otherwise require circular imports (e.g., `queuedata`, `pilot_version`, cgroup state). Accessing it from anywhere:

```python
from pilot.common.pilotcache import get_pilot_cache
cache = get_pilot_cache()
cache.queuedata  # the QueueData object
```

---

## Utilities: `pilot/util/`

The `util/` directory contains ~35 purpose-specific utility modules.

| Module | Purpose |
|---|---|
| `https.py` | All HTTPS communication with the PanDA server: SSL setup, `send_update()`, `request()`, OIDC token refresh, curl fallback |
| `config.py` | INI configuration reader; exposes global `config` object |
| `constants.py` | Integer constants, timing key names, version components |
| `auxiliary.py` | General helpers: pilot state transitions, `set_pilot_state()`, banner printing |
| `processes.py` | Process management: killing subprocesses, CPU time, thread abort detection |
| `container.py` | `execute()` — runs shell commands / containers, handles stdout/stderr |
| `filehandling.py` | File I/O helpers: safe read/write, JSON parsing, checksum calculation |
| `timing.py` | Records timestamps to `pilot_timing.json` for performance analysis |
| `heartbeat.py` | Updates the `pilot_heartbeat.json` file |
| `monitoring.py` | Job-level monitoring checks called from the monitor thread |
| `queuehandling.py` | Queue utilities: `put_in_queue()`, `abort_jobs_in_queues()` |
| `loggingsupport.py` | `establish_logging()` — configures Python logging to file + stream |
| `cvmfs.py` | CVMFS availability checks |
| `disk.py` | Local disk space checks |
| `proxy.py` | X.509 proxy and VOMS proxy validation |
| `harvester.py` | Harvester integration: job request files, kill-worker signal |
| `workernode.py` | CPU topology, GPU detection, worker node map construction |
| `networking.py` | IPv4/IPv6 detection, DNS, network diagnostics |
| `batchsystem.py` | HTCondor, SLURM, PBS detection and version checks |
| `cgroups.py` | Linux cgroup creation and memory monitoring (under HTCondor) |
| `activemq.py` | ActiveMQ message broker interface (for ES and SPHENiX) |
| `realtimelogger.py` | Near real-time log streaming to Logstash |
| `transport.py` | Low-level transport layer helpers |
| `tracereport.py` | Rucio trace report construction and submission |
| `ruciopath.py` | Rucio LFN/SURL path utilities |
| `math.py` | Mathematical helpers (median, statistics) |
| `features.py` | `MachineFeatures` / `JobFeatures` (HEPiX standard) |
| `psutils.py` | Process information via `/proc` (memory, CPU per-process) |
| `parameters.py` | Parameter parsing utilities |
| `middleware.py` | Middleware availability detection |
| `mpi.py` | MPI process handling for HPC jobs |
| `condor.py` | HTCondor ClassAd utilities |

### `https.py` in depth

This is one of the most critical modules. It implements a multi-layer HTTP client:

1. **Primary backend:** `requests` library with SSL context (if available).
2. **Secondary backend:** `urllib` with SSL context.
3. **Fallback backend:** `curl` subprocess (for environments where Python SSL is broken).

All server calls go through `send_update(endpoint, data, url, port)`. The pilot defaults to HTTPS on port 25443 to `pandaserver.cern.ch`. The `--url` and `-p`/`--port` arguments override these.

OIDC token management:
- `update_local_oidc_token_info()` downloads a fresh token from the server when the cached one is about to expire.
- `get_local_oidc_token_info()` reads the locally cached token.
- Token renewal can be disabled via `--no-token-renewal` or via `no_token_renewal` in the queue's `catchall` field.

---

## Configuration: `pilot/util/default.cfg`

The INI-style config file (`pilot/util/default.cfg`) is read once at startup by `pilot/util/config.py` and exposed globally as `config`. It has sections:

| Section | Key parameters |
|---|---|
| `[Experiment]` | experiment name |
| `[Pilot]` | log file names, PanDA server URL, heartbeat intervals, free-space limits, looping-job timeouts, monitoring check intervals, utility commands |
| `[Information]` | URLs and cache paths for queuedata, PanDA queue list, DDM endpoints (CRIC) |
| `[Payload]` | job report file name, stdout/stderr file names, payload checks, memory limits per resource type |
| `[Container]` | container setup type (ALRB/Singularity), script names, middleware container image |
| `[Harvester]` | job request file, kill-worker file, worker attributes file |
| `[HPC]` | scratch disk path |
| `[File]` | checksum type (adler32) |
| `[Rucio]` | Rucio trace URL, Rucio host |
| `[Message_broker]` | ActiveMQ URL and port |
| `[Token]` | OIDC token refresh interval |
| `[Workernode]` | worker node map file names |

The config can be overridden by setting the `HARVESTER_PILOT_CONFIG` environment variable to a path pointing to an alternative config file.

Accessing the config:
```python
from pilot.util.config import config
heartbeat_interval = config.Pilot.heartbeat  # → 1800
```

---

## Error Codes: `pilot/common/errorcodes.py`

Error codes are integers defined as class-level constants on `ErrorCodes`. They are intentionally kept identical to the legacy Pilot 1 numbering because:
- The **PanDA server** parses these codes to classify failures.
- The **ATLAS dashboard (PanDA monitor)** displays error descriptions based on these codes.
- Changing a code would break historical comparisons and monitoring.

Selected important codes:

| Code | Constant | Meaning |
|---|---|---|
| 1098 | `NOLOCALSPACE` | Not enough disk space |
| 1099 | `STAGEINFAILED` | Stage-in failure |
| 1110 | `SETUPFAILURE` | Environment setup failed |
| 1137 | `STAGEOUTFAILED` | Stage-out failure |
| 1144 | `PANDAKILL` | Job killed by PanDA server |
| 1150 | `LOOPINGJOB` | Payload is looping (no I/O activity) |
| 1200 | `KILLSIGNAL` | Generic kill signal received |
| 1201–1208 | `SIGTERM`–`SIGINT` | Specific signal codes |
| 1212 | `PAYLOADOUTOFMEMORY` | Payload exceeded memory limit |
| 1213 | `REACHEDMAXTIME` | Job exceeded wall-clock limit |
| 1305 | `PAYLOADEXECUTIONFAILURE` | Payload returned non-zero exit code |

---

## Shared State: `pilot/common/pilotcache.py`

The `PilotCache` singleton avoids circular imports when many modules need access to the same runtime state. It is initialized once in `pilot.py` and is accessible from any module without importing `pilot.py`:

```python
from pilot.common.pilotcache import get_pilot_cache
cache = get_pilot_cache()
```

Key fields:

| Field | Set by | Used by |
|---|---|---|
| `queuedata` | `pilot.py` after `infosys.init()` | `job.py`, `payload.py`, monitor |
| `pilot_version` | `pilot.py` `set_environment_variables()` | various |
| `pilot_home_dir` | `pilot.py` | file handling |
| `use_cgroups` | `pilot.py` (HTCondor check) | `monitor.py` |
| `cgroups` | `monitor.cgroup_control` | process accounting |
| `resource_types` | `pilot.py` via `get_memory_limits()` | payload resource checks |
| `harvester_submitmode` | `pilot.py` | `job.py` |

---

## Testing: `pilot/test/`

Tests use Python's built-in `unittest` framework and are auto-discoverable:

```bash
python3 -m unittest           # run all tests
python3 -m unittest -v pilot/test/test_utils.py
```

| Test file | What it covers |
|---|---|
| `test_utils.py` | General utility functions |
| `test_copytools_mv.py` | `mv` copytool (local file moves) |
| `test_copytools_rucio.py` | Rucio copytool integration |
| `test_exception.py` | `PilotException` and error code mapping |
| `test_analytics.py` | Analytics API |
| `test_harvester.py` | Harvester integration helpers |
| `test_jobreport_parser.py` | Job report JSON parsing |
| `test_timeout.py` | Timeout/alarm utilities |
| `test_esprocess.py` | Event Service process management |
| `test_esstager.py` | Event Service staging |
| `test_esworkexecutor.py` | Event Service work executor |
| `test_escommunicator.py` | Event Service communication |

The no-dependency design means tests can be run without a PanDA server, CVMFS, or any grid middleware.

---

## CI/CD

GitHub Actions workflows (`.github/workflows/`):

| Workflow | Trigger | What it does |
|---|---|---|
| `unit-tests.yml` | Push / PR | Runs `python3 -m unittest` on Python 3.9, 3.11, 3.12 |
| `flake8-workflow.yml` | Push / PR | `flake8 pilot.py pilot/` (max line length 160, complexity 15) |
| `vericode-pylint-check.yml` | Push / PR | Pylint static analysis |
| `circular-imports.yml` | Push / PR | Detects circular import chains |
| `build-docs.yml` | Push / PR | Builds Sphinx documentation |

---

## Threading Model

The pilot is fundamentally multi-threaded. All threads are `ExcThread` instances (subclass of `threading.Thread`) that communicate exclusively via `queue.Queue` objects — no shared mutable state except through the `PilotCache` singleton and the `args` namespace.

```
MainThread
└── workflow.run()
     ├── ExcThread("job")       job.control(queues, traces, args)
     ├── ExcThread("data")      data.control(queues, traces, args)
     ├── ExcThread("payload")   payload.control(queues, traces, args)
     ├── ExcThread("monitor")   monitor.control(queues, traces, args)
     └── ExcThread("cgroup_monitor")  monitor.cgroup_control(...)
```

Each control thread may spawn its own sub-threads internally (e.g., `job.control` spawns `retrieve` and `job_monitor` sub-threads; `data.control` spawns `copytool_in`, `copytool_out`, `queue_monitoring`).

**Exception propagation:** If a sub-thread raises an unhandled exception, `ExcThread.run()` catches it and puts it in the thread's `bucket` queue. The workflow's main loop polls each thread's bucket. If an exception is found, it triggers `args.graceful_stop` to initiate an orderly shutdown.

**Graceful stop:** `args.graceful_stop` is a `threading.Event`. Every loop in every thread checks `args.graceful_stop.is_set()` (typically via `should_abort(args)` in `pilot/util/common.py`). Setting it causes all threads to exit their main loops and return.

---

## Signal Handling and Graceful Shutdown

When the pilot receives a kill signal (SIGTERM, SIGINT, etc.):

1. `interrupt()` in `generic.py` is called (registered via `signal.signal()`).
2. `args.abort_job` is set — instructs threads to abort the current job.
3. `args.graceful_stop` is set — instructs all threads to finish their current iteration and exit.
4. The handler waits up to 180 seconds for `args.job_aborted` to be set (by the workflow main loop).
5. If the maximum kill wait time (`MAX_KILL_WAIT_TIME + 60s`) is exceeded, the pilot force-kills itself via `kill_processes(os.getpid())`.

Signal-to-error-code mapping: `SIGTERM` → 1201, `SIGQUIT` → 1202, `SIGSEGV` → 1203, etc. These are reported to the PanDA server so that operators know why a job ended.

---

## Harvester Integration

[PanDA Harvester](https://github.com/PanDAWMS/panda-harvester) is a resource-facing agent that manages pilot submission across batch systems. The pilot supports two Harvester modes:

**PULL mode** (default):
- The pilot contacts the PanDA server directly to get a job.
- Writes `worker_requestjob.json` in the launch directory to signal it wants another job.

**PUSH mode** (`--harvester-submit-mode PUSH`):
- Harvester pre-stages the job description in `pandaJobData.out`.
- The pilot reads it from the file instead of contacting the server.
- Harvester also pre-stages input files; the pilot uses the `mv` copytool to move them into place.

After all jobs are done, the pilot writes a `kill_worker` file. Harvester watches for this file to know when to decommission the worker.

Key arguments for Harvester:
- `--harvester-workdir` — path where Harvester places job files.
- `--harvester-datadir` — path to pre-staged input data.
- `--harvester-eventstatusdump` — path to event status JSON (for ES jobs).
- `--harvester-workerattributes` — path to worker attributes JSON.

---

## Key Design Decisions

### Zero runtime dependencies
The core pilot (`pilot.py`, `arguments.py`, and all of `pilot/`) has no mandatory third-party dependencies. The only entry in `requirements.txt` is `pre-commit` (development tooling). This is essential because:
- Worker nodes in grid computing often have no internet access and no pip.
- The pilot is distributed as a tarball and must run with only what is available in the standard Python library.
- Optional libraries (`requests`, `certifi`, `rucio-clients`) are imported with `try/except ImportError` guards.

### Error code backward compatibility
Pilot 1 numeric error codes are preserved exactly. The PanDA server and monitoring infrastructure have years of institutional knowledge built around these numbers.

### Plugin architecture for experiments
Using `__import__` to load the user plugin at runtime means:
- Adding support for a new experiment requires only a new subdirectory under `pilot/user/`.
- No modification to core pilot code is needed.
- Experiments can override specific behaviors (environment setup, memory monitoring, proxy handling) without touching shared code.

### Queues instead of shared state
Using `queue.Queue` as the inter-thread communication mechanism (rather than shared data structures with locks) prevents race conditions and makes the data flow explicit. Each queue represents a specific stage in the job lifecycle.

### `ExcThread` for safe exception handling
Python threads silently swallow exceptions by default. `ExcThread` solves this by catching any exception in `run()` and placing it in a per-thread `bucket` queue. The workflow's main loop monitors these buckets, ensuring that exceptions in any thread are detected and trigger a graceful shutdown rather than leaving the pilot in an unknown state.

### INI config over environment variables
The `default.cfg` INI file provides structured, documented defaults. Environment variables and command-line arguments can override specific values, but the config file is the authoritative source of defaults. This avoids the anti-pattern of scattering magic values across the codebase.

### Cgroup support via HTCondor
When running under modern HTCondor with cgroup support (`is_htcondor_version_sufficient()`), the pilot creates a cgroup for the job. This enables accurate memory accounting for the payload and prevents memory overcommit from being attributed to the pilot itself.

---

## Contributing

1. Check `TODO.md` and `STYLEGUIDE.md` for open items and code standards.
2. Fork `PanDAWMS/pilot3` into your account, clone it, add `upstream` remote:
   ```bash
   git clone https://github.com/USERNAME/pilot3.git
   cd pilot3
   git remote add upstream https://github.com/PanDAWMS/pilot3.git
   ```
3. Always branch from `next`:
   ```bash
   git checkout next
   git fetch upstream && git merge upstream/next
   git checkout -b my-feature
   ```
4. All contributions go to **`next`** (or **`hotfix`** for urgent fixes). Pull requests directly to `master` are **rejected** — `master` triggers automatic pilot tarball creation and must only be updated via the release process.
5. Before submitting, verify:
   ```bash
   flake8 pilot.py pilot/   # PEP8, max line 160, complexity 15
   pylint pilot/
   python3 -m unittest
   ```

---

## Building Documentation

1. Install Sphinx: `pip install sphinx sphinx-rtd-theme`.
2. Build:
   ```bash
   cd doc && make html
   ```
3. Open `doc/_build/html/index.html` in a browser.

To document a new module, add to the corresponding `.rst` file:
```rst
.. automodule:: pilot.util.my_new_module
    :members:
```
