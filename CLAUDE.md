# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PanDA Pilot 3 is a dependency-free Python application that manages distributed job execution on the ATLAS/PanDA computing grid. It runs on worker nodes, fetches jobs from the PanDA server, stages input data, executes payloads, and stages output data back.

## Commands

**Run all tests:**
```
python3 -m unittest
```

**Run a single test file:**
```
python3 -m unittest -v pilot/test/test_copytools_mv.py
```

**Lint:**
```
flake8 pilot.py pilot/
pylint <path to pilot module>
```

**Run the pilot:**
```
./pilot.py -q <PANDA_QUEUE>
```

**Build docs:**
```
cd doc && make html
```

## Code Style

- PEP8 + Flake8: max line length **160**, max McCabe complexity **15**
- Flake8 config is in `.flake8` (ignores E262, E265, E266, N804, W504, B902, N818)
- Pylint is also enforced via CI
- Supported Python: 3.9, 3.11, 3.12

## Architecture

### Entry Point and Control Flow

`pilot.py` is the entry point. It parses arguments (`arguments.py`), initializes logging and the info service, then hands off to a **workflow** in `pilot/workflow/`. The workflow (`generic.py`, `analysis.py`, `production.py`, and HPC/event-service variants) launches the four core control threads using queues for coordination:

- `pilot/control/job.py` — fetches jobs from PanDA server, puts them on queues
- `pilot/control/data.py` — handles stage-in/stage-out via copytools; monitors `data_in`/`data_out` queues
- `pilot/control/payload.py` — executes the job payload subprocess
- `pilot/control/monitor.py` — monitors running jobs and pilot heartbeat

### Key Subsystems

**Info Service** (`pilot/info/`): A singleton `infosys` aggregates queue data, site data, and storage data from external sources (PanDA/AGIS) and local config. `InfoService` is the high-level API; `ExtInfoProvider` fetches from external endpoints; `QueueData`, `StorageData`, `FileSpec`, `JobData` are the data model classes.

**Copytools** (`pilot/copytool/`): Each file is a plugin for a specific transfer protocol (rucio, xrdcp, mv, etc.). `pilot/api/data.py` provides `StageInClient`/`StageOutClient` that select and invoke the appropriate copytool.

**User plugins** (`pilot/user/`): Experiment-specific customizations (atlas, generic, epic, darkside, rubin, ska, sphenix). Each has a `setup.py`, `common.py`, and optional modules. The atlas plugin is the most complete reference implementation.

**Error handling**: `pilot/common/errorcodes.py` defines numeric error codes (matching legacy Pilot 1 codes expected by PanDA server). `PilotException` in `pilot/common/exception.py` wraps them. `ExcThread` is a thread subclass that captures exceptions from worker threads.

**Configuration**: `pilot/util/config.py` reads an INI-style config from `pilot/util/default.cfg` (or `HARVESTER_PILOT_CONFIG` env var). Accessed globally as `from pilot.util.config import config`.

**PanDA communication**: `pilot/util/https.py` handles all HTTP(S) interactions with the PanDA server, including job fetching, heartbeats, and status updates.

**Caching**: `pilot/common/pilotcache.py` provides a shared in-memory dict (accessed via `get_pilot_cache()`) used across modules to share state without circular imports.

### Threading Model

The workflow launches control threads (job, data, payload, monitor) as `ExcThread` instances communicating via `queue.Queue` objects. Queues like `job_q`, `data_in`, `data_out`, `completed_jobs` form the data pipeline. The monitor thread detects stuck/failed states and initiates cleanup.

## Branching

- Contributions go to the `next` branch (or `hotfix` for urgent fixes), **not directly to `master`**
- `master` triggers automatic pilot tarball creation — direct PRs to master are rejected
