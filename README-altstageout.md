# Stage-out to an Alternative SE (`allow_altstageout`)

`allow_altstageout` is a boolean flag that enables a **two-attempt stage-out fallback**: if a file fails to transfer to its primary destination RSE, the pilot automatically retries to a pre-resolved alternative RSE (`ddmendpoint_alt`).

---

## Where it comes from (priority order)

1. **PanDA Queue params** (`pilot/info/queuedata.py:180`) — `QueueData.allow_altstageout()` reads `queuedata.params['allow_altstageout']` and casts it to `bool`. This is set in CRIC/AGIS under the queue's `params` field.

2. **PanDA Job definition** (`pilot/info/jobdata.py:601`) — `JobData.allow_altstageout()` checks the queue-level value **first** (it takes precedence). If the queue has no preference, it falls back to `JobData.altstageout`, which comes from the `altStageOut` field of the job description sent by the PanDA server.

   The job-level value is cleaned by `clean__altstageout()`: the string `"on"` → `True`, `"off"` → `False`.

**The queue-level setting always wins over the job-level setting.**

---

## How `ddmendpoint_alt` is populated

Before stage-out begins, `StageOutClient.prepare_destinations()` (`pilot/api/data.py:1670`) resolves both endpoints for every output file from `queuedata.astorages`:

- `ddmendpoint` — the primary RSE (first in the allowed storages list for the activity).
- `ddmendpoint_alt` — the next RSE in the cycled storages list that is not excluded (e.g. not the nucleus endpoint, not the same as primary).

Three cases are handled per file:

| Situation | Primary (`ddmendpoint`) | Alternative (`ddmendpoint_alt`) |
|---|---|---|
| `fspec.ddmendpoint` not set | `storages[0]` (default) | next entry in list |
| `fspec.ddmendpoint` is not in allowed storages | `storages[0]` (default) | original requested endpoint (unless in `alt_exclude`) |
| `fspec.ddmendpoint` is in allowed storages | kept as-is | next entry after it in the cycled list |

The `alt_exclude` list prevents certain endpoints (e.g. the nucleus) from being used as alternatives. It is passed in from `_do_stageout` as `list(filter(None, [job.nucleus]))`.

---

## How the fallback is executed

Implemented in `_do_stageout()` (`pilot/control/data.py:987`):

```
1. client.transfer(xdata, raise_exception=not altstageout)
   └─ all files attempted to primary ddmendpoint

2. remain_files = [f for f in xdata if f.require_transfer()]
   has_altstorage = all(f.ddmendpoint_alt and f.ddmendpoint != f.ddmendpoint_alt
                        for f in remain_files)

3. if altstageout AND remain_files AND has_altstorage:
       for f in remain_files:
           f.ddmendpoint = f.ddmendpoint_alt   # swap to alt
           f.ddmendpoint_alt = None
           f.is_altstaged = True
       job.piloterrordiags.append(f'Alternative stage-out for {lfns}')
       client.transfer(xdata, ...)             # second attempt to alt SE
```

Key details:

- The first `transfer()` call is made with `raise_exception=False` when `altstageout` is `True`, so a partial failure does not immediately abort.
- The retry only fires if **every** remaining file has a valid and distinct `ddmendpoint_alt`. If even one file has no alternative, the fallback is skipped entirely.
- After the swap, `ddmendpoint_alt` is cleared and `is_altstaged = True` is set on the `FileSpec` to record that the file ended up on the alternative SE.
- A diagnostic message is appended to `job.piloterrordiags` listing the affected LFNs.

---

## Relevant files

| File | Role |
|---|---|
| `pilot/info/queuedata.py:180` | `QueueData.allow_altstageout()` — reads queue-level param |
| `pilot/info/jobdata.py:601` | `JobData.allow_altstageout()` — merges queue + job preference |
| `pilot/info/jobdata.py:674` | `clean__altstageout()` — normalises `"on"`/`"off"` strings |
| `pilot/api/data.py:1670` | `StageOutClient.prepare_destinations()` — populates `ddmendpoint_alt` |
| `pilot/control/data.py:987` | `_do_stageout()` — executes the two-attempt fallback logic |
