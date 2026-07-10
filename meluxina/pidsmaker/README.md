# MeluXina PIDSMaker Runs

This directory is the MeluXina-local entry point for running native PIDSMaker methods on the Orange raw export.

Default single-run sweeps live in `sweeps/` and use PIDSMaker's native method configs: `velox`, `magic`, `orthrus`, and `kairos`.

Submit one method from the repo root:

```bash
python meluxina/submit.py meluxina/pidsmaker/configs/recap_raw_velox.yml --dry-run
python meluxina/submit.py meluxina/pidsmaker/configs/recap_raw_velox.yml
```

Useful overrides:

```bash
export MELUXINA_PIDSMAKER_IMAGE=/path/to/pidsmaker-pids.sif
export ORANGE_EXPORT_ROOT=/mnt/tier2/project/p201223/pidsmaker-across-capture-tools/capture_export/pidsmaker_export
```

Local smoke files should go under `meluxina/pidsmaker/local/` or use `smoke` in the filename. Those paths are ignored so they can be used for testing without being committed.
