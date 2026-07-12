# Orange RECAP Raw ASHA Final e12 Results

Artifact: `orange_recap_raw100_asha_model_weight_resume_final_e12_20260712`

This directory snapshots the completed PIDSMaker ASHA run on Orange RECAP raw exports.

Important protocol note: this run used additive model-weight resume for ASHA HPO rungs, with a fresh optimizer after promotion. It is not a paper-perfect ASHA checkpoint resume because optimizer state, RNG state, and data-loader/generator state were not restored by default.

## Contents

- `metadata.json`: run metadata and best final result per method.
- `final_e12_results.tsv`: compact machine-readable final result table.
- `final_e12_results.json`: processed table plus raw bundled state/final/r3 data.
- `raw_final_json/<method>/*.json`: raw final e12 result JSONs copied from MeluXina.
- `states/*_state.json`: ASHA controller state snapshots.
- `status/summary.txt`: `./meluxina/job_status.sh summary` output at capture time.
- `status/hpo_detail.txt`: `./meluxina/job_status.sh hpo` output at capture time.
- `status/final_results_table.txt`: `./meluxina/job_status.sh final` output at capture time.

## Best Final e12 ADP

| Method | Final ADP | Config |
|---|---:|---|
| `velox` | `0.718` | `c167_lr0p001_wd0_dim64_emb128_batch2048_seed0` |
| `magic` | `0.813` | `c209_win4m_lr0p001_wd0p001_dim32_mask0p7_seed0` |
| `orthrus` | `0.778` | `c233_lr0p0001_wd1em05_capmedium_batch2048_neigh50_seed0` |
| `kairos` | `0.654` | `c001_lr2em05_wd0p001_capsmall_batch512_neigh10_seed0` |

## Reproduce The Status View

```bash
./meluxina/job_status.sh final
```
