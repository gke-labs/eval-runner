<!-- eval-runner:comparison:eval-runner/docs-eval-batches-tokens -->

## RUNNING

Running batch 2 (1 tasks continuing)

| eval | base | this PR | Δ (95% CI) | label | samples | tokens | W/L/T |
| --- | --- | --- | --- | --- | --- | --- | --- |
| docs/readme\_comprehension | 0.60 | 0.90 | +0.30 [+0.10, +0.50] | improved | 16 | 800 (+R 100) | 8/2/6 |
| docs/readme\_comprehension\_blind | 0.20 | 0.15 | -0.05 [-0.15, +0.00] | inconclusive | 16 | — | 1/2/13 |
| docs/readme\_summary | — | — | — | running | — | — | — |

$0.01 spent (default prices), 32 of 100 possible samples · paired against base commit a81d0c4 · `/comparisons/eval-runner/docs-eval-batches-tokens`

- comment `/eval more 16` to run 16 more samples (docs/readme\_comprehension\_blind)
