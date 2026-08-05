# Benchmark harness and load targets

Type: grilling
Status: open
Blocked by: 06

## Question

Design the benchmark that proves the target: N concurrent pushers with
flat/bounded memory and no timeouts. Decide: the workload set (drawn from
real closure sizes on this infrastructure), N and the pass/fail criteria
(memory ceiling, p99 latencies, zero failed pushes), tooling (custom driver
using the garret client vs oha/vegeta-style generators), the environment
(against Garage or a local S3 stand-in), what the harness measures on the
pull side too, and how benchmarks stay runnable for regression tracking
during implementation.
