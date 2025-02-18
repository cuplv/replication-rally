# *Replication Rally!*

Replication Rally is a framework for performance evaluations of key-value stores. Currently, drivers are implemented for [`etcd`](https://github.com/etcd-io/etcd)
and [`ferry`](https://github.com/cuplv/super-v).

## Outcome files

The outcome of an experiment is stored in a CSV file with the
following fields:

* [0] Network ID (String)
* [1] Competitor ID (String)
* [2] Cluster size (Nat nodes)
* [3] Request total (Nat ops)
* [4] Request rate (Nat ops/s)
* [5] Average request latency (Float ms) *
* [6] Average response latency (Float ms) *
* [7] Throughput (Float ops/ms) *
* [8] Failed operations (Nat ops) *
* [9] Contributing reps (Nat reps)
* [10] Single-retry operations (Nat ops) *
* [11] Multi-retry operations (Nat ops) *
* [12] Client count (Nat clients)
* [13] Imposed message latency (Nat ms)

Starred fields are measured, while unstarred fields are configured.

## Measuring latency

Latencies are measured using the Instant struct in Rust.  First, when
an experiment round starts, the RoundStart Instant is captured.  Then,
when each request 'i' is initiated, a MeasuredRequest[i] Instant is
captured.  Finally, as each request receives its response, a
MeasuredResponse[i] Instant is captured.

When the experiment round is complete, the following values are
calculated for each request 'i'.

    MeasuredRequestOffset[i] = RoundStart - MeasuredRequest[i]

    ScheduledRequestOffset[i] = i * (1 Second / OpsPerSec)

    RequestLatency[i] = MeasuredRequestOffset - ScheduledRequestOffset

    ResponseLatency[i] = MeasuredResponse[i] - MeasuredRequest[i]
