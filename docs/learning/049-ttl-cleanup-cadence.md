# TTL cleanup cadence

## Why it matters

TTL cleanup cadence is one of the design choices that turns a toy key-value map into a network service with predictable behavior. Understanding it makes failures and performance characteristics easier to reason about.

## In this project

Trace this concern through `src/lib.rs` and, where connection setup is involved, `src/main.rs`. The implementation keeps the relevant state and control flow explicit so the ttl cleanup cadence behavior can be inspected without a framework.

## Try it

Create the smallest client interaction or unit test that exercises this behavior, record the response, and then alter one assumption. Compare the result before restoring the intended invariant.

