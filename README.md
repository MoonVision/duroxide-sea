# Duroxide SeaORM provider

`DuroxideSeaProvider` stores durable workflow history and queues in SQLite or PostgreSQL.
The crate re-exports its pinned runtime as `duroxide_sea::duroxide`, so applications use
the same context, registry, and provider types without a separate runtime dependency.

## Local async work

The runtime supports `context.run_local(future)` for repeatable async host work within a
replay turn, including on Tokio's current-thread runtime. Local outcomes are not persisted;
use activities for effects whose outcomes must survive replay. Local futures must not await
durable activities, timers, or events. Their values and completion ordering must preserve
workflow decisions and durable scheduling order.

```rust
use duroxide_sea::duroxide::{OrchestrationContext, OrchestrationRegistry};

async fn workflow(ctx: OrchestrationContext, input: String) -> Result<String, String> {
    let prepared = ctx.run_local(async { input }).await;
    ctx.schedule_activity("Process", prepared).await
}

let orchestrations = OrchestrationRegistry::builder()
    .register("Workflow", workflow)
    .build();
```

Run `cargo test --all-features` to exercise the SQLite provider validation suite and
current-thread async replay, retry, and restart integration tests.
