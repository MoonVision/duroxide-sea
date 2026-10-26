# Duroxide SeaORM provider

`DuroxideSeaProvider` stores durable workflow history and queues in SQLite or PostgreSQL.
The crate re-exports official Duroxide 0.1.32 as `duroxide_sea::duroxide`, so applications use
the same context, registry, and provider types without a separate runtime dependency.

## Application preparation

`DuroxideSeaProvider::with_preparation` accepts an `OrchestrationPreparation` implementation.
Its async `prepare` method receives each locked orchestration item before synchronous replay.
Applications can load immutable inputs, compile programs, and hydrate recorded payloads here,
then expose prepared values to their orchestration through an application-owned in-memory registry.
Preparation must preserve replay decisions from the original inputs and recorded history.

The provider renews the orchestration lease while preparation awaits I/O. A preparation error
abandons the item for redelivery. The returned opaque `Arc<dyn Send + Sync>` keeps application data
alive through the turn and any acknowledgement retries. The provider releases it after
acknowledgement, abandonment, or lease expiry observed by a subsequent fetch.

Activities own external effects and persist their results before returning. Preparation and
async activities support Tokio's current-thread runtime. Orchestrations use standard Duroxide
activities, timers, and events for durable waits.

## Verification

Run `cargo test --all-features` to exercise the SQLite provider validation suite and
current-thread preparation, lease renewal, prepared data lifetime, activity retry, and restart tests.

With a disposable PostgreSQL database, run
`TEST_POSTGRES_URL=postgres://localhost/duroxide_test cargo test --test postgres_sessions -- --ignored`
to verify session renewal across multiple workers and expired or idle sessions.
