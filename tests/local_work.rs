use duroxide_sea::duroxide::{
    BackoffStrategy, Client, EventKind, OrchestrationRegistry, OrchestrationStatus, RetryPolicy,
    providers::Provider as _,
    runtime::{Runtime, RuntimeOptions, registry::ActivityRegistry},
};
use sea_orm::{ConnectionTrait as _, DatabaseBackend, Statement};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

mod support;

#[tokio::test]
async fn async_host_io_repeats_while_activity_retries_and_results_survive_restart() {
    let provider = support::sqlite_provider().await;
    let db = support::sqlite_database().await;
    let local_calls = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));
    let activities = ActivityRegistry::builder()
        .register("Echo", {
            let attempts = attempts.clone();
            move |_, input: String| {
                let attempt = attempts.fetch_add(1, Ordering::Relaxed);
                async move {
                    if attempt == 0 {
                        Err("transient".into())
                    } else {
                        Ok(input)
                    }
                }
            }
        })
        .build();
    let orchestrations = OrchestrationRegistry::builder()
        .register("Local", {
            let local_calls = local_calls.clone();
            move |ctx, _: String| {
                let db = db.clone();
                let local_calls = local_calls.clone();
                async move {
                    let value = ctx
                        .run_local(async {
                            local_calls.fetch_add(1, Ordering::Relaxed);
                            let row = db
                                .query_one_raw(Statement::from_string(
                                    DatabaseBackend::Sqlite,
                                    "SELECT 42 AS answer",
                                ))
                                .await
                                .map_err(|error| error.to_string())?
                                .ok_or("missing answer")?;
                            tokio::time::sleep(Duration::from_millis(5)).await;
                            row.try_get::<i32>("", "answer")
                                .map_err(|error| error.to_string())
                        })
                        .await?;
                    let output = ctx
                        .schedule_activity_with_retry(
                            "Echo",
                            value.to_string(),
                            RetryPolicy::new(2).with_backoff(BackoffStrategy::Fixed {
                                delay: Duration::from_millis(1),
                            }),
                        )
                        .await?;
                    ctx.schedule_timer(Duration::from_millis(1)).await;
                    ctx.schedule_wait("resume").await;
                    ctx.run_local(async {
                        tokio::task::yield_now().await;
                        Ok(output)
                    })
                    .await
                }
            }
        })
        .build();
    let options = RuntimeOptions {
        dispatcher_long_poll_timeout: Duration::from_millis(10),
        ..Default::default()
    };
    let client = Client::new(provider.clone());
    let first = Runtime::start_with_options(
        provider.clone(),
        activities.clone(),
        orchestrations.clone(),
        options.clone(),
    )
    .await;
    client
        .start_orchestration("local", "Local", "")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if provider
                .read("local")
                .await
                .unwrap()
                .iter()
                .any(|event| matches!(event.kind, EventKind::ExternalSubscribed { .. }))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    first.shutdown(Some(100)).await;
    assert_eq!(attempts.load(Ordering::Relaxed), 2);
    let calls_before_restart = local_calls.load(Ordering::Relaxed);
    assert!(calls_before_restart >= 2);
    let second =
        Runtime::start_with_options(provider.clone(), activities, orchestrations, options).await;
    client
        .raise_event("local", "resume", "ready")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("local", Duration::from_secs(5))
        .await
        .unwrap();
    second.shutdown(Some(100)).await;
    assert!(matches!(status, OrchestrationStatus::Completed { output, .. } if output == "42"));
    assert_eq!(
        attempts.load(Ordering::Relaxed),
        2,
        "completed activities must not repeat after restart"
    );
    assert!(local_calls.load(Ordering::Relaxed) > calls_before_restart);
    let history = provider.read("local").await.unwrap();
    let activities: Vec<_> = history
        .iter()
        .filter_map(|event| match &event.kind {
            EventKind::ActivityScheduled { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(activities, ["Echo", "Echo"]);
}
