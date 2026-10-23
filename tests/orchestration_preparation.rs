use async_trait::async_trait;
use duroxide_sea::{
    DuroxideSeaProvider, OrchestrationPreparation,
    duroxide::{
        BackoffStrategy, Client, Event, EventKind, INITIAL_EXECUTION_ID, OrchestrationRegistry,
        OrchestrationStatus, RetryPolicy,
        providers::{ExecutionMetadata, OrchestrationItem, Provider, ProviderError},
        runtime::{Runtime, RuntimeOptions, registry::ActivityRegistry},
    },
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[allow(
    dead_code,
    reason = "Shared database fixtures are used by separate test binaries."
)]
mod support;

struct Preparation {
    db: DatabaseConnection,
    turns: Mutex<HashMap<String, Weak<Prepared>>>,
    calls: AtomicUsize,
    delay: Duration,
}

struct Prepared {
    value: i32,
}

impl Preparation {
    fn new(db: DatabaseConnection, delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            db,
            turns: Mutex::default(),
            calls: AtomicUsize::new(0),
            delay,
        })
    }

    fn orchestrations(self: &Arc<Self>) -> OrchestrationRegistry {
        OrchestrationRegistry::builder()
            .register("Prepared", {
                let preparation = self.clone();
                move |ctx, _: String| {
                    let prepared = preparation
                        .turns
                        .lock()
                        .unwrap()
                        .get(&ctx.instance_id())
                        .unwrap()
                        .upgrade()
                        .unwrap();
                    async move {
                        let output = ctx
                            .schedule_activity_with_retry(
                                "Echo",
                                prepared.value.to_string(),
                                RetryPolicy::new(2).with_backoff(BackoffStrategy::Fixed {
                                    delay: Duration::from_millis(1),
                                }),
                            )
                            .await?;
                        ctx.schedule_wait("resume").await;
                        Ok(output)
                    }
                }
            })
            .build()
    }
}

#[async_trait]
impl OrchestrationPreparation for Preparation {
    async fn prepare(
        &self,
        item: &OrchestrationItem,
    ) -> Result<Arc<dyn Send + Sync>, ProviderError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let row = self
            .db
            .query_one_raw(Statement::from_string(
                DatabaseBackend::Sqlite,
                "SELECT 42 AS answer",
            ))
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(self.delay).await;
        let prepared = Arc::new(Prepared {
            value: row.try_get("", "answer").unwrap(),
        });
        self.turns
            .lock()
            .unwrap()
            .insert(item.instance.clone(), Arc::downgrade(&prepared));
        Ok(prepared)
    }
}

#[tokio::test]
async fn preparation_and_activities_await_io_on_current_thread_and_recover_after_restart() {
    let db = support::sqlite_database().await;
    let preparation = Preparation::new(db.clone(), Duration::from_millis(5));
    let provider =
        Arc::new(DuroxideSeaProvider::new(db.clone()).with_preparation(preparation.clone()));
    let attempts = Arc::new(AtomicUsize::new(0));
    let activities = ActivityRegistry::builder()
        .register("Echo", {
            let attempts = attempts.clone();
            move |_, input: String| {
                let attempt = attempts.fetch_add(1, Ordering::Relaxed);
                async move {
                    assert_eq!(input, "42");
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    if attempt == 0 {
                        Err("transient".into())
                    } else {
                        Ok(input)
                    }
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
        preparation.orchestrations(),
        options.clone(),
    )
    .await;
    client
        .start_orchestration("prepared", "Prepared", "")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if provider
                .read("prepared")
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
    assert!(preparation.calls.load(Ordering::Relaxed) >= 2);
    assert!(
        preparation
            .turns
            .lock()
            .unwrap()
            .values()
            .all(|turn| turn.upgrade().is_none())
    );
    drop(client);
    drop(provider);
    drop(preparation);
    let preparation = Preparation::new(db.clone(), Duration::from_millis(5));
    let provider = Arc::new(DuroxideSeaProvider::new(db).with_preparation(preparation.clone()));
    let client = Client::new(provider.clone());
    let second = Runtime::start_with_options(
        provider.clone(),
        activities,
        preparation.orchestrations(),
        options,
    )
    .await;
    client
        .raise_event("prepared", "resume", "ready")
        .await
        .unwrap();
    let status = client
        .wait_for_orchestration("prepared", Duration::from_secs(10))
        .await
        .unwrap();
    second.shutdown(Some(100)).await;
    assert!(matches!(status, OrchestrationStatus::Completed { output, .. } if output == "42"));
    assert_eq!(
        attempts.load(Ordering::Relaxed),
        2,
        "recorded activities survive a cold provider restart"
    );
    assert_eq!(preparation.calls.load(Ordering::Relaxed), 1);
    let history = provider.read("prepared").await.unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|event| matches!(event.kind, EventKind::ActivityScheduled { .. }))
            .count(),
        2
    );
}

#[tokio::test]
async fn preparation_renews_lease_and_releases_data_at_turn_boundaries() {
    let db = support::sqlite_database().await;
    let preparation = Preparation::new(db.clone(), Duration::from_millis(240));
    let provider =
        Arc::new(DuroxideSeaProvider::new(db.clone()).with_preparation(preparation.clone()));
    let client = Client::new(provider.clone());
    client
        .start_orchestration("lease", "Prepared", "")
        .await
        .unwrap();
    let fetch = tokio::spawn({
        let provider = provider.clone();
        async move {
            provider
                .fetch_orchestration_item(Duration::from_millis(120), Duration::ZERO, None)
                .await
                .unwrap()
                .unwrap()
        }
    });
    tokio::time::sleep(Duration::from_millis(180)).await;
    let competing = DuroxideSeaProvider::new(db);
    assert!(
        competing
            .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );
    let (_, token, _) = fetch.await.unwrap();
    let abandoned = preparation.turns.lock().unwrap()["lease"].clone();
    assert_eq!(abandoned.upgrade().unwrap().value, 42);
    provider
        .abandon_orchestration_item(&token, None, false)
        .await
        .unwrap();
    assert!(abandoned.upgrade().is_none());

    let (_, token, _) = provider
        .fetch_orchestration_item(Duration::from_millis(120), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    let expired = preparation.turns.lock().unwrap()["lease"].clone();
    assert!(expired.upgrade().is_some());
    tokio::time::sleep(Duration::from_millis(140)).await;
    let (_, replacement, _) = provider
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(token, replacement);
    assert!(expired.upgrade().is_none());
    assert!(
        preparation.turns.lock().unwrap()["lease"]
            .upgrade()
            .is_some()
    );
    assert_eq!(preparation.calls.load(Ordering::Relaxed), 3);

    let started = Event::with_event_id(
        1,
        "lease",
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: "Prepared".into(),
            version: "1.0.0".into(),
            input: "".into(),
            parent_instance: None,
            parent_id: None,
            parent_execution_id: None,
            carry_forward_events: None,
            initial_custom_status: None,
        },
    );
    provider
        .ack_orchestration_item(
            &replacement,
            INITIAL_EXECUTION_ID,
            vec![started],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("Prepared".into()),
                orchestration_version: Some("1.0.0".into()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();
    assert_eq!(provider.read("lease").await.unwrap().len(), 1);
    assert!(
        preparation.turns.lock().unwrap()["lease"]
            .upgrade()
            .is_none()
    );
}
