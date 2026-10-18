use std::time::Duration;

use duroxide::providers::{ExecutionMetadata, Provider, WorkItem};
use duroxide::{Event, EventKind, INITIAL_EXECUTION_ID};

mod support;

use support::sqlite_provider as provider;

fn start_item(instance: &str) -> WorkItem {
    WorkItem::StartOrchestration {
        instance: instance.to_string(),
        orchestration: "TestWorkflow".to_string(),
        input: "{}".to_string(),
        version: Some("1.0.0".to_string()),
        parent_instance: None,
        parent_id: None,
        parent_execution_id: None,
        execution_id: INITIAL_EXECUTION_ID,
    }
}

fn orchestration_started_event(instance: &str) -> Event {
    Event::with_event_id(
        1,
        instance,
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: "TestWorkflow".to_string(),
            version: "1.0.0".to_string(),
            input: "{}".to_string(),
            parent_instance: None,
            parent_id: None,
            parent_execution_id: None,
            carry_forward_events: None,
            initial_custom_status: None,
        },
    )
}

#[tokio::test]
async fn abandon_work_item_rejects_invalid_tokens() {
    let provider = provider().await;

    for ignore_attempt in [false, true] {
        let error = provider
            .abandon_work_item("unknown-token", None, ignore_attempt)
            .await
            .expect_err("invalid token should be rejected");
        assert_eq!(
            (error.operation.as_str(), error.is_retryable()),
            ("abandon_work_item", false)
        );
    }
}

#[tokio::test]
async fn runs_migrations_and_constructs_provider() {
    let provider = provider().await;
    assert_eq!(provider.name(), "duroxide-sea");
}

#[tokio::test]
async fn appends_and_reads_history() {
    let provider = provider().await;
    let event = orchestration_started_event("history-instance");

    provider
        .append_with_execution(
            "history-instance",
            INITIAL_EXECUTION_ID,
            vec![event.clone()],
        )
        .await
        .unwrap();

    let history = provider
        .read_with_execution("history-instance", INITIAL_EXECUTION_ID)
        .await
        .unwrap();
    assert_eq!(history, vec![event]);
}

#[tokio::test]
async fn fetches_and_acks_orchestration_item() {
    let provider = provider().await;
    provider
        .enqueue_for_orchestrator(start_item("orch-instance"), None)
        .await
        .unwrap();

    let (item, token, attempts) = provider
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("orchestration item");
    assert_eq!(item.instance, "orch-instance");
    assert_eq!(attempts, 1);

    let event = orchestration_started_event("orch-instance");

    provider
        .ack_orchestration_item(
            &token,
            INITIAL_EXECUTION_ID,
            vec![event],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("TestWorkflow".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    assert!(
        provider
            .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn fetches_and_acks_worker_item_with_completion() {
    let provider = provider().await;
    provider
        .enqueue_for_orchestrator(start_item("worker-instance"), None)
        .await
        .unwrap();
    let (_, start_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("start item");
    provider
        .ack_orchestration_item(
            &start_token,
            INITIAL_EXECUTION_ID,
            vec![orchestration_started_event("worker-instance")],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("TestWorkflow".to_string()),
                orchestration_version: Some("1.0.0".to_string()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();

    provider
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "worker-instance".to_string(),
            execution_id: INITIAL_EXECUTION_ID,
            id: 7,
            name: "activity".to_string(),
            input: "{}".to_string(),
            session_id: None,
            tag: None,
        })
        .await
        .unwrap();

    let (item, token, attempts) = provider
        .fetch_work_item(
            Duration::from_secs(5),
            Duration::ZERO,
            None,
            &duroxide::providers::TagFilter::DefaultOnly,
        )
        .await
        .unwrap()
        .expect("worker item");
    assert!(matches!(item, WorkItem::ActivityExecute { id: 7, .. }));
    assert_eq!(attempts, 1);

    provider
        .ack_work_item(
            &token,
            Some(WorkItem::ActivityCompleted {
                instance: "worker-instance".to_string(),
                execution_id: INITIAL_EXECUTION_ID,
                id: 7,
                result: "ok".to_string(),
            }),
        )
        .await
        .unwrap();

    let (item, _, _) = provider
        .fetch_orchestration_item(Duration::from_secs(5), Duration::ZERO, None)
        .await
        .unwrap()
        .expect("completion item");
    assert!(matches!(
        item.messages.as_slice(),
        [WorkItem::ActivityCompleted { id: 7, .. }]
    ));
}

#[tokio::test]
async fn queue_waits_release_the_only_connection_and_observe_external_writes() {
    use duroxide::providers::TagFilter;
    use duroxide_sea::{DuroxideSeaProvider, migration::Migrator};
    use sea_orm::{ConnectOptions, Database};
    use sea_orm_migration::MigratorTrait;
    use std::sync::Arc;

    let mut options = ConnectOptions::new(format!(
        "sqlite:file:notify-{}?mode=memory&cache=shared",
        uuid::Uuid::now_v7()
    ));
    options.max_connections(1).sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    let provider = Arc::new(DuroxideSeaProvider::new(db.clone()));
    let limit = Duration::from_secs(2);
    let reader = provider.clone();
    let orchestration = tokio::spawn(async move {
        reader
            .fetch_orchestration_item(limit, limit, None)
            .await
            .unwrap()
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    tokio::time::timeout(limit, db.ping())
        .await
        .unwrap()
        .unwrap();
    provider
        .enqueue_for_orchestrator(start_item("notify"), None)
        .await
        .unwrap();
    let (_, token, _) = tokio::time::timeout(limit, orchestration)
        .await
        .unwrap()
        .unwrap();
    provider
        .ack_orchestration_item(
            &token,
            INITIAL_EXECUTION_ID,
            vec![orchestration_started_event("notify")],
            vec![],
            vec![],
            ExecutionMetadata {
                orchestration_name: Some("TestWorkflow".into()),
                orchestration_version: Some("1.0.0".into()),
                status: Some("Running".into()),
                ..Default::default()
            },
            vec![],
        )
        .await
        .unwrap();
    let reader = provider.clone();
    let worker = tokio::spawn(async move {
        reader
            .fetch_work_item(limit, limit, None, &TagFilter::DefaultOnly)
            .await
            .unwrap()
            .unwrap()
    });
    let reader = provider.clone();
    let orchestration = tokio::spawn(async move {
        reader
            .fetch_orchestration_item(limit, limit, None)
            .await
            .unwrap()
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    tokio::time::timeout(limit, db.ping())
        .await
        .unwrap()
        .unwrap();
    // A different provider instance has independent notifications, as another process would.
    let external = DuroxideSeaProvider::new(db.clone());
    external
        .enqueue_for_worker(WorkItem::ActivityExecute {
            instance: "notify".into(),
            execution_id: INITIAL_EXECUTION_ID,
            id: 7,
            name: "activity".into(),
            input: "{}".into(),
            session_id: None,
            tag: None,
        })
        .await
        .unwrap();
    let (item, token, _) = tokio::time::timeout(limit, worker).await.unwrap().unwrap();
    assert!(matches!(item, WorkItem::ActivityExecute { id: 7, .. }));
    provider
        .ack_work_item(
            &token,
            Some(WorkItem::ActivityCompleted {
                instance: "notify".into(),
                execution_id: INITIAL_EXECUTION_ID,
                id: 7,
                result: "done".into(),
            }),
        )
        .await
        .unwrap();
    let (item, _, _) = tokio::time::timeout(limit, orchestration)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        item.messages.as_slice(),
        [WorkItem::ActivityCompleted { id: 7, .. }]
    ));
    let started = std::time::Instant::now();
    assert!(
        provider
            .fetch_work_item(
                limit,
                Duration::from_millis(20),
                None,
                &TagFilter::DefaultOnly
            )
            .await
            .unwrap()
            .is_none()
    );
    assert!(started.elapsed() < limit);
    tokio::time::timeout(limit, db.ping())
        .await
        .unwrap()
        .unwrap();
}
