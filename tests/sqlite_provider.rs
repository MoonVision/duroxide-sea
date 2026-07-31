use std::sync::Arc;
use std::time::Duration;

use duroxide::provider_validations::{
    ProviderFactory,
    kv_store::{
        test_kv_delta_clear_all_tombstones_store, test_kv_delta_client_reads_merged,
        test_kv_delta_snapshot_excludes_current_execution,
        test_kv_delta_snapshot_includes_completed_execution,
        test_kv_delta_tombstone_overrides_store,
    },
    test_get_instance_stats_carry_forward, test_get_instance_stats_kv_delta_only,
    test_get_instance_stats_kv_merged, test_instance_creation_via_metadata,
    test_worker_item_immediate_visibility, test_worker_peek_lock_semantics,
};
use duroxide::providers::{ExecutionMetadata, Provider, WorkItem};
use duroxide::{Event, EventKind, INITIAL_EXECUTION_ID};
use duroxide_sea::{DuroxideSeaProvider, migration::Migrator};
use sea_orm::{ConnectOptions, Database};
use sea_orm_migration::MigratorTrait;

async fn sea_provider() -> DuroxideSeaProvider {
    let database_url = format!(
        "sqlite:file:duroxide-sea-{}?mode=memory&cache=shared",
        uuid::Uuid::now_v7()
    );
    let mut options = ConnectOptions::new(database_url);
    options.max_connections(5).sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    DuroxideSeaProvider::new(db)
}

async fn provider() -> Arc<DuroxideSeaProvider> {
    Arc::new(sea_provider().await)
}

struct SeaSqliteFactory;

#[async_trait::async_trait]
impl ProviderFactory for SeaSqliteFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(sea_provider().await)
    }

    fn lock_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }
}

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
async fn provider_validation_worker_item_immediate_visibility() {
    test_worker_item_immediate_visibility(&SeaSqliteFactory).await;
}

#[tokio::test]
async fn provider_validation_worker_peek_lock_semantics() {
    test_worker_peek_lock_semantics(&SeaSqliteFactory).await;
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
async fn provider_validation_instance_creation_via_metadata() {
    test_instance_creation_via_metadata(&SeaSqliteFactory).await;
}

#[tokio::test]
async fn provider_validation_kv_snapshot_excludes_current_execution_delta() {
    test_kv_delta_snapshot_excludes_current_execution(&SeaSqliteFactory).await;
}

#[tokio::test]
async fn provider_validation_kv_snapshot_includes_completed_execution() {
    test_kv_delta_snapshot_includes_completed_execution(&SeaSqliteFactory).await;
}

#[tokio::test]
async fn provider_validation_kv_client_reads_merged_state() {
    test_kv_delta_client_reads_merged(&SeaSqliteFactory).await;
}

#[tokio::test]
async fn provider_validation_kv_tombstone_overrides_stored_value() {
    test_kv_delta_tombstone_overrides_store(&SeaSqliteFactory).await;
}

#[tokio::test]
async fn provider_validation_kv_clear_all_tombstones_stored_values() {
    test_kv_delta_clear_all_tombstones_store(&SeaSqliteFactory).await;
}

#[tokio::test]
async fn provider_validation_kv_stats_include_delta_only_values() {
    test_get_instance_stats_kv_delta_only(&SeaSqliteFactory).await;
}

#[tokio::test]
async fn provider_validation_kv_stats_merge_store_and_delta() {
    test_get_instance_stats_kv_merged(&SeaSqliteFactory).await;
}

#[tokio::test]
async fn provider_validation_stats_count_carry_forward_events() {
    test_get_instance_stats_carry_forward(&SeaSqliteFactory).await;
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
