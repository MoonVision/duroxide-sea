use std::sync::Arc;
use std::time::Duration;

use duroxide::provider_validations::ProviderFactory;
use duroxide::providers::Provider;
use duroxide_sea::DuroxideSeaProvider;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};

mod support;

const TEST_LOCK_TIMEOUT: Duration = Duration::from_secs(1);

struct SeaSqliteFactory;

#[async_trait::async_trait]
impl ProviderFactory for SeaSqliteFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        support::sqlite_provider().await
    }

    fn lock_timeout(&self) -> Duration {
        TEST_LOCK_TIMEOUT
    }
}

struct SharedSeaSqliteFactory {
    db: DatabaseConnection,
    provider: Arc<DuroxideSeaProvider>,
}

impl SharedSeaSqliteFactory {
    async fn new() -> Self {
        let db = support::sqlite_database().await;
        let provider = Arc::new(DuroxideSeaProvider::new(db.clone()));
        Self { db, provider }
    }
}

#[async_trait::async_trait]
impl ProviderFactory for SharedSeaSqliteFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        self.provider.clone()
    }

    fn lock_timeout(&self) -> Duration {
        TEST_LOCK_TIMEOUT
    }

    async fn corrupt_instance_history(&self, instance: &str) {
        self.db
            .execute_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "UPDATE duroxide_history SET event_data = 'NOT_VALID_JSON{{{' WHERE instance_id = ?",
                [instance.into()],
            ))
            .await
            .expect("failed to corrupt instance history");
    }

    async fn get_max_attempt_count(&self, instance: &str) -> u32 {
        let row = self
            .db
            .query_one_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT COALESCE(MAX(attempt_count), 0) AS max_attempt_count
                 FROM duroxide_orchestrator_queue
                 WHERE instance_id = ?",
                [instance.into()],
            ))
            .await
            .expect("failed to query maximum attempt count")
            .expect("maximum attempt query should return one row");
        let count: i64 = row
            .try_get("", "max_attempt_count")
            .expect("maximum attempt count should be an integer");
        u32::try_from(count).expect("maximum attempt count should fit in a u32")
    }
}

macro_rules! validation_tests {
    ($factory:expr; $validations:ident; $( $test:ident ),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $test() {
                let factory = $factory;
                $validations::$test(&factory).await;
            }
        )+
    };
}

// TODO: Add the 38 ProviderAdmin-dependent validations when duroxide-sea
// implements that optional capability: management queries, deletion, pruning,
// bulk deletion, and the dependent cancellation, capability-filtering, and KV tests.

mod instance_creation {
    use super::*;
    use duroxide::provider_validations as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_instance_creation_via_metadata,
        test_no_instance_creation_on_enqueue,
        test_null_version_handling,
        test_sub_orchestration_instance_creation,
    }
}

mod atomicity {
    use super::*;
    use duroxide::provider_validations as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_atomicity_failure_rollback,
        test_multi_operation_atomic_ack,
        test_lock_released_only_on_successful_ack,
        test_concurrent_ack_prevention,
    }
}

mod error_handling {
    use super::*;
    use duroxide::provider_validations as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_corrupted_serialization_data,
        test_duplicate_event_id_rejection,
        test_invalid_lock_token_on_ack,
        test_lock_expiration_during_ack,
        test_missing_instance_metadata,
    }

    validation_tests! {
        SharedSeaSqliteFactory::new().await;
        validations;
        test_read_corrupted_history_returns_error,
        test_read_with_execution_corrupted_history_returns_error,
    }
}

mod instance_locking {
    use super::*;
    use duroxide::provider_validations as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_ack_only_affects_locked_messages,
        test_completions_arriving_during_lock_blocked,
        test_concurrent_instance_fetching,
        test_cross_instance_lock_isolation,
        test_exclusive_instance_lock,
        test_invalid_lock_token_rejection,
        test_lock_token_uniqueness,
        test_message_tagging_during_lock,
        test_multi_threaded_lock_contention,
        test_multi_threaded_lock_expiration_recovery,
        test_multi_threaded_no_duplicate_processing,
    }
}

mod lock_expiration {
    use super::*;
    use duroxide::provider_validations as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_abandon_releases_lock_immediately,
        test_abandon_work_item_releases_lock,
        test_abandon_work_item_with_delay,
        test_concurrent_lock_attempts_respect_expiration,
        test_lock_expires_after_timeout,
        test_lock_renewal_on_ack,
        test_orchestration_lock_renewal_after_expiration,
        test_worker_ack_fails_after_lock_expiry,
        test_worker_lock_renewal_after_ack,
        test_worker_lock_renewal_after_expiration,
        test_worker_lock_renewal_extends_timeout,
        test_worker_lock_renewal_invalid_token,
        test_worker_lock_renewal_success,
    }
}

mod multi_execution {
    use super::*;
    use duroxide::provider_validations as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_continue_as_new_creates_new_execution,
        test_execution_history_persistence,
        test_execution_id_sequencing,
        test_execution_isolation,
        test_latest_execution_detection,
    }
}

mod queue_semantics {
    use super::*;
    use duroxide::provider_validations as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_lost_lock_token_handling,
        test_orphan_queue_messages_dropped,
        test_timer_delayed_visibility,
        test_worker_ack_atomicity,
        test_worker_delayed_visibility_skips_future_items,
        test_worker_item_immediate_visibility,
        test_worker_peek_lock_semantics,
        test_worker_queue_fifo_ordering,
    }
}

mod instance_stats {
    use super::*;
    use duroxide::provider_validations as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_get_instance_stats_carry_forward,
        test_get_instance_stats_history,
        test_get_instance_stats_kv,
        test_get_instance_stats_kv_delta_only,
        test_get_instance_stats_kv_merged,
        test_get_instance_stats_nonexistent,
    }
}

mod polling {
    use super::*;
    use duroxide::provider_validations::long_polling as validations;

    // The two long-poll waiting validations are intentionally omitted because
    // the SQLite provider implements short polling and ignores poll_timeout.

    #[tokio::test]
    async fn test_short_poll_returns_immediately() {
        let provider = SeaSqliteFactory.create_provider().await;
        validations::test_short_poll_returns_immediately(
            &*provider,
            SeaSqliteFactory.short_poll_threshold(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_short_poll_work_item_returns_immediately() {
        let provider = SeaSqliteFactory.create_provider().await;
        validations::test_short_poll_work_item_returns_immediately(
            &*provider,
            SeaSqliteFactory.short_poll_threshold(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_respects_timeout_upper_bound() {
        let provider = SeaSqliteFactory.create_provider().await;
        validations::test_fetch_respects_timeout_upper_bound(&*provider).await;
    }
}

mod poison_message {
    use super::*;
    use duroxide::provider_validations::poison_message as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        abandon_orchestration_item_ignore_attempt_decrements,
        abandon_work_item_ignore_attempt_decrements,
        attempt_count_is_per_message,
        ignore_attempt_never_goes_negative,
        max_attempt_count_across_message_batch,
        orchestration_attempt_count_increments_on_refetch,
        orchestration_attempt_count_starts_at_one,
        worker_attempt_count_increments_on_lock_expiry,
        worker_attempt_count_starts_at_one,
    }
}

mod cancellation {
    use super::*;
    use duroxide::provider_validations as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_ack_work_item_fails_when_entry_deleted,
        test_ack_work_item_none_deletes_without_enqueue,
        test_batch_cancellation_deletes_multiple_activities,
        test_cancelled_activities_deleted_from_worker_queue,
        test_cancelling_nonexistent_activities_is_idempotent,
        test_fetch_returns_missing_state_when_instance_deleted,
        test_fetch_returns_running_state_for_active_orchestration,
        test_fetch_returns_terminal_state_when_orchestration_completed,
        test_fetch_returns_terminal_state_when_orchestration_continued_as_new,
        test_fetch_returns_terminal_state_when_orchestration_failed,
        test_renew_fails_when_entry_deleted,
        test_renew_returns_missing_when_instance_deleted,
        test_renew_returns_running_when_orchestration_active,
        test_renew_returns_terminal_when_orchestration_completed,
        test_same_activity_in_worker_items_and_cancelled_is_noop,
    }
}

mod capability_filtering {
    use super::*;
    use duroxide::provider_validations::capability_filtering as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_ack_stores_pinned_version_via_metadata_update,
        test_concurrent_filtered_fetch_no_double_lock,
        test_continue_as_new_execution_gets_own_pinned_version,
        test_fetch_filter_boundary_versions,
        test_fetch_filter_does_not_lock_skipped_instances,
        test_fetch_filter_null_pinned_version_always_compatible,
        test_fetch_filter_skips_incompatible_selects_compatible,
        test_fetch_single_range_only_uses_first_range,
        test_fetch_with_compatible_filter_returns_item,
        test_fetch_with_filter_none_returns_any_item,
        test_fetch_with_incompatible_filter_skips_item,
        test_filter_with_empty_supported_versions_returns_nothing,
        test_pinned_version_immutable_across_ack_cycles,
        test_pinned_version_stored_via_ack_metadata,
        test_provider_updates_pinned_version_when_told,
    }

    validation_tests! {
        SharedSeaSqliteFactory::new().await;
        validations;
        test_fetch_corrupted_history_filtered_vs_unfiltered,
        test_fetch_deserialization_error_eventually_reaches_poison,
        test_fetch_deserialization_error_increments_attempt_count,
        test_fetch_filter_applied_before_history_deserialization,
    }
}

mod sessions {
    use super::*;
    use duroxide::provider_validations::sessions as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_abandoned_session_item_ignore_attempt,
        test_abandoned_session_item_retryable,
        test_ack_updates_session_last_activity,
        test_activity_lock_expires_session_lock_valid_same_worker_refetches,
        test_both_locks_expire_different_worker_claims,
        test_cleanup_keeps_active_sessions,
        test_cleanup_keeps_sessions_with_pending_items,
        test_cleanup_removes_expired_no_items,
        test_cleanup_then_new_item_recreates_session,
        test_concurrent_session_claim_only_one_wins,
        test_different_sessions_different_workers,
        test_mixed_session_and_non_session_items,
        test_non_session_items_fetchable_by_any_worker,
        test_non_session_items_returned_with_session_config,
        test_none_session_skips_session_items,
        test_original_worker_reclaims_expired_session,
        test_renew_session_lock_active,
        test_renew_session_lock_after_expiry_returns_zero,
        test_renew_session_lock_no_sessions,
        test_renew_session_lock_skips_idle,
        test_renew_work_item_updates_session_last_activity,
        test_session_affinity_blocks_other_worker,
        test_session_affinity_same_worker,
        test_session_claimable_after_lock_expiry,
        test_session_item_claimable_when_no_session,
        test_session_items_processed_in_order,
        test_session_lock_expires_activity_lock_valid_ack_succeeds,
        test_session_lock_expires_new_owner_gets_redelivery,
        test_session_lock_expires_same_worker_reacquires,
        test_session_lock_renewal_extends_past_original_timeout,
        test_session_takeover_after_lock_expiry,
        test_shared_worker_id_any_caller_can_fetch_owned_session,
        test_some_session_returns_all_items,
    }
}

mod custom_status {
    use super::*;
    use duroxide::provider_validations::custom_status as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_custom_status_clear,
        test_custom_status_default_on_new_instance,
        test_custom_status_none_preserves,
        test_custom_status_nonexistent_instance,
        test_custom_status_polling_no_change,
        test_custom_status_set,
        test_custom_status_version_increments,
    }
}

mod tag_filtering {
    use super::*;
    use duroxide::provider_validations::tag_filtering as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_any_filter_fetches_everything,
        test_default_and_fetches_untagged_and_matching,
        test_default_only_fetches_untagged,
        test_multi_runtime_tag_isolation,
        test_multi_tag_filter,
        test_none_filter_returns_nothing,
        test_tag_preserved_through_ack_orchestration_item,
        test_tag_round_trip_preservation,
        test_tag_survives_abandon_and_refetch,
        test_tags_fetches_only_matching,
    }
}

mod kv_store {
    use super::*;
    use duroxide::provider_validations::kv_store as validations;

    validation_tests! {
        SeaSqliteFactory;
        validations;
        test_kv_clear_all,
        test_kv_clear_isolation,
        test_kv_clear_nonexistent_key,
        test_kv_clear_single,
        test_kv_cross_execution_overwrite,
        test_kv_cross_execution_remove_readd,
        test_kv_delta_clear_all_tombstones_store,
        test_kv_delta_client_reads_merged,
        test_kv_delta_merged_on_can,
        test_kv_delta_merged_on_completion,
        test_kv_delta_snapshot_excludes_current_execution,
        test_kv_delta_snapshot_includes_completed_execution,
        test_kv_delta_tombstone_overrides_store,
        test_kv_empty_value,
        test_kv_get_nonexistent,
        test_kv_get_unknown_instance,
        test_kv_instance_isolation,
        test_kv_large_value,
        test_kv_overwrite,
        test_kv_set_after_clear,
        test_kv_set_and_get,
        test_kv_snapshot_after_clear_all,
        test_kv_snapshot_after_clear_single,
        test_kv_snapshot_cross_execution,
        test_kv_snapshot_empty,
        test_kv_snapshot_in_fetch,
        test_kv_special_chars_in_key,
    }
}
