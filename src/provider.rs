use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use duroxide::providers::{
    DispatcherCapabilityFilter, ExecutionMetadata, OrchestrationItem, Provider, ProviderError,
    ScheduledActivityIdentifier, SessionFetchConfig, TagFilter, WorkItem,
};
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, SystemStats};
use sea_orm::{
    ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction, QueryResult,
    Statement, TransactionTrait, Value,
};
use uuid::Uuid;

use crate::kv_store;

#[derive(Clone)]
pub struct DuroxideSeaProvider {
    db: DatabaseConnection,
}

impl DuroxideSeaProvider {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn enqueue_for_orchestrator_in_tx(
        txn: &DatabaseTransaction,
        item: WorkItem,
        delay: Option<Duration>,
    ) -> Result<(), ProviderError> {
        enqueue_orchestrator_on(txn, item, delay).await
    }
}

#[async_trait]
impl Provider for DuroxideSeaProvider {
    fn name(&self) -> &str {
        "duroxide-sea"
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        _poll_timeout: Duration,
        filter: Option<&DispatcherCapabilityFilter>,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        let txn = self
            .db
            .begin()
            .await
            .map_err(|e| db_error("fetch_orchestration_item", e))?;
        let now = now_ms();

        let candidates = query_all(
            &txn,
            "SELECT q.instance_id, MIN(q.id) AS first_id
             FROM duroxide_orchestrator_queue q
             LEFT JOIN duroxide_instance_locks il ON q.instance_id = il.instance_id
             WHERE q.visible_at <= ? AND (il.instance_id IS NULL OR il.locked_until <= ?)
             GROUP BY q.instance_id
             ORDER BY first_id
             LIMIT 50",
            vec![v_i64(now), v_i64(now)],
            "fetch_orchestration_item",
        )
        .await?;

        for candidate in candidates {
            let instance_id: String = get(&candidate, "instance_id", "fetch_orchestration_item")?;

            if !is_capability_compatible(&txn, &instance_id, filter).await? {
                continue;
            }

            let lock_token = Uuid::now_v7().to_string();
            let locked_until = now.saturating_add(duration_ms(lock_timeout));

            let lock_result = execute(
                &txn,
                "INSERT INTO duroxide_instance_locks (instance_id, lock_token, locked_until, locked_at)
                 VALUES (?, ?, ?, ?)
                 ON CONFLICT(instance_id) DO UPDATE
                 SET lock_token = excluded.lock_token,
                     locked_until = excluded.locked_until,
                     locked_at = excluded.locked_at
                 WHERE duroxide_instance_locks.locked_until <= ?",
                vec![
                    v_str(&instance_id),
                    v_str(&lock_token),
                    v_i64(locked_until),
                    v_i64(now),
                    v_i64(now),
                ],
                "fetch_orchestration_item",
            )
            .await?;

            if lock_result == 0 {
                continue;
            }

            let updated = execute(
                &txn,
                "UPDATE duroxide_orchestrator_queue
                 SET lock_token = ?, locked_until = ?, attempt_count = attempt_count + 1
                 WHERE instance_id = ? AND visible_at <= ? AND (lock_token IS NULL OR locked_until <= ?)",
                vec![
                    v_str(&lock_token),
                    v_i64(locked_until),
                    v_str(&instance_id),
                    v_i64(now),
                    v_i64(now),
                ],
                "fetch_orchestration_item",
            )
            .await?;

            if updated == 0 {
                let _ = execute(
                    &txn,
                    "DELETE FROM duroxide_instance_locks WHERE lock_token = ?",
                    vec![v_str(&lock_token)],
                    "fetch_orchestration_item",
                )
                .await;
                txn.commit()
                    .await
                    .map_err(|e| db_error("fetch_orchestration_item", e))?;
                return Ok(None);
            }

            let message_rows = query_all(
                &txn,
                "SELECT id, work_item, attempt_count
                 FROM duroxide_orchestrator_queue
                 WHERE lock_token = ?
                 ORDER BY id",
                vec![v_str(&lock_token)],
                "fetch_orchestration_item",
            )
            .await?;

            let mut messages = Vec::with_capacity(message_rows.len());
            let mut max_attempt_count = 0u32;
            for row in &message_rows {
                let attempt_count: i64 = get(row, "attempt_count", "fetch_orchestration_item")?;
                max_attempt_count = max_attempt_count.max(attempt_count as u32);
                let work_item_json: String = get(row, "work_item", "fetch_orchestration_item")?;
                let item = serde_json::from_str(&work_item_json).map_err(|e| {
                    ProviderError::permanent(
                        "fetch_orchestration_item",
                        format!("Failed to deserialize work item: {e}"),
                    )
                })?;
                messages.push(item);
            }

            let instance_row = query_one(
                &txn,
                "SELECT orchestration_name, orchestration_version, current_execution_id
                 FROM duroxide_instances
                 WHERE instance_id = ?",
                vec![v_str(&instance_id)],
                "fetch_orchestration_item",
            )
            .await?;

            let (orchestration_name, version, execution_id, history) =
                if let Some(row) = instance_row {
                    let name: String = get(&row, "orchestration_name", "fetch_orchestration_item")?;
                    let version: Option<String> =
                        get_opt(&row, "orchestration_version", "fetch_orchestration_item")?;
                    let execution_id: i64 =
                        get(&row, "current_execution_id", "fetch_orchestration_item")?;
                    match read_history_on(&txn, &instance_id, execution_id as u64).await {
                        Ok(history) => (
                            name,
                            version.unwrap_or_else(|| "unknown".to_string()),
                            execution_id as u64,
                            history,
                        ),
                        Err(e) => {
                            let kv_snapshot = HashMap::new();
                            txn.commit()
                                .await
                                .map_err(|e| db_error("fetch_orchestration_item", e))?;
                            return Ok(Some((
                                OrchestrationItem {
                                    instance: instance_id,
                                    orchestration_name: name,
                                    execution_id: execution_id as u64,
                                    version: version.unwrap_or_else(|| "unknown".to_string()),
                                    history: Vec::new(),
                                    messages,
                                    history_error: Some(e.to_string()),
                                    kv_snapshot,
                                },
                                lock_token,
                                max_attempt_count,
                            )));
                        }
                    }
                } else if let Some((name, version, execution_id)) =
                    start_metadata_from_messages(&messages)
                {
                    (
                        name,
                        version.unwrap_or_else(|| "unknown".to_string()),
                        execution_id,
                        Vec::new(),
                    )
                } else if messages
                    .iter()
                    .all(|item| matches!(item, WorkItem::QueueMessage { .. }))
                {
                    execute(
                        &txn,
                        "DELETE FROM duroxide_orchestrator_queue WHERE lock_token = ?",
                        vec![v_str(&lock_token)],
                        "fetch_orchestration_item",
                    )
                    .await?;
                    execute(
                        &txn,
                        "DELETE FROM duroxide_instance_locks WHERE lock_token = ?",
                        vec![v_str(&lock_token)],
                        "fetch_orchestration_item",
                    )
                    .await?;
                    txn.commit()
                        .await
                        .map_err(|e| db_error("fetch_orchestration_item", e))?;
                    return Ok(None);
                } else {
                    txn.rollback().await.ok();
                    return Ok(None);
                };

            let kv_snapshot = kv_store::snapshot(&txn, &instance_id).await?;

            txn.commit()
                .await
                .map_err(|e| db_error("fetch_orchestration_item", e))?;

            return Ok(Some((
                OrchestrationItem {
                    instance: instance_id,
                    orchestration_name,
                    execution_id,
                    version,
                    history,
                    messages,
                    history_error: None,
                    kv_snapshot,
                },
                lock_token,
                max_attempt_count,
            )));
        }

        txn.commit()
            .await
            .map_err(|e| db_error("fetch_orchestration_item", e))?;
        Ok(None)
    }

    async fn ack_orchestration_item(
        &self,
        lock_token: &str,
        execution_id: u64,
        history_delta: Vec<Event>,
        worker_items: Vec<WorkItem>,
        orchestrator_items: Vec<WorkItem>,
        metadata: ExecutionMetadata,
        cancelled_activities: Vec<ScheduledActivityIdentifier>,
    ) -> Result<(), ProviderError> {
        let txn = self
            .db
            .begin()
            .await
            .map_err(|e| db_error("ack_orchestration_item", e))?;
        let now = now_ms();

        let lock_row = query_one(
            &txn,
            "SELECT instance_id FROM duroxide_instance_locks WHERE lock_token = ? AND locked_until > ?",
            vec![v_str(lock_token), v_i64(now)],
            "ack_orchestration_item",
        )
        .await?
        .ok_or_else(|| ProviderError::permanent("ack_orchestration_item", "Invalid or expired lock token"))?;
        let instance_id: String = get(&lock_row, "instance_id", "ack_orchestration_item")?;

        ensure_instance_and_execution_on(&txn, &instance_id, execution_id, &metadata).await?;
        append_history_on(&txn, &instance_id, execution_id, &history_delta).await?;
        apply_metadata_on(&txn, &instance_id, execution_id, &metadata).await?;
        apply_custom_status_on(&txn, &instance_id, &history_delta).await?;
        kv_store::apply_events(
            &txn,
            &instance_id,
            history_delta,
            metadata.status.as_deref(),
        )
        .await?;

        for item in worker_items {
            enqueue_worker_on(&txn, item).await?;
        }

        for item in orchestrator_items {
            enqueue_orchestrator_on(&txn, item, None).await?;
        }

        for cancelled in cancelled_activities {
            execute(
                &txn,
                "DELETE FROM duroxide_worker_queue
                 WHERE instance_id = ? AND execution_id = ? AND activity_id = ?",
                vec![
                    v_str(cancelled.instance),
                    v_u64(cancelled.execution_id),
                    v_u64(cancelled.activity_id),
                ],
                "ack_orchestration_item",
            )
            .await?;
        }

        execute(
            &txn,
            "DELETE FROM duroxide_orchestrator_queue WHERE lock_token = ?",
            vec![v_str(lock_token)],
            "ack_orchestration_item",
        )
        .await?;
        execute(
            &txn,
            "DELETE FROM duroxide_instance_locks WHERE lock_token = ?",
            vec![v_str(lock_token)],
            "ack_orchestration_item",
        )
        .await?;

        txn.commit()
            .await
            .map_err(|e| db_error("ack_orchestration_item", e))?;
        Ok(())
    }

    async fn abandon_orchestration_item(
        &self,
        lock_token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let txn = self
            .db
            .begin()
            .await
            .map_err(|e| db_error("abandon_orchestration_item", e))?;
        let visible_at = now_ms().saturating_add(delay.map_or(0, duration_ms));

        let changed = if ignore_attempt {
            execute(
                &txn,
                "UPDATE duroxide_orchestrator_queue
                 SET lock_token = NULL, locked_until = NULL, visible_at = ?,
                     attempt_count = CASE WHEN attempt_count > 0 THEN attempt_count - 1 ELSE 0 END
                 WHERE lock_token = ?",
                vec![v_i64(visible_at), v_str(lock_token)],
                "abandon_orchestration_item",
            )
            .await?
        } else {
            execute(
                &txn,
                "UPDATE duroxide_orchestrator_queue
                 SET lock_token = NULL, locked_until = NULL, visible_at = ?
                 WHERE lock_token = ?",
                vec![v_i64(visible_at), v_str(lock_token)],
                "abandon_orchestration_item",
            )
            .await?
        };

        execute(
            &txn,
            "DELETE FROM duroxide_instance_locks WHERE lock_token = ?",
            vec![v_str(lock_token)],
            "abandon_orchestration_item",
        )
        .await?;

        txn.commit()
            .await
            .map_err(|e| db_error("abandon_orchestration_item", e))?;

        if changed == 0 {
            return Err(ProviderError::permanent(
                "abandon_orchestration_item",
                "Invalid lock token",
            ));
        }
        Ok(())
    }

    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        let row = query_one(
            &self.db,
            "SELECT current_execution_id FROM duroxide_instances WHERE instance_id = ?",
            vec![v_str(instance)],
            "read",
        )
        .await?;
        let execution_id = if let Some(row) = row {
            let id: i64 = get(&row, "current_execution_id", "read")?;
            id as u64
        } else {
            latest_execution_id_on(&self.db, instance)
                .await?
                .unwrap_or(duroxide::INITIAL_EXECUTION_ID)
        };
        read_history_on(&self.db, instance, execution_id).await
    }

    async fn read_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        read_history_on(&self.db, instance, execution_id).await
    }

    async fn append_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
        new_events: Vec<Event>,
    ) -> Result<(), ProviderError> {
        append_history_on(&self.db, instance, execution_id, &new_events).await
    }

    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        enqueue_worker_on(&self.db, item).await
    }

    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        _poll_timeout: Duration,
        session: Option<&SessionFetchConfig>,
        tag_filter: &TagFilter,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        if matches!(tag_filter, TagFilter::None) {
            return Ok(None);
        }

        let txn = self
            .db
            .begin()
            .await
            .map_err(|e| db_error("fetch_work_item", e))?;
        let now = now_ms();
        let rows = query_all(
            &txn,
            "SELECT id, work_item, session_id, tag
             FROM duroxide_worker_queue
             WHERE visible_at <= ? AND (lock_token IS NULL OR locked_until <= ?)
             ORDER BY id
             LIMIT 100",
            vec![v_i64(now), v_i64(now)],
            "fetch_work_item",
        )
        .await?;

        for row in rows {
            let id: i64 = get(&row, "id", "fetch_work_item")?;
            let tag: Option<String> = get_opt(&row, "tag", "fetch_work_item")?;
            if !tag_filter.matches(tag.as_deref()) {
                continue;
            }

            let session_id: Option<String> = get_opt(&row, "session_id", "fetch_work_item")?;
            if !claim_session_if_eligible(&txn, session, session_id.as_deref(), now).await? {
                continue;
            }

            let lock_token = Uuid::now_v7().to_string();
            let locked_until = now.saturating_add(duration_ms(lock_timeout));
            let changed = execute(
                &txn,
                "UPDATE duroxide_worker_queue
                 SET lock_token = ?, locked_until = ?, attempt_count = attempt_count + 1
                 WHERE id = ? AND visible_at <= ? AND (lock_token IS NULL OR locked_until <= ?)",
                vec![
                    v_str(&lock_token),
                    v_i64(locked_until),
                    v_i64(id),
                    v_i64(now),
                    v_i64(now),
                ],
                "fetch_work_item",
            )
            .await?;
            if changed == 0 {
                continue;
            }

            if let Some(session_id) = &session_id {
                touch_session_on(&txn, session_id, now).await?;
            }

            let locked_row = query_one(
                &txn,
                "SELECT work_item, attempt_count FROM duroxide_worker_queue WHERE id = ?",
                vec![v_i64(id)],
                "fetch_work_item",
            )
            .await?
            .ok_or_else(|| ProviderError::permanent("fetch_work_item", "Locked row disappeared"))?;
            let work_json: String = get(&locked_row, "work_item", "fetch_work_item")?;
            let attempt_count: i64 = get(&locked_row, "attempt_count", "fetch_work_item")?;
            let item = serde_json::from_str(&work_json).map_err(|e| {
                ProviderError::permanent(
                    "fetch_work_item",
                    format!("Failed to deserialize work item: {e}"),
                )
            })?;

            txn.commit()
                .await
                .map_err(|e| db_error("fetch_work_item", e))?;
            return Ok(Some((item, lock_token, attempt_count as u32)));
        }

        txn.commit()
            .await
            .map_err(|e| db_error("fetch_work_item", e))?;
        Ok(None)
    }

    async fn ack_work_item(
        &self,
        token: &str,
        completion: Option<WorkItem>,
    ) -> Result<(), ProviderError> {
        let txn = self
            .db
            .begin()
            .await
            .map_err(|e| db_error("ack_work_item", e))?;
        let now = now_ms();
        let row = query_one(
            &txn,
            "SELECT session_id FROM duroxide_worker_queue WHERE lock_token = ? AND locked_until > ?",
            vec![v_str(token), v_i64(now)],
            "ack_work_item",
        )
        .await?;
        let session_id: Option<String> = row
            .as_ref()
            .map(|row| get_opt::<String>(row, "session_id", "ack_work_item"))
            .transpose()?
            .flatten();

        let deleted = execute(
            &txn,
            "DELETE FROM duroxide_worker_queue WHERE lock_token = ? AND locked_until > ?",
            vec![v_str(token), v_i64(now)],
            "ack_work_item",
        )
        .await?;
        if deleted == 0 {
            txn.rollback().await.ok();
            return Err(ProviderError::permanent(
                "ack_work_item",
                "Lock token not found or expired",
            ));
        }

        if let Some(session_id) = session_id {
            touch_session_on(&txn, &session_id, now).await?;
        }

        if let Some(completion) = completion {
            enqueue_orchestrator_on(&txn, completion, None).await?;
        }

        txn.commit()
            .await
            .map_err(|e| db_error("ack_work_item", e))?;
        Ok(())
    }

    async fn renew_work_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        let txn = self
            .db
            .begin()
            .await
            .map_err(|e| db_error("renew_work_item_lock", e))?;
        let now = now_ms();
        let row = query_one(
            &txn,
            "SELECT session_id FROM duroxide_worker_queue WHERE lock_token = ? AND locked_until > ?",
            vec![v_str(token), v_i64(now)],
            "renew_work_item_lock",
        )
        .await?;
        let session_id: Option<String> = row
            .as_ref()
            .map(|row| get_opt::<String>(row, "session_id", "renew_work_item_lock"))
            .transpose()?
            .flatten();
        let changed = execute(
            &txn,
            "UPDATE duroxide_worker_queue SET locked_until = ? WHERE lock_token = ? AND locked_until > ?",
            vec![
                v_i64(now.saturating_add(duration_ms(extend_for))),
                v_str(token),
                v_i64(now),
            ],
            "renew_work_item_lock",
        )
        .await?;
        if changed == 0 {
            txn.rollback().await.ok();
            return Err(ProviderError::permanent(
                "renew_work_item_lock",
                "Lock token not found or expired",
            ));
        }
        if let Some(session_id) = session_id {
            touch_session_on(&txn, &session_id, now).await?;
        }
        txn.commit()
            .await
            .map_err(|e| db_error("renew_work_item_lock", e))?;
        Ok(())
    }

    async fn renew_session_lock(
        &self,
        owner_ids: &[&str],
        extend_for: Duration,
        idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        if owner_ids.is_empty() {
            return Ok(0);
        }
        let now = now_ms();
        let idle_cutoff = now.saturating_sub(duration_ms(idle_timeout));
        let placeholders = placeholders(self.db.get_database_backend(), 3, owner_ids.len());
        let sql = format!(
            "UPDATE duroxide_sessions
             SET locked_until = ?
             WHERE locked_until > ? AND last_activity_at > ? AND worker_id IN ({placeholders})"
        );
        let mut values = vec![
            v_i64(now.saturating_add(duration_ms(extend_for))),
            v_i64(now),
            v_i64(idle_cutoff),
        ];
        values.extend(owner_ids.iter().map(|id| v_str(*id)));
        execute(&self.db, &sql, values, "renew_session_lock")
            .await
            .map(|count| count as usize)
    }

    async fn cleanup_orphaned_sessions(
        &self,
        _idle_timeout: Duration,
    ) -> Result<usize, ProviderError> {
        let now = now_ms();
        execute(
            &self.db,
            "DELETE FROM duroxide_sessions
             WHERE locked_until < ?
               AND NOT EXISTS (
                   SELECT 1 FROM duroxide_worker_queue
                   WHERE duroxide_worker_queue.session_id = duroxide_sessions.session_id
               )",
            vec![v_i64(now)],
            "cleanup_orphaned_sessions",
        )
        .await
        .map(|count| count as usize)
    }

    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let visible_at = now_ms().saturating_add(delay.map_or(0, duration_ms));
        let changed = if ignore_attempt {
            execute(
                &self.db,
                "UPDATE duroxide_worker_queue
                 SET lock_token = NULL, locked_until = NULL, visible_at = ?,
                     attempt_count = CASE WHEN attempt_count > 0 THEN attempt_count - 1 ELSE 0 END
                 WHERE lock_token = ?",
                vec![v_i64(visible_at), v_str(token)],
                "abandon_work_item",
            )
            .await?
        } else {
            execute(
                &self.db,
                "UPDATE duroxide_worker_queue
                 SET lock_token = NULL, locked_until = NULL, visible_at = ?
                 WHERE lock_token = ?",
                vec![v_i64(visible_at), v_str(token)],
                "abandon_work_item",
            )
            .await?
        };

        if changed == 0 {
            return Err(ProviderError::permanent(
                "abandon_work_item",
                "Invalid lock token or already acked",
            ));
        }

        Ok(())
    }

    async fn renew_orchestration_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        let now = now_ms();
        let locked_until = now.saturating_add(duration_ms(extend_for));
        let changed = execute(
            &self.db,
            "UPDATE duroxide_instance_locks
             SET locked_until = ?
             WHERE lock_token = ? AND locked_until > ?",
            vec![v_i64(locked_until), v_str(token), v_i64(now)],
            "renew_orchestration_item_lock",
        )
        .await?;
        if changed == 0 {
            return Err(ProviderError::permanent(
                "renew_orchestration_item_lock",
                "Lock token not found or expired",
            ));
        }
        execute(
            &self.db,
            "UPDATE duroxide_orchestrator_queue SET locked_until = ? WHERE lock_token = ?",
            vec![v_i64(locked_until), v_str(token)],
            "renew_orchestration_item_lock",
        )
        .await?;
        Ok(())
    }

    async fn enqueue_for_orchestrator(
        &self,
        item: WorkItem,
        delay: Option<Duration>,
    ) -> Result<(), ProviderError> {
        enqueue_orchestrator_on(&self.db, item, delay).await
    }

    async fn get_custom_status(
        &self,
        instance: &str,
        last_seen_version: u64,
    ) -> Result<Option<(Option<String>, u64)>, ProviderError> {
        let row = query_one(
            &self.db,
            "SELECT custom_status, custom_status_version
             FROM duroxide_instances
             WHERE instance_id = ? AND custom_status_version > ?",
            vec![v_str(instance), v_u64(last_seen_version)],
            "get_custom_status",
        )
        .await?;
        row.map(|row| {
            let status = get_opt(&row, "custom_status", "get_custom_status")?;
            let version: i64 = get(&row, "custom_status_version", "get_custom_status")?;
            Ok((status, version as u64))
        })
        .transpose()
    }

    async fn get_kv_value(
        &self,
        instance: &str,
        key: &str,
    ) -> Result<Option<String>, ProviderError> {
        kv_store::get_value(&self.db, instance, key).await
    }

    async fn get_kv_all_values(
        &self,
        instance: &str,
    ) -> Result<HashMap<String, String>, ProviderError> {
        kv_store::get_all_values(&self.db, instance).await
    }

    async fn get_instance_stats(
        &self,
        instance: &str,
    ) -> Result<Option<SystemStats>, ProviderError> {
        let execution_id = latest_execution_id_on(&self.db, instance).await?;
        let Some(execution_id) = execution_id else {
            return Ok(None);
        };

        let history_row = query_one(
            &self.db,
            "SELECT COUNT(*) AS event_count, COALESCE(SUM(LENGTH(event_data)), 0) AS history_size
             FROM duroxide_history
             WHERE instance_id = ? AND execution_id = ?",
            vec![v_str(instance), v_u64(execution_id)],
            "get_instance_stats",
        )
        .await?
        .ok_or_else(|| ProviderError::permanent("get_instance_stats", "Missing aggregate row"))?;
        let (kv_user_key_count, kv_total_value_bytes) = kv_store::stats(&self.db, instance).await?;
        let carry_forward_row = query_one(
            &self.db,
            "SELECT event_data
             FROM duroxide_history
             WHERE instance_id = ? AND execution_id = ? AND event_id = ?",
            vec![
                v_str(instance),
                v_u64(execution_id),
                v_u64(INITIAL_EVENT_ID),
            ],
            "get_instance_stats",
        )
        .await?;

        let history_event_count: i64 = get(&history_row, "event_count", "get_instance_stats")?;
        let history_size_bytes: i64 = get(&history_row, "history_size", "get_instance_stats")?;
        let queue_pending_count = carry_forward_row
            .map(|row| {
                let event_data: String = get(&row, "event_data", "get_instance_stats")?;
                let event: Event = serde_json::from_str(&event_data).map_err(|e| {
                    ProviderError::permanent(
                        "get_instance_stats",
                        format!("Failed to deserialize OrchestrationStarted event: {e}"),
                    )
                })?;
                Ok::<u64, ProviderError>(match event.kind {
                    EventKind::OrchestrationStarted {
                        carry_forward_events: Some(events),
                        ..
                    } => events.len() as u64,
                    _ => 0,
                })
            })
            .transpose()?
            .unwrap_or(0);

        Ok(Some(SystemStats {
            history_event_count: history_event_count as u64,
            history_size_bytes: history_size_bytes as u64,
            queue_pending_count,
            kv_user_key_count,
            kv_total_value_bytes,
        }))
    }
}

async fn is_capability_compatible<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
    filter: Option<&DispatcherCapabilityFilter>,
) -> Result<bool, ProviderError> {
    let Some(filter) = filter else {
        return Ok(true);
    };
    if filter.supported_duroxide_versions.is_empty() {
        return Ok(false);
    }

    let row = query_one(
        conn,
        "SELECT e.duroxide_version_major, e.duroxide_version_minor, e.duroxide_version_patch
         FROM duroxide_instances i
         JOIN duroxide_executions e ON i.instance_id = e.instance_id AND i.current_execution_id = e.execution_id
         WHERE i.instance_id = ?",
        vec![v_str(instance_id)],
        "fetch_orchestration_item",
    )
    .await?;

    let Some(row) = row else {
        return Ok(true);
    };

    let major: Option<i64> = get_opt(&row, "duroxide_version_major", "fetch_orchestration_item")?;
    let minor: Option<i64> = get_opt(&row, "duroxide_version_minor", "fetch_orchestration_item")?;
    let patch: Option<i64> = get_opt(&row, "duroxide_version_patch", "fetch_orchestration_item")?;
    let (Some(major), Some(minor), Some(patch)) = (major, minor, patch) else {
        return Ok(true);
    };

    let version = semver::Version::new(major as u64, minor as u64, patch as u64);
    Ok(filter.is_compatible(&version))
}

async fn ensure_instance_and_execution_on<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
    execution_id: u64,
    metadata: &ExecutionMetadata,
) -> Result<(), ProviderError> {
    let now = now_ms();
    let name = metadata
        .orchestration_name
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    let version = metadata.orchestration_version.clone();

    execute(
        conn,
        "INSERT INTO duroxide_instances
         (instance_id, orchestration_name, orchestration_version, current_execution_id, parent_instance_id, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(instance_id) DO NOTHING",
        vec![
            v_str(instance_id),
            v_str(&name),
            v_opt_str(version.as_deref()),
            v_u64(execution_id),
            v_opt_str(metadata.parent_instance_id.as_deref()),
            v_i64(now),
            v_i64(now),
        ],
        "ack_orchestration_item",
    )
    .await?;

    if metadata.orchestration_name.is_some() || metadata.orchestration_version.is_some() {
        execute(
            conn,
            "UPDATE duroxide_instances
             SET orchestration_name = COALESCE(?, orchestration_name),
                 orchestration_version = COALESCE(?, orchestration_version),
                 updated_at = ?
             WHERE instance_id = ?",
            vec![
                v_opt_str(metadata.orchestration_name.as_deref()),
                v_opt_str(metadata.orchestration_version.as_deref()),
                v_i64(now),
                v_str(instance_id),
            ],
            "ack_orchestration_item",
        )
        .await?;
    }

    execute(
        conn,
        "UPDATE duroxide_instances
         SET current_execution_id = CASE WHEN current_execution_id < ? THEN ? ELSE current_execution_id END,
             updated_at = ?
         WHERE instance_id = ?",
        vec![
            v_u64(execution_id),
            v_u64(execution_id),
            v_i64(now),
            v_str(instance_id),
        ],
        "ack_orchestration_item",
    )
    .await?;

    execute(
        conn,
        "INSERT INTO duroxide_executions (instance_id, execution_id, status, started_at)
         VALUES (?, ?, 'Running', ?)
         ON CONFLICT(instance_id, execution_id) DO NOTHING",
        vec![v_str(instance_id), v_u64(execution_id), v_i64(now)],
        "ack_orchestration_item",
    )
    .await?;

    if let Some(pinned) = &metadata.pinned_duroxide_version {
        execute(
            conn,
            "UPDATE duroxide_executions
             SET duroxide_version_major = ?, duroxide_version_minor = ?, duroxide_version_patch = ?
             WHERE instance_id = ? AND execution_id = ?",
            vec![
                v_u64(pinned.major),
                v_u64(pinned.minor),
                v_u64(pinned.patch),
                v_str(instance_id),
                v_u64(execution_id),
            ],
            "ack_orchestration_item",
        )
        .await?;
    }

    Ok(())
}

async fn apply_metadata_on<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
    execution_id: u64,
    metadata: &ExecutionMetadata,
) -> Result<(), ProviderError> {
    if let Some(status) = &metadata.status {
        execute(
            conn,
            "UPDATE duroxide_executions
             SET status = ?, output = ?, completed_at = ?
             WHERE instance_id = ? AND execution_id = ?",
            vec![
                v_str(status),
                v_opt_str(metadata.output.as_deref()),
                v_i64(now_ms()),
                v_str(instance_id),
                v_u64(execution_id),
            ],
            "ack_orchestration_item",
        )
        .await?;
    }
    Ok(())
}

async fn apply_custom_status_on<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
    history_delta: &[Event],
) -> Result<(), ProviderError> {
    let status = history_delta
        .iter()
        .rev()
        .find_map(|event| match &event.kind {
            EventKind::CustomStatusUpdated { status } => Some(status.clone()),
            _ => None,
        });

    if let Some(status) = status {
        execute(
            conn,
            "UPDATE duroxide_instances
             SET custom_status = ?, custom_status_version = custom_status_version + 1, updated_at = ?
             WHERE instance_id = ?",
            vec![v_opt_str(status.as_deref()), v_i64(now_ms()), v_str(instance_id)],
            "ack_orchestration_item",
        )
        .await?;
    }

    Ok(())
}

async fn enqueue_worker_on<C: ConnectionTrait>(
    conn: &C,
    item: WorkItem,
) -> Result<(), ProviderError> {
    let work_item = serde_json::to_string(&item).map_err(|e| {
        ProviderError::permanent("enqueue_for_worker", format!("Serialization error: {e}"))
    })?;
    let (instance_id, execution_id, activity_id, session_id, tag) = match &item {
        WorkItem::ActivityExecute {
            instance,
            execution_id,
            id,
            session_id,
            tag,
            ..
        } => (
            Some(instance.as_str()),
            Some(*execution_id),
            Some(*id),
            session_id.as_deref(),
            tag.as_deref(),
        ),
        _ => (None, None, None, None, None),
    };

    execute(
        conn,
        "INSERT INTO duroxide_worker_queue
         (work_item, visible_at, created_at, instance_id, execution_id, activity_id, session_id, tag)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        vec![
            v_str(work_item),
            v_i64(now_ms()),
            v_i64(now_ms()),
            v_opt_str(instance_id),
            v_opt_u64(execution_id),
            v_opt_u64(activity_id),
            v_opt_str(session_id),
            v_opt_str(tag),
        ],
        "enqueue_for_worker",
    )
    .await?;
    Ok(())
}

async fn enqueue_orchestrator_on<C: ConnectionTrait>(
    conn: &C,
    item: WorkItem,
    delay: Option<Duration>,
) -> Result<(), ProviderError> {
    let instance = work_item_instance(&item).ok_or_else(|| {
        ProviderError::permanent(
            "enqueue_for_orchestrator",
            "Invalid orchestrator work item type",
        )
    })?;
    let work_item = serde_json::to_string(&item).map_err(|e| {
        ProviderError::permanent(
            "enqueue_for_orchestrator",
            format!("Serialization error: {e}"),
        )
    })?;
    let visible_at = if let Some(delay) = delay {
        now_ms().saturating_add(duration_ms(delay))
    } else if let WorkItem::TimerFired { fire_at_ms, .. } = &item {
        (*fire_at_ms).min(i64::MAX as u64) as i64
    } else {
        now_ms()
    };

    execute(
        conn,
        "INSERT INTO duroxide_orchestrator_queue (instance_id, work_item, visible_at, created_at)
         VALUES (?, ?, ?, ?)",
        vec![
            v_str(instance),
            v_str(work_item),
            v_i64(visible_at),
            v_i64(now_ms()),
        ],
        "enqueue_for_orchestrator",
    )
    .await?;
    Ok(())
}

async fn append_history_on<C: ConnectionTrait>(
    conn: &C,
    instance: &str,
    execution_id: u64,
    events: &[Event],
) -> Result<(), ProviderError> {
    for event in events {
        let event_data = serde_json::to_string(event).map_err(|e| {
            ProviderError::permanent("append_with_execution", format!("Serialization error: {e}"))
        })?;
        execute(
            conn,
            "INSERT INTO duroxide_history
             (instance_id, execution_id, event_id, event_type, event_data, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
            vec![
                v_str(instance),
                v_u64(execution_id),
                v_u64(event.event_id()),
                v_str(event_type(event)),
                v_str(event_data),
                v_i64(now_ms()),
            ],
            "append_with_execution",
        )
        .await?;
    }
    Ok(())
}

async fn read_history_on<C: ConnectionTrait>(
    conn: &C,
    instance: &str,
    execution_id: u64,
) -> Result<Vec<Event>, ProviderError> {
    let rows = query_all(
        conn,
        "SELECT event_data FROM duroxide_history
         WHERE instance_id = ? AND execution_id = ?
         ORDER BY event_id",
        vec![v_str(instance), v_u64(execution_id)],
        "read_with_execution",
    )
    .await?;
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let event_data: String = get(&row, "event_data", "read_with_execution")?;
        let event = serde_json::from_str(&event_data).map_err(|e| {
            ProviderError::permanent(
                "read_with_execution",
                format!("Failed to deserialize event: {e}"),
            )
        })?;
        events.push(event);
    }
    Ok(events)
}

async fn latest_execution_id_on<C: ConnectionTrait>(
    conn: &C,
    instance: &str,
) -> Result<Option<u64>, ProviderError> {
    let row = query_one(
        conn,
        "SELECT MAX(execution_id) AS execution_id FROM duroxide_executions WHERE instance_id = ?",
        vec![v_str(instance)],
        "latest_execution_id",
    )
    .await?;
    row.map(|row| {
        let value: Option<i64> = get_opt(&row, "execution_id", "latest_execution_id")?;
        Ok(value.map(|v| v as u64))
    })
    .transpose()
    .map(Option::flatten)
}

async fn claim_session_if_eligible<C: ConnectionTrait>(
    conn: &C,
    config: Option<&SessionFetchConfig>,
    session_id: Option<&str>,
    now: i64,
) -> Result<bool, ProviderError> {
    let Some(session_id) = session_id else {
        return Ok(true);
    };
    let Some(config) = config else {
        return Ok(false);
    };

    let row = query_one(
        conn,
        "SELECT worker_id, locked_until FROM duroxide_sessions WHERE session_id = ?",
        vec![v_str(session_id)],
        "fetch_work_item",
    )
    .await?;

    if let Some(row) = row {
        let worker_id: String = get(&row, "worker_id", "fetch_work_item")?;
        let locked_until: i64 = get(&row, "locked_until", "fetch_work_item")?;
        if locked_until > now && worker_id != config.owner_id {
            return Ok(false);
        }
    }

    execute(
        conn,
        "INSERT INTO duroxide_sessions (session_id, worker_id, locked_until, last_activity_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(session_id) DO UPDATE
         SET worker_id = excluded.worker_id,
             locked_until = excluded.locked_until,
             last_activity_at = excluded.last_activity_at
         WHERE duroxide_sessions.locked_until <= ? OR duroxide_sessions.worker_id = ?",
        vec![
            v_str(session_id),
            v_str(&config.owner_id),
            v_i64(now.saturating_add(duration_ms(config.lock_timeout))),
            v_i64(now),
            v_i64(now),
            v_str(&config.owner_id),
        ],
        "fetch_work_item",
    )
    .await
    .map(|rows| rows > 0)
}

async fn touch_session_on<C: ConnectionTrait>(
    conn: &C,
    session_id: &str,
    now: i64,
) -> Result<(), ProviderError> {
    execute(
        conn,
        "UPDATE duroxide_sessions SET last_activity_at = ? WHERE session_id = ?",
        vec![v_i64(now), v_str(session_id)],
        "session_touch",
    )
    .await?;
    Ok(())
}

fn start_metadata_from_messages(messages: &[WorkItem]) -> Option<(String, Option<String>, u64)> {
    messages.iter().find_map(|item| match item {
        WorkItem::StartOrchestration {
            orchestration,
            version,
            execution_id,
            ..
        } => Some((orchestration.clone(), version.clone(), *execution_id)),
        WorkItem::ContinueAsNew {
            orchestration,
            version,
            ..
        } => Some((
            orchestration.clone(),
            version.clone(),
            duroxide::INITIAL_EXECUTION_ID,
        )),
        _ => None,
    })
}

fn work_item_instance(item: &WorkItem) -> Option<&str> {
    match item {
        WorkItem::StartOrchestration { instance, .. }
        | WorkItem::ActivityCompleted { instance, .. }
        | WorkItem::ActivityFailed { instance, .. }
        | WorkItem::TimerFired { instance, .. }
        | WorkItem::ExternalRaised { instance, .. }
        | WorkItem::QueueMessage { instance, .. }
        | WorkItem::CancelInstance { instance, .. }
        | WorkItem::ContinueAsNew { instance, .. } => Some(instance),
        #[cfg(feature = "replay-version-test")]
        WorkItem::ExternalRaised2 { instance, .. } => Some(instance),
        WorkItem::SubOrchCompleted {
            parent_instance, ..
        }
        | WorkItem::SubOrchFailed {
            parent_instance, ..
        } => Some(parent_instance),
        WorkItem::ActivityExecute { .. } => None,
    }
}

pub(crate) async fn query_one<C: ConnectionTrait>(
    conn: &C,
    sql: &str,
    values: Vec<Value>,
    operation: &'static str,
) -> Result<Option<QueryResult>, ProviderError> {
    conn.query_one(statement(conn.get_database_backend(), sql, values))
        .await
        .map_err(|e| db_error(operation, e))
}

pub(crate) async fn query_all<C: ConnectionTrait>(
    conn: &C,
    sql: &str,
    values: Vec<Value>,
    operation: &'static str,
) -> Result<Vec<QueryResult>, ProviderError> {
    conn.query_all(statement(conn.get_database_backend(), sql, values))
        .await
        .map_err(|e| db_error(operation, e))
}

pub(crate) async fn execute<C: ConnectionTrait>(
    conn: &C,
    sql: &str,
    values: Vec<Value>,
    operation: &'static str,
) -> Result<u64, ProviderError> {
    conn.execute(statement(conn.get_database_backend(), sql, values))
        .await
        .map(|result| result.rows_affected())
        .map_err(|e| db_error(operation, e))
}

fn statement(backend: DatabaseBackend, sql: &str, values: Vec<Value>) -> Statement {
    Statement::from_sql_and_values(backend, normalize_placeholders(backend, sql), values)
}

fn normalize_placeholders(backend: DatabaseBackend, sql: &str) -> String {
    if backend != DatabaseBackend::Postgres {
        return sql.to_string();
    }

    let mut index = 1usize;
    let mut normalized = String::with_capacity(sql.len() + 8);
    for ch in sql.chars() {
        if ch == '?' {
            normalized.push('$');
            normalized.push_str(&index.to_string());
            index += 1;
        } else {
            normalized.push(ch);
        }
    }
    normalized
}

fn placeholders(backend: DatabaseBackend, start_index: usize, count: usize) -> String {
    (0..count)
        .map(|offset| match backend {
            DatabaseBackend::Postgres => format!("${}", start_index + offset),
            _ => "?".to_string(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn get<T>(
    row: &QueryResult,
    column: &str,
    operation: &'static str,
) -> Result<T, ProviderError>
where
    T: sea_orm::TryGetable,
{
    row.try_get("", column).map_err(|e| {
        ProviderError::permanent(operation, format!("Failed to read column `{column}`: {e}"))
    })
}

pub(crate) fn get_opt<T>(
    row: &QueryResult,
    column: &str,
    operation: &'static str,
) -> Result<Option<T>, ProviderError>
where
    T: sea_orm::TryGetable,
{
    row.try_get("", column).map_err(|e| {
        ProviderError::permanent(operation, format!("Failed to read column `{column}`: {e}"))
    })
}

fn db_error(operation: &'static str, error: sea_orm::DbErr) -> ProviderError {
    let message = error.to_string();
    if message.contains("UNIQUE")
        || message.contains("duplicate key")
        || message.contains("PRIMARY KEY")
    {
        ProviderError::permanent(operation, message)
    } else if message.contains("database is locked")
        || message.contains("timeout")
        || message.contains("connection")
    {
        ProviderError::retryable(operation, message)
    } else {
        ProviderError::retryable(operation, message)
    }
}

fn event_type(event: &Event) -> &'static str {
    match &event.kind {
        EventKind::OrchestrationStarted { .. } => "OrchestrationStarted",
        EventKind::OrchestrationCompleted { .. } => "OrchestrationCompleted",
        EventKind::OrchestrationFailed { .. } => "OrchestrationFailed",
        EventKind::ActivityScheduled { .. } => "ActivityScheduled",
        EventKind::ActivityCompleted { .. } => "ActivityCompleted",
        EventKind::ActivityFailed { .. } => "ActivityFailed",
        EventKind::ActivityCancelRequested { .. } => "ActivityCancelRequested",
        EventKind::TimerCreated { .. } => "TimerCreated",
        EventKind::TimerFired { .. } => "TimerFired",
        EventKind::ExternalSubscribed { .. } => "ExternalSubscribed",
        EventKind::ExternalEvent { .. } => "ExternalEvent",
        EventKind::OrchestrationChained { .. } => "OrchestrationChained",
        EventKind::SubOrchestrationScheduled { .. } => "SubOrchestrationScheduled",
        EventKind::SubOrchestrationCompleted { .. } => "SubOrchestrationCompleted",
        EventKind::SubOrchestrationFailed { .. } => "SubOrchestrationFailed",
        EventKind::SubOrchestrationCancelRequested { .. } => "SubOrchestrationCancelRequested",
        EventKind::OrchestrationContinuedAsNew { .. } => "OrchestrationContinuedAsNew",
        EventKind::OrchestrationCancelRequested { .. } => "OrchestrationCancelRequested",
        EventKind::ExternalSubscribedCancelled { .. } => "ExternalSubscribedCancelled",
        EventKind::QueueSubscribed { .. } => "QueueSubscribed",
        EventKind::QueueEventDelivered { .. } => "QueueEventDelivered",
        EventKind::QueueSubscriptionCancelled { .. } => "QueueSubscriptionCancelled",
        EventKind::CustomStatusUpdated { .. } => "CustomStatusUpdated",
        #[cfg(feature = "replay-version-test")]
        EventKind::ExternalSubscribed2 { .. } => "ExternalSubscribed2",
        #[cfg(feature = "replay-version-test")]
        EventKind::ExternalEvent2 { .. } => "ExternalEvent2",
        EventKind::KeyValueSet { .. } => "KeyValueSet",
        EventKind::KeyValueCleared { .. } => "KeyValueCleared",
        EventKind::KeyValuesCleared => "KeyValuesCleared",
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn duration_ms(duration: Duration) -> i64 {
    duration.as_millis().min(i64::MAX as u128) as i64
}

pub(crate) fn v_str(value: impl AsRef<str>) -> Value {
    v_owned_str(value.as_ref().to_string())
}

pub(crate) fn v_owned_str(value: String) -> Value {
    Value::String(Some(Box::new(value)))
}

pub(crate) fn v_opt_str(value: Option<&str>) -> Value {
    Value::String(value.map(|value| Box::new(value.to_string())))
}

pub(crate) fn v_i64(value: i64) -> Value {
    Value::BigInt(Some(value))
}

pub(crate) fn v_u64(value: u64) -> Value {
    Value::BigInt(Some(value.min(i64::MAX as u64) as i64))
}

fn v_opt_u64(value: Option<u64>) -> Value {
    Value::BigInt(value.map(|value| value.min(i64::MAX as u64) as i64))
}
