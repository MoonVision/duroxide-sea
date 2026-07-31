use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use duroxide::providers::{KvEntry, ProviderError};
use duroxide::{Event, EventKind};
use sea_orm::{ConnectionTrait, DatabaseBackend};

use crate::provider::{
    execute, get, get_opt, query_all, query_one, v_i64, v_opt_str, v_owned_str, v_str, v_u64,
};

pub(crate) async fn apply_events<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
    history_delta: Vec<Event>,
    execution_status: Option<&str>,
) -> Result<(), ProviderError> {
    for event in history_delta {
        match event.kind {
            EventKind::KeyValueSet {
                key,
                value,
                last_updated_at_ms,
            } => set_delta(conn, instance_id, key, value, last_updated_at_ms).await?,
            EventKind::KeyValueCleared { key } => {
                clear_delta_key(conn, instance_id, key).await?;
            }
            EventKind::KeyValuesCleared => clear_all_delta(conn, instance_id).await?,
            _ => {}
        }
    }

    if execution_status.is_some_and(is_terminal_status) {
        merge_delta(conn, instance_id).await?;
    }

    Ok(())
}

pub(crate) async fn snapshot<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
) -> Result<HashMap<String, KvEntry>, ProviderError> {
    let rows = query_all(
        conn,
        "SELECT key, value, last_updated_at_ms FROM duroxide_kv_store WHERE instance_id = ?",
        vec![v_str(instance_id)],
        "fetch_orchestration_item",
    )
    .await?;
    let mut snapshot = HashMap::with_capacity(rows.len());
    for row in rows {
        let key: String = get(&row, "key", "fetch_orchestration_item")?;
        let value: String = get(&row, "value", "fetch_orchestration_item")?;
        let last_updated_at_ms: i64 = get(&row, "last_updated_at_ms", "fetch_orchestration_item")?;
        snapshot.insert(
            key,
            KvEntry {
                value,
                last_updated_at_ms: last_updated_at_ms as u64,
            },
        );
    }
    Ok(snapshot)
}

pub(crate) async fn get_value<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
    key: &str,
) -> Result<Option<String>, ProviderError> {
    let row = query_one(
        conn,
        "WITH input(instance_id, key) AS (SELECT ?, ?)
         SELECT CASE
                    WHEN delta.key IS NOT NULL THEN delta.value
                    ELSE store.value
                END AS value
         FROM input
         LEFT JOIN duroxide_kv_delta AS delta
           ON delta.instance_id = input.instance_id AND delta.key = input.key
         LEFT JOIN duroxide_kv_store AS store
           ON store.instance_id = input.instance_id AND store.key = input.key",
        vec![v_str(instance_id), v_str(key)],
        "get_kv_value",
    )
    .await?
    .ok_or_else(|| ProviderError::permanent("get_kv_value", "Missing merged value row"))?;
    get_opt(&row, "value", "get_kv_value")
}

pub(crate) async fn get_all_values<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
) -> Result<HashMap<String, String>, ProviderError> {
    let rows = query_all(
        conn,
        "WITH input(instance_id) AS (SELECT ?)
         SELECT store.key, store.value
         FROM duroxide_kv_store AS store
         JOIN input ON input.instance_id = store.instance_id
         WHERE NOT EXISTS (
             SELECT 1
             FROM duroxide_kv_delta AS delta
             WHERE delta.instance_id = store.instance_id AND delta.key = store.key
         )
         UNION ALL
         SELECT delta.key, delta.value
         FROM duroxide_kv_delta AS delta
         JOIN input ON input.instance_id = delta.instance_id
         WHERE delta.value IS NOT NULL",
        vec![v_str(instance_id)],
        "get_kv_all_values",
    )
    .await?;
    let mut values = HashMap::with_capacity(rows.len());
    for row in rows {
        let key: String = get(&row, "key", "get_kv_all_values")?;
        let value: String = get(&row, "value", "get_kv_all_values")?;
        values.insert(key, value);
    }
    Ok(values)
}

pub(crate) async fn stats<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
) -> Result<(u64, u64), ProviderError> {
    let value_length = match conn.get_database_backend() {
        DatabaseBackend::Sqlite => "LENGTH(CAST(value AS BLOB))",
        DatabaseBackend::Postgres | DatabaseBackend::MySql => "OCTET_LENGTH(value)",
    };
    let sql = format!(
        "WITH input(instance_id) AS (SELECT ?),
         effective_values(value) AS (
             SELECT store.value
             FROM duroxide_kv_store AS store
             JOIN input ON input.instance_id = store.instance_id
             WHERE NOT EXISTS (
                 SELECT 1
                 FROM duroxide_kv_delta AS delta
                 WHERE delta.instance_id = store.instance_id AND delta.key = store.key
             )
             UNION ALL
             SELECT delta.value
             FROM duroxide_kv_delta AS delta
             JOIN input ON input.instance_id = delta.instance_id
             WHERE delta.value IS NOT NULL
         )
         SELECT COUNT(*) AS key_count,
                COALESCE(SUM({value_length}), 0) AS value_bytes
         FROM effective_values"
    );
    let row = query_one(conn, &sql, vec![v_str(instance_id)], "get_instance_stats")
        .await?
        .ok_or_else(|| {
            ProviderError::permanent("get_instance_stats", "Missing KV aggregate row")
        })?;
    let count: i64 = get(&row, "key_count", "get_instance_stats")?;
    let value_bytes: i64 = get(&row, "value_bytes", "get_instance_stats")?;
    Ok((count as u64, value_bytes as u64))
}

async fn set_delta<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
    key: String,
    value: String,
    last_updated_at_ms: u64,
) -> Result<(), ProviderError> {
    execute(
        conn,
        "INSERT INTO duroxide_kv_delta (instance_id, key, value, last_updated_at_ms)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(instance_id, key) DO UPDATE
         SET value = excluded.value, last_updated_at_ms = excluded.last_updated_at_ms",
        vec![
            v_str(instance_id),
            v_owned_str(key),
            v_owned_str(value),
            v_u64(last_updated_at_ms),
        ],
        "ack_orchestration_item",
    )
    .await?;
    Ok(())
}

async fn clear_delta_key<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
    key: String,
) -> Result<(), ProviderError> {
    execute(
        conn,
        "INSERT INTO duroxide_kv_delta (instance_id, key, value, last_updated_at_ms)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(instance_id, key) DO UPDATE
         SET value = excluded.value, last_updated_at_ms = excluded.last_updated_at_ms",
        vec![
            v_str(instance_id),
            v_owned_str(key),
            v_opt_str(None),
            v_i64(now_ms()),
        ],
        "ack_orchestration_item",
    )
    .await?;
    Ok(())
}

async fn clear_all_delta<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
) -> Result<(), ProviderError> {
    let timestamp = now_ms();
    execute(
        conn,
        "UPDATE duroxide_kv_delta
         SET value = NULL, last_updated_at_ms = ?
         WHERE instance_id = ?",
        vec![v_i64(timestamp), v_str(instance_id)],
        "ack_orchestration_item",
    )
    .await?;
    execute(
        conn,
        "INSERT INTO duroxide_kv_delta (instance_id, key, value, last_updated_at_ms)
         SELECT instance_id, key, NULL, ? FROM duroxide_kv_store WHERE instance_id = ?
         ON CONFLICT(instance_id, key) DO NOTHING",
        vec![v_i64(timestamp), v_str(instance_id)],
        "ack_orchestration_item",
    )
    .await?;
    Ok(())
}

async fn merge_delta<C: ConnectionTrait>(conn: &C, instance_id: &str) -> Result<(), ProviderError> {
    execute(
        conn,
        "INSERT INTO duroxide_kv_store (instance_id, key, value, last_updated_at_ms)
         SELECT instance_id, key, value, last_updated_at_ms
         FROM duroxide_kv_delta
         WHERE instance_id = ? AND value IS NOT NULL
         ON CONFLICT(instance_id, key) DO UPDATE
         SET value = excluded.value, last_updated_at_ms = excluded.last_updated_at_ms",
        vec![v_str(instance_id)],
        "ack_orchestration_item",
    )
    .await?;
    execute(
        conn,
        "DELETE FROM duroxide_kv_store
         WHERE instance_id = ?
           AND key IN (
               SELECT key FROM duroxide_kv_delta
               WHERE instance_id = ? AND value IS NULL
           )",
        vec![v_str(instance_id), v_str(instance_id)],
        "ack_orchestration_item",
    )
    .await?;
    execute(
        conn,
        "DELETE FROM duroxide_kv_delta WHERE instance_id = ?",
        vec![v_str(instance_id)],
        "ack_orchestration_item",
    )
    .await?;
    Ok(())
}

fn is_terminal_status(status: &str) -> bool {
    matches!(status, "Completed" | "ContinuedAsNew" | "Failed")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
