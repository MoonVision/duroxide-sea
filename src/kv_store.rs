use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use duroxide::providers::{KvEntry, ProviderError};
use duroxide::{Event, EventKind};
use sea_orm::ConnectionTrait;

use crate::provider::{
    execute, get, get_opt, query_all, query_one, v_i64, v_opt_str, v_str, v_u64,
};

pub(crate) async fn apply_events<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
    history_delta: &[Event],
    execution_status: Option<&str>,
) -> Result<(), ProviderError> {
    for event in history_delta {
        match &event.kind {
            EventKind::KeyValueSet {
                key,
                value,
                last_updated_at_ms,
            } => set_delta(conn, instance_id, key, value, *last_updated_at_ms).await?,
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
    let mut snapshot = HashMap::new();
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
    let delta_row = query_one(
        conn,
        "SELECT value FROM duroxide_kv_delta WHERE instance_id = ? AND key = ?",
        vec![v_str(instance_id), v_str(key)],
        "get_kv_value",
    )
    .await?;
    if let Some(row) = delta_row {
        return get_opt(&row, "value", "get_kv_value");
    }

    let store_row = query_one(
        conn,
        "SELECT value FROM duroxide_kv_store WHERE instance_id = ? AND key = ?",
        vec![v_str(instance_id), v_str(key)],
        "get_kv_value",
    )
    .await?;
    store_row
        .map(|row| get(&row, "value", "get_kv_value"))
        .transpose()
}

pub(crate) async fn get_all_values<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
) -> Result<HashMap<String, String>, ProviderError> {
    let store_rows = query_all(
        conn,
        "SELECT key, value FROM duroxide_kv_store WHERE instance_id = ?",
        vec![v_str(instance_id)],
        "get_kv_all_values",
    )
    .await?;
    let mut values = HashMap::new();
    for row in store_rows {
        let key: String = get(&row, "key", "get_kv_all_values")?;
        let value: String = get(&row, "value", "get_kv_all_values")?;
        values.insert(key, value);
    }

    let delta_rows = query_all(
        conn,
        "SELECT key, value FROM duroxide_kv_delta WHERE instance_id = ?",
        vec![v_str(instance_id)],
        "get_kv_all_values",
    )
    .await?;
    for row in delta_rows {
        let key: String = get(&row, "key", "get_kv_all_values")?;
        let value: Option<String> = get_opt(&row, "value", "get_kv_all_values")?;
        if let Some(value) = value {
            values.insert(key, value);
        } else {
            values.remove(&key);
        }
    }

    Ok(values)
}

pub(crate) async fn stats<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
) -> Result<(u64, u64), ProviderError> {
    let values = get_all_values(conn, instance_id).await?;
    let count = values.len() as u64;
    let value_bytes = values.values().map(String::len).sum::<usize>() as u64;
    Ok((count, value_bytes))
}

async fn set_delta<C: ConnectionTrait>(
    conn: &C,
    instance_id: &str,
    key: &str,
    value: &str,
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
            v_str(key),
            v_str(value),
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
    key: &str,
) -> Result<(), ProviderError> {
    execute(
        conn,
        "INSERT INTO duroxide_kv_delta (instance_id, key, value, last_updated_at_ms)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(instance_id, key) DO UPDATE
         SET value = excluded.value, last_updated_at_ms = excluded.last_updated_at_ms",
        vec![
            v_str(instance_id),
            v_str(key),
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
