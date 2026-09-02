use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        let backend = db.get_database_backend();

        for sql in create_sql(backend)? {
            db.execute_raw(Statement::from_string(backend, sql)).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        let backend = db.get_database_backend();

        for table in [
            "duroxide_kv_store",
            "duroxide_sessions",
            "duroxide_instance_locks",
            "duroxide_worker_queue",
            "duroxide_orchestrator_queue",
            "duroxide_history",
            "duroxide_executions",
            "duroxide_instances",
        ] {
            db.execute_raw(Statement::from_string(
                backend,
                format!("DROP TABLE IF EXISTS {table}"),
            ))
            .await?;
        }

        Ok(())
    }
}

fn create_sql(backend: DatabaseBackend) -> Result<Vec<String>, DbErr> {
    Ok(match backend {
        DatabaseBackend::Sqlite => sqlite_create_sql(),
        DatabaseBackend::Postgres => postgres_create_sql(),
        _ => return Err(DbErr::Custom("unsupported database backend".to_owned())),
    })
}

fn sqlite_create_sql() -> Vec<String> {
    vec![
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_instances (
            instance_id TEXT PRIMARY KEY,
            orchestration_name TEXT NOT NULL,
            orchestration_version TEXT,
            current_execution_id INTEGER NOT NULL DEFAULT 1,
            custom_status TEXT,
            custom_status_version INTEGER NOT NULL DEFAULT 0,
            parent_instance_id TEXT,
            created_at INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL DEFAULT 0
        )
        "#,
        "CREATE INDEX IF NOT EXISTS idx_duroxide_instances_parent ON duroxide_instances(parent_instance_id)",
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_executions (
            instance_id TEXT NOT NULL,
            execution_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'Running',
            output TEXT,
            duroxide_version_major INTEGER,
            duroxide_version_minor INTEGER,
            duroxide_version_patch INTEGER,
            started_at INTEGER NOT NULL DEFAULT 0,
            completed_at INTEGER,
            PRIMARY KEY (instance_id, execution_id)
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_history (
            instance_id TEXT NOT NULL,
            execution_id INTEGER NOT NULL,
            event_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            event_data TEXT NOT NULL,
            created_at INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (instance_id, execution_id, event_id)
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_orchestrator_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instance_id TEXT NOT NULL,
            work_item TEXT NOT NULL,
            visible_at INTEGER NOT NULL DEFAULT 0,
            lock_token TEXT,
            locked_until INTEGER,
            attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
            created_at INTEGER NOT NULL DEFAULT 0
        )
        "#,
        "CREATE INDEX IF NOT EXISTS idx_duroxide_orch_visible ON duroxide_orchestrator_queue(visible_at, lock_token)",
        "CREATE INDEX IF NOT EXISTS idx_duroxide_orch_instance ON duroxide_orchestrator_queue(instance_id)",
        "CREATE INDEX IF NOT EXISTS idx_duroxide_orch_lock ON duroxide_orchestrator_queue(lock_token)",
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_worker_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            work_item TEXT NOT NULL,
            visible_at INTEGER NOT NULL DEFAULT 0,
            lock_token TEXT,
            locked_until INTEGER,
            attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
            created_at INTEGER NOT NULL DEFAULT 0,
            instance_id TEXT,
            execution_id INTEGER,
            activity_id INTEGER,
            session_id TEXT,
            tag TEXT
        )
        "#,
        "CREATE INDEX IF NOT EXISTS idx_duroxide_worker_available ON duroxide_worker_queue(visible_at, lock_token, id)",
        "CREATE INDEX IF NOT EXISTS idx_duroxide_worker_identity ON duroxide_worker_queue(instance_id, execution_id, activity_id)",
        "CREATE INDEX IF NOT EXISTS idx_duroxide_worker_session ON duroxide_worker_queue(session_id)",
        "CREATE INDEX IF NOT EXISTS idx_duroxide_worker_tag ON duroxide_worker_queue(tag)",
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_instance_locks (
            instance_id TEXT PRIMARY KEY,
            lock_token TEXT NOT NULL,
            locked_until INTEGER NOT NULL,
            locked_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_sessions (
            session_id TEXT PRIMARY KEY,
            worker_id TEXT NOT NULL,
            locked_until INTEGER NOT NULL,
            last_activity_at INTEGER NOT NULL
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_kv_store (
            instance_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            execution_id INTEGER NOT NULL DEFAULT 1,
            last_updated_at_ms INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (instance_id, key)
        )
        "#,
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn postgres_create_sql() -> Vec<String> {
    vec![
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_instances (
            instance_id TEXT PRIMARY KEY,
            orchestration_name TEXT NOT NULL,
            orchestration_version TEXT,
            current_execution_id BIGINT NOT NULL DEFAULT 1,
            custom_status TEXT,
            custom_status_version BIGINT NOT NULL DEFAULT 0,
            parent_instance_id TEXT,
            created_at BIGINT NOT NULL DEFAULT 0,
            updated_at BIGINT NOT NULL DEFAULT 0
        )
        "#,
        "CREATE INDEX IF NOT EXISTS idx_duroxide_instances_parent ON duroxide_instances(parent_instance_id)",
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_executions (
            instance_id TEXT NOT NULL,
            execution_id BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'Running',
            output TEXT,
            duroxide_version_major BIGINT,
            duroxide_version_minor BIGINT,
            duroxide_version_patch BIGINT,
            started_at BIGINT NOT NULL DEFAULT 0,
            completed_at BIGINT,
            PRIMARY KEY (instance_id, execution_id)
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_history (
            instance_id TEXT NOT NULL,
            execution_id BIGINT NOT NULL,
            event_id BIGINT NOT NULL,
            event_type TEXT NOT NULL,
            event_data TEXT NOT NULL,
            created_at BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (instance_id, execution_id, event_id)
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_orchestrator_queue (
            id BIGSERIAL PRIMARY KEY,
            instance_id TEXT NOT NULL,
            work_item TEXT NOT NULL,
            visible_at BIGINT NOT NULL DEFAULT 0,
            lock_token TEXT,
            locked_until BIGINT,
            attempt_count BIGINT NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
            created_at BIGINT NOT NULL DEFAULT 0
        )
        "#,
        "CREATE INDEX IF NOT EXISTS idx_duroxide_orch_visible ON duroxide_orchestrator_queue(visible_at, lock_token)",
        "CREATE INDEX IF NOT EXISTS idx_duroxide_orch_instance ON duroxide_orchestrator_queue(instance_id)",
        "CREATE INDEX IF NOT EXISTS idx_duroxide_orch_lock ON duroxide_orchestrator_queue(lock_token)",
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_worker_queue (
            id BIGSERIAL PRIMARY KEY,
            work_item TEXT NOT NULL,
            visible_at BIGINT NOT NULL DEFAULT 0,
            lock_token TEXT,
            locked_until BIGINT,
            attempt_count BIGINT NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
            created_at BIGINT NOT NULL DEFAULT 0,
            instance_id TEXT,
            execution_id BIGINT,
            activity_id BIGINT,
            session_id TEXT,
            tag TEXT
        )
        "#,
        "CREATE INDEX IF NOT EXISTS idx_duroxide_worker_available ON duroxide_worker_queue(visible_at, lock_token, id)",
        "CREATE INDEX IF NOT EXISTS idx_duroxide_worker_identity ON duroxide_worker_queue(instance_id, execution_id, activity_id)",
        "CREATE INDEX IF NOT EXISTS idx_duroxide_worker_session ON duroxide_worker_queue(session_id)",
        "CREATE INDEX IF NOT EXISTS idx_duroxide_worker_tag ON duroxide_worker_queue(tag)",
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_instance_locks (
            instance_id TEXT PRIMARY KEY,
            lock_token TEXT NOT NULL,
            locked_until BIGINT NOT NULL,
            locked_at BIGINT NOT NULL
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_sessions (
            session_id TEXT PRIMARY KEY,
            worker_id TEXT NOT NULL,
            locked_until BIGINT NOT NULL,
            last_activity_at BIGINT NOT NULL
        )
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS duroxide_kv_store (
            instance_id TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            execution_id BIGINT NOT NULL DEFAULT 1,
            last_updated_at_ms BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (instance_id, key)
        )
        "#,
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}
