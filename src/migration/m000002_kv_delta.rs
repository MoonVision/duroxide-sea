use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        let backend = db.get_database_backend();

        for sql in up_sql(backend) {
            db.execute(Statement::from_string(backend, sql)).await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        let backend = db.get_database_backend();

        for sql in down_sql(backend) {
            db.execute(Statement::from_string(backend, sql)).await?;
        }

        Ok(())
    }
}

fn up_sql(backend: DatabaseBackend) -> Vec<String> {
    match backend {
        DatabaseBackend::Sqlite => vec![
            r#"
            CREATE TABLE IF NOT EXISTS duroxide_kv_delta (
                instance_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                last_updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (instance_id, key)
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS duroxide_kv_store_new (
                instance_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                last_updated_at_ms INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (instance_id, key)
            )
            "#,
            r#"
            INSERT OR IGNORE INTO duroxide_kv_store_new (instance_id, key, value, last_updated_at_ms)
            SELECT instance_id, key, value, last_updated_at_ms FROM duroxide_kv_store
            "#,
            "DROP TABLE duroxide_kv_store",
            "ALTER TABLE duroxide_kv_store_new RENAME TO duroxide_kv_store",
        ],
        DatabaseBackend::Postgres | DatabaseBackend::MySql => vec![
            r#"
            CREATE TABLE IF NOT EXISTS duroxide_kv_delta (
                instance_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                last_updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (instance_id, key)
            )
            "#,
            "ALTER TABLE duroxide_kv_store DROP COLUMN execution_id",
        ],
    }
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn down_sql(backend: DatabaseBackend) -> Vec<String> {
    let execution_id_type = match backend {
        DatabaseBackend::Sqlite => "INTEGER",
        DatabaseBackend::Postgres | DatabaseBackend::MySql => "BIGINT",
    };

    vec![
        format!(
            "ALTER TABLE duroxide_kv_store ADD COLUMN execution_id {execution_id_type} NOT NULL DEFAULT 1"
        ),
        "DROP TABLE IF EXISTS duroxide_kv_delta".to_string(),
    ]
}
