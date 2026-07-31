use std::sync::Arc;

use duroxide_sea::{DuroxideSeaProvider, migration::Migrator};
use sea_orm::{ConnectOptions, Database, DatabaseConnection};
use sea_orm_migration::MigratorTrait;

pub async fn sqlite_database() -> DatabaseConnection {
    let database_url = format!(
        "sqlite:file:duroxide-sea-{}?mode=memory&cache=shared",
        uuid::Uuid::now_v7()
    );
    let mut options = ConnectOptions::new(database_url);
    options.max_connections(5).sqlx_logging(false);
    let db = Database::connect(options).await.unwrap();
    Migrator::up(&db, None).await.unwrap();
    db
}

pub async fn sqlite_provider() -> Arc<DuroxideSeaProvider> {
    Arc::new(DuroxideSeaProvider::new(sqlite_database().await))
}
