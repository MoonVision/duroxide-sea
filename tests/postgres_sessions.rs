use std::time::{Duration, SystemTime, UNIX_EPOCH};

use duroxide::providers::Provider;
use duroxide_sea::{DuroxideSeaProvider, migration::Migrator};
use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement};
use sea_orm_migration::MigratorTrait;

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn session_renewal_binds_owner_ids_after_all_timestamps() {
    let db = Database::connect(std::env::var("TEST_POSTGRES_URL").unwrap())
        .await
        .unwrap();
    Migrator::up(&db, None).await.unwrap();
    let provider = DuroxideSeaProvider::new(db.clone());
    let prefix = uuid::Uuid::now_v7();
    let first = format!("{prefix}-first");
    let second = format!("{prefix}-second");
    let other = format!("{prefix}-other");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let cases = [
        ("first", &first, now + 60_000, now, true),
        ("second", &second, now + 60_000, now, true),
        ("idle", &first, now + 60_000, now - 120_000, false),
        ("expired", &second, now - 1, now, false),
        ("other", &other, now + 60_000, now, false),
    ];
    for (name, worker, locked_until, last_activity_at, _) in cases {
        db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "INSERT INTO duroxide_sessions (session_id, worker_id, locked_until, last_activity_at) VALUES ($1, $2, $3, $4)",
            [format!("{prefix}-{name}").into(), worker.clone().into(), locked_until.into(), last_activity_at.into()],
        )).await.unwrap();
    }
    assert_eq!(
        provider
            .renew_session_lock(
                &[&first, &second],
                Duration::from_secs(120),
                Duration::from_secs(60),
            )
            .await
            .unwrap(),
        2
    );
    for (name, _, original, _, renewed) in cases {
        let row = db
            .query_one_raw(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT locked_until FROM duroxide_sessions WHERE session_id = $1",
                [format!("{prefix}-{name}").into()],
            ))
            .await
            .unwrap()
            .unwrap();
        let actual: i64 = row.try_get("", "locked_until").unwrap();
        if renewed {
            assert!(actual > original);
        } else {
            assert_eq!(actual, original);
        }
        db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "DELETE FROM duroxide_sessions WHERE session_id = $1",
            [format!("{prefix}-{name}").into()],
        ))
        .await
        .unwrap();
    }
}
