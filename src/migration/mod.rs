use sea_orm_migration::prelude::*;

mod m000001_duroxide_tables;
mod m000002_kv_delta;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m000001_duroxide_tables::Migration),
            Box::new(m000002_kv_delta::Migration),
        ]
    }
}
