use std::sync::Arc;

use async_trait::async_trait;
use duroxide::providers::{OrchestrationItem, ProviderError};
use sea_orm::DatabaseTransaction;

/// Prepare application data before the runtime enters synchronous replay.
#[async_trait]
pub trait OrchestrationPreparation: Send + Sync {
    async fn prepare(
        &self,
        item: &OrchestrationItem,
    ) -> Result<Arc<dyn PreparedOrchestration>, ProviderError>;
}

/// Retained for a fetched turn; commits application writes in its acknowledgement transaction.
#[async_trait]
pub trait PreparedOrchestration: Send + Sync {
    async fn commit(&self, transaction: &DatabaseTransaction) -> Result<(), ProviderError>;
}
