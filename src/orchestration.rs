use std::sync::Arc;

use async_trait::async_trait;
use duroxide::providers::{OrchestrationItem, ProviderError};

/// Load application data before synchronous replay. The returned value is retained
/// until acknowledgement, abandonment, or lease expiry observed by a subsequent fetch.
#[async_trait]
pub trait OrchestrationPreparation: Send + Sync {
    async fn prepare(
        &self,
        item: &OrchestrationItem,
    ) -> Result<Arc<dyn Send + Sync>, ProviderError>;
}
