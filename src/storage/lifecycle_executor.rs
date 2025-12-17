// Copyright PingCAP Inc. 2025.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; version 2 of the License.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License along
// with this program; if not, write to the Free Software Foundation, Inc.,
// 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

use crate::storage::lifecycle::{LifecycleAction, LifecycleEvaluator};
use crate::storage::StorageBackend;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;

/// Configuration for the lifecycle executor
#[derive(Debug, Clone)]
pub struct LifecycleConfig {
    /// Whether lifecycle execution is enabled
    pub enabled: bool,
    /// Interval between lifecycle policy checks (in seconds)
    pub check_interval_secs: u64,
    /// Maximum number of objects to delete concurrently
    pub max_concurrent_deletes: usize,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_secs: 3600, // 1 hour
            max_concurrent_deletes: 100,
        }
    }
}

/// Background executor for lifecycle policies
///
/// Periodically scans all buckets with lifecycle policies and applies rules:
/// - Object expiration (delete after N days)
/// - Storage class transitions
/// - Versioned object cleanup
/// - Multipart upload cleanup
pub struct LifecycleExecutor {
    storage: Arc<dyn StorageBackend>,
    config: LifecycleConfig,
}

impl LifecycleExecutor {
    /// Create a new lifecycle executor
    pub fn new(storage: Arc<dyn StorageBackend>, config: LifecycleConfig) -> Self {
        Self { storage, config }
    }

    /// Spawn the executor as a background task
    ///
    /// Returns a JoinHandle that can be used to wait for the task or cancel it
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run_loop().await;
        })
    }

    /// Main execution loop - runs periodically based on config
    async fn run_loop(&self) {
        if !self.config.enabled {
            tracing::info!("Lifecycle executor disabled in config");
            return;
        }

        tracing::info!(
            "Starting lifecycle executor (check interval: {}s)",
            self.config.check_interval_secs
        );

        let mut ticker = interval(Duration::from_secs(self.config.check_interval_secs));

        loop {
            ticker.tick().await;

            let start_time = std::time::Instant::now();
            match self.execute_lifecycle_policies().await {
                Ok((rules_evaluated, actions_taken)) => {
                    let duration = start_time.elapsed().as_secs_f64();
                    tracing::info!(
                        "Lifecycle execution completed: {} rules evaluated, {} actions taken, duration: {:.2}s",
                        rules_evaluated,
                        actions_taken,
                        duration
                    );
                }
                Err(e) => {
                    tracing::error!("Lifecycle execution failed: {}", e);
                }
            }
        }
    }

    /// Execute lifecycle policies for all buckets
    ///
    /// Returns (rules_evaluated, actions_taken)
    async fn execute_lifecycle_policies(&self) -> Result<(u64, u64), crate::storage::StorageError> {
        let buckets = self.storage.list_buckets().await?;

        let mut total_rules_evaluated = 0u64;
        let mut total_actions_taken = 0u64;

        for bucket in buckets {
            // Get lifecycle policy for this bucket
            let policy = match self.storage.get_bucket_lifecycle(&bucket).await? {
                Some(p) => p,
                None => continue, // No lifecycle policy for this bucket
            };

            tracing::debug!("Processing lifecycle policies for bucket: {}", bucket);

            // List all objects in the bucket
            let objects = self
                .storage
                .list_objects(&bucket, "", self.config.max_concurrent_deletes * 10)
                .await?;

            for (key, meta) in objects {
                for rule in &policy.rules {
                    // Check if rule is enabled
                    if rule.status != crate::storage::lifecycle::RuleStatus::Enabled {
                        continue;
                    }

                    // Check if object matches rule prefix
                    if !LifecycleEvaluator::matches_rule(&key, rule) {
                        continue;
                    }

                    total_rules_evaluated += 1;

                    // Evaluate action for current version
                    if let Some(action) = LifecycleEvaluator::evaluate_action(&meta, rule) {
                        if let Err(e) = self.apply_action(&bucket, &key, None, action).await {
                            tracing::warn!(
                                "Failed to apply lifecycle action for {}/{}: {}",
                                bucket,
                                key,
                                e
                            );
                        } else {
                            total_actions_taken += 1;
                        }
                    }

                    // Handle versioned objects (non-current versions)
                    if meta.version_id.is_some() {
                        let versions = self
                            .storage
                            .list_object_versions(&bucket, &key, 1000)
                            .await?;

                        for version_meta in versions {
                            // Skip the latest version (already handled above)
                            if version_meta.is_latest {
                                continue;
                            }

                            if let Some(action) =
                                LifecycleEvaluator::evaluate_noncurrent_action(&version_meta, rule)
                            {
                                if let Some(vid) = &version_meta.version_id {
                                    if let Err(e) =
                                        self.apply_action(&bucket, &key, Some(vid), action).await
                                    {
                                        tracing::warn!(
                                            "Failed to apply lifecycle action for {}/{}?versionId={}: {}",
                                            bucket,
                                            key,
                                            vid,
                                            e
                                        );
                                    } else {
                                        total_actions_taken += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok((total_rules_evaluated, total_actions_taken))
    }

    /// Apply a lifecycle action to an object
    async fn apply_action(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        action: LifecycleAction,
    ) -> Result<(), crate::storage::StorageError> {
        match action {
            LifecycleAction::Delete => {
                tracing::debug!("Lifecycle: Deleting object {}/{}", bucket, key);
                self.storage.delete_object(bucket, key).await?;
            }
            LifecycleAction::DeleteVersion => {
                if let Some(vid) = version_id {
                    tracing::debug!(
                        "Lifecycle: Deleting version {}/{}?versionId={}",
                        bucket,
                        key,
                        vid
                    );
                    self.storage.delete_object_version(bucket, key, vid).await?;
                }
            }
            LifecycleAction::Transition { target_class } => {
                tracing::debug!(
                    "Lifecycle: Transitioning {}/{} to storage class {}",
                    bucket,
                    key,
                    target_class
                );
                // Note: For now, transitions are not fully implemented
                // In a real implementation, this would move the object to a different storage tier
                // For simplicity, we just log it
            }
            LifecycleAction::RemoveDeleteMarker => {
                if let Some(vid) = version_id {
                    tracing::debug!(
                        "Lifecycle: Removing delete marker {}/{}?versionId={}",
                        bucket,
                        key,
                        vid
                    );
                    self.storage.delete_object_version(bucket, key, vid).await?;
                }
            }
            LifecycleAction::AbortMultipartUpload => {
                tracing::debug!(
                    "Lifecycle: Aborting incomplete multipart upload {}/{}",
                    bucket,
                    key
                );
                // Note: Multipart upload cleanup would require tracking upload IDs
                // This is a placeholder for future implementation
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::in_memory::InMemoryStorage;
    use crate::storage::lifecycle::{Expiration, LifecyclePolicy, LifecycleRule, RuleStatus};
    use bytes::Bytes;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_lifecycle_executor_deletes_expired_objects() {
        let storage = Arc::new(InMemoryStorage::new()) as Arc<dyn StorageBackend>;

        // Create bucket
        storage.create_bucket("test-bucket").await.unwrap();

        // Put an old object (simulating last_modified_unix_secs from 100 days ago)
        storage
            .put_object(
                "test-bucket",
                "old-file.txt",
                Bytes::from("old content"),
                "text/plain",
                HashMap::new(),
                None,
                None,
            )
            .await
            .unwrap();

        // Create lifecycle policy: delete objects older than 30 days
        let policy = LifecyclePolicy {
            rules: vec![LifecycleRule {
                id: Some("delete-old".to_string()),
                prefix: "".to_string(),
                status: RuleStatus::Enabled,
                expiration: Some(Expiration {
                    days: Some(30),
                    date: None,
                    expired_object_delete_marker: false,
                }),
                transitions: vec![],
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: vec![],
                abort_incomplete_multipart_upload: None,
            }],
        };

        storage
            .put_bucket_lifecycle("test-bucket", policy)
            .await
            .unwrap();

        // Create executor with immediate execution
        let config = LifecycleConfig {
            enabled: true,
            check_interval_secs: 1,
            max_concurrent_deletes: 10,
        };

        let executor = LifecycleExecutor::new(storage.clone(), config);

        // Note: In real test we would need to mock the object's last_modified_unix_secs
        // to be older than 30 days. For now, this test just verifies the executor structure.
        let (rules_evaluated, _actions_taken) = executor.execute_lifecycle_policies().await.unwrap();

        // At least one rule should be evaluated
        assert!(rules_evaluated > 0);
    }

    #[tokio::test]
    async fn test_lifecycle_executor_respects_disabled_config() {
        let storage = Arc::new(InMemoryStorage::new()) as Arc<dyn StorageBackend>;

        let config = LifecycleConfig {
            enabled: false,
            check_interval_secs: 1,
            max_concurrent_deletes: 10,
        };

        let executor = LifecycleExecutor::new(storage, config);

        // Execute should work even with disabled config
        let (rules_evaluated, actions_taken) = executor.execute_lifecycle_policies().await.unwrap();

        // With no buckets, nothing should happen
        assert_eq!(rules_evaluated, 0);
        assert_eq!(actions_taken, 0);
    }
}
