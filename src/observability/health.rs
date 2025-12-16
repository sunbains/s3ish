/// Health check implementations for storage and auth backends
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::Authenticator;
use crate::storage::StorageBackend;

/// Overall health status
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: String,
    pub timestamp: String,
    pub checks: Vec<HealthCheck>,
}

/// Individual health check result
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthCheck {
    pub name: String,
    pub status: String,
    pub message: Option<String>,
    pub duration_ms: f64,
}

/// Check storage backend health
pub async fn check_storage_health(storage: &Arc<dyn StorageBackend>) -> HealthCheck {
    let start = std::time::Instant::now();

    match storage.list_buckets().await {
        Ok(_) => HealthCheck {
            name: "storage".to_string(),
            status: "healthy".to_string(),
            message: None,
            duration_ms: start.elapsed().as_secs_f64() * 1000.0,
        },
        Err(e) => HealthCheck {
            name: "storage".to_string(),
            status: "unhealthy".to_string(),
            message: Some(format!("Storage check failed: {}", e)),
            duration_ms: start.elapsed().as_secs_f64() * 1000.0,
        },
    }
}

/// Check auth backend health
pub async fn check_auth_health(auth: &Arc<dyn Authenticator>) -> HealthCheck {
    let start = std::time::Instant::now();

    // Try to get a non-existent user (should return InvalidCredentials, indicating auth is working)
    match auth.secret_for("__health_check__").await {
        Err(crate::auth::AuthError::InvalidCredentials) => HealthCheck {
            name: "auth".to_string(),
            status: "healthy".to_string(),
            message: None,
            duration_ms: start.elapsed().as_secs_f64() * 1000.0,
        },
        Err(crate::auth::AuthError::Internal(msg)) => HealthCheck {
            name: "auth".to_string(),
            status: "unhealthy".to_string(),
            message: Some(format!("Auth internal error: {}", msg)),
            duration_ms: start.elapsed().as_secs_f64() * 1000.0,
        },
        _ => HealthCheck {
            name: "auth".to_string(),
            status: "healthy".to_string(),
            message: None,
            duration_ms: start.elapsed().as_secs_f64() * 1000.0,
        },
    }
}

/// Get overall health status by checking all backends
pub async fn get_health_status(
    storage: &Arc<dyn StorageBackend>,
    auth: &Arc<dyn Authenticator>,
) -> HealthStatus {
    let checks = vec![
        check_storage_health(storage).await,
        check_auth_health(auth).await,
    ];

    let all_healthy = checks.iter().all(|c| c.status == "healthy");

    HealthStatus {
        status: if all_healthy {
            "healthy".to_string()
        } else {
            "unhealthy".to_string()
        },
        timestamp: chrono::Utc::now().to_rfc3339(),
        checks,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_structure() {
        let check = HealthCheck {
            name: "test".to_string(),
            status: "healthy".to_string(),
            message: None,
            duration_ms: 1.5,
        };

        assert_eq!(check.name, "test");
        assert_eq!(check.status, "healthy");
        assert_eq!(check.duration_ms, 1.5);
    }

    #[test]
    fn test_health_status_all_healthy() {
        let checks = vec![
            HealthCheck {
                name: "storage".to_string(),
                status: "healthy".to_string(),
                message: None,
                duration_ms: 1.0,
            },
            HealthCheck {
                name: "auth".to_string(),
                status: "healthy".to_string(),
                message: None,
                duration_ms: 0.5,
            },
        ];

        let all_healthy = checks.iter().all(|c| c.status == "healthy");
        assert!(all_healthy);
    }

    #[test]
    fn test_health_status_one_unhealthy() {
        let checks = vec![
            HealthCheck {
                name: "storage".to_string(),
                status: "healthy".to_string(),
                message: None,
                duration_ms: 1.0,
            },
            HealthCheck {
                name: "auth".to_string(),
                status: "unhealthy".to_string(),
                message: Some("Error".to_string()),
                duration_ms: 0.5,
            },
        ];

        let all_healthy = checks.iter().all(|c| c.status == "healthy");
        assert!(!all_healthy);
    }
}
