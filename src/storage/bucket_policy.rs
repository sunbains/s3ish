use crate::storage::StorageError;
use serde::{Deserialize, Serialize};

/// S3-compatible bucket policy document
///
/// Defines access permissions for S3 buckets and objects using IAM policy syntax
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BucketPolicy {
    #[serde(rename = "Version")]
    pub version: String,

    #[serde(rename = "Statement")]
    pub statements: Vec<Statement>,
}

/// A single policy statement
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Statement {
    #[serde(rename = "Sid", skip_serializing_if = "Option::is_none")]
    pub sid: Option<String>,

    #[serde(rename = "Effect")]
    pub effect: Effect,

    #[serde(rename = "Principal")]
    pub principal: Principal,

    #[serde(rename = "Action")]
    pub action: Action,

    #[serde(rename = "Resource")]
    pub resource: Resource,

    #[serde(rename = "Condition", skip_serializing_if = "Option::is_none")]
    pub condition: Option<Condition>,
}

/// Effect of a policy statement
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Effect {
    Allow,
    Deny,
}

/// Principal that the policy applies to
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Principal {
    /// All principals (*)
    All(String),
    /// AWS principals
    Aws(PrincipalValue),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum PrincipalValue {
    Single(String),
    Multiple(Vec<String>),
}

/// Actions that can be performed
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Action {
    Single(String),
    Multiple(Vec<String>),
}

/// Resources that the policy applies to
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Resource {
    Single(String),
    Multiple(Vec<String>),
}

/// Conditions for when the policy applies
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Condition {
    #[serde(flatten)]
    pub conditions: std::collections::HashMap<String, std::collections::HashMap<String, serde_json::Value>>,
}

impl BucketPolicy {
    /// Parse a bucket policy from JSON
    pub fn from_json(json: &str) -> Result<Self, StorageError> {
        serde_json::from_str(json)
            .map_err(|e| StorageError::InvalidInput(format!("Invalid policy JSON: {}", e)))
    }

    /// Serialize a bucket policy to JSON
    pub fn to_json(&self) -> Result<String, StorageError> {
        serde_json::to_string_pretty(self)
            .map_err(|e| StorageError::Internal(format!("Failed to serialize policy: {}", e)))
    }

    /// Validate the policy structure
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.version != "2012-10-17" && self.version != "2008-10-17" {
            return Err(StorageError::InvalidInput(format!(
                "Invalid policy version: {}. Must be 2012-10-17 or 2008-10-17",
                self.version
            )));
        }

        if self.statements.is_empty() {
            return Err(StorageError::InvalidInput(
                "Policy must contain at least one statement".to_string(),
            ));
        }

        for (idx, statement) in self.statements.iter().enumerate() {
            statement.validate(idx)?;
        }

        Ok(())
    }

    /// Evaluate if an action is allowed for a principal on a resource
    pub fn evaluate(
        &self,
        principal: &str,
        action: &str,
        resource: &str,
    ) -> PolicyDecision {
        let mut has_allow = false;
        let mut has_deny = false;

        for statement in &self.statements {
            if !statement.matches_principal(principal) {
                continue;
            }

            if !statement.matches_action(action) {
                continue;
            }

            if !statement.matches_resource(resource) {
                continue;
            }

            match statement.effect {
                Effect::Allow => has_allow = true,
                Effect::Deny => has_deny = true,
            }
        }

        // Explicit deny takes precedence
        if has_deny {
            PolicyDecision::Deny
        } else if has_allow {
            PolicyDecision::Allow
        } else {
            PolicyDecision::ImplicitDeny
        }
    }
}

impl Statement {
    fn validate(&self, idx: usize) -> Result<(), StorageError> {
        // Validate that actions are not empty
        match &self.action {
            Action::Single(s) if s.is_empty() => {
                return Err(StorageError::InvalidInput(format!(
                    "Statement {}: Action cannot be empty",
                    idx
                )));
            }
            Action::Multiple(v) if v.is_empty() => {
                return Err(StorageError::InvalidInput(format!(
                    "Statement {}: Action list cannot be empty",
                    idx
                )));
            }
            _ => {}
        }

        // Validate that resources are not empty
        match &self.resource {
            Resource::Single(s) if s.is_empty() => {
                return Err(StorageError::InvalidInput(format!(
                    "Statement {}: Resource cannot be empty",
                    idx
                )));
            }
            Resource::Multiple(v) if v.is_empty() => {
                return Err(StorageError::InvalidInput(format!(
                    "Statement {}: Resource list cannot be empty",
                    idx
                )));
            }
            _ => {}
        }

        Ok(())
    }

    fn matches_principal(&self, principal: &str) -> bool {
        match &self.principal {
            Principal::All(s) if s == "*" => true,
            Principal::Aws(PrincipalValue::Single(s)) => {
                s == "*" || s == principal || wildcard_match(s, principal)
            }
            Principal::Aws(PrincipalValue::Multiple(list)) => {
                list.iter().any(|s| s == "*" || s == principal || wildcard_match(s, principal))
            }
            _ => false,
        }
    }

    fn matches_action(&self, action: &str) -> bool {
        match &self.action {
            Action::Single(s) => s == "*" || s == action || wildcard_match(s, action),
            Action::Multiple(list) => {
                list.iter().any(|s| s == "*" || s == action || wildcard_match(s, action))
            }
        }
    }

    fn matches_resource(&self, resource: &str) -> bool {
        match &self.resource {
            Resource::Single(s) => s == "*" || s == resource || wildcard_match(s, resource),
            Resource::Multiple(list) => {
                list.iter().any(|s| s == "*" || s == resource || wildcard_match(s, resource))
            }
        }
    }
}

/// Result of policy evaluation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDecision {
    /// Explicitly allowed by policy
    Allow,
    /// Explicitly denied by policy
    Deny,
    /// No matching policy (default deny)
    ImplicitDeny,
}

/// Simple wildcard matching (supports * only)
fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    if !pattern.contains('*') {
        return pattern == value;
    }

    // Split by * and match each part
    let parts: Vec<&str> = pattern.split('*').collect();

    if parts.is_empty() {
        return false;
    }

    let mut pos = 0;

    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if i == 0 && !value[pos..].starts_with(part) {
            return false;
        }

        if let Some(found_pos) = value[pos..].find(part) {
            pos += found_pos + part.len();
        } else {
            return false;
        }
    }

    // If pattern ends with *, we're done
    if pattern.ends_with('*') {
        true
    } else {
        // Otherwise, make sure we consumed the whole value
        pos == value.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bucket_policy() {
        let json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::my-bucket/*"
                }
            ]
        }"#;

        let policy = BucketPolicy::from_json(json).unwrap();
        assert_eq!(policy.version, "2012-10-17");
        assert_eq!(policy.statements.len(), 1);
        assert_eq!(policy.statements[0].effect, Effect::Allow);
    }

    #[test]
    fn test_policy_validation() {
        let policy = BucketPolicy {
            version: "2012-10-17".to_string(),
            statements: vec![Statement {
                sid: None,
                effect: Effect::Allow,
                principal: Principal::All("*".to_string()),
                action: Action::Single("s3:GetObject".to_string()),
                resource: Resource::Single("arn:aws:s3:::bucket/*".to_string()),
                condition: None,
            }],
        };

        assert!(policy.validate().is_ok());
    }

    #[test]
    fn test_policy_evaluation_allow() {
        let policy = BucketPolicy {
            version: "2012-10-17".to_string(),
            statements: vec![Statement {
                sid: None,
                effect: Effect::Allow,
                principal: Principal::All("*".to_string()),
                action: Action::Single("s3:GetObject".to_string()),
                resource: Resource::Single("arn:aws:s3:::bucket/*".to_string()),
                condition: None,
            }],
        };

        let decision = policy.evaluate("anyone", "s3:GetObject", "arn:aws:s3:::bucket/file.txt");
        assert_eq!(decision, PolicyDecision::Allow);
    }

    #[test]
    fn test_policy_evaluation_deny() {
        let policy = BucketPolicy {
            version: "2012-10-17".to_string(),
            statements: vec![
                Statement {
                    sid: None,
                    effect: Effect::Allow,
                    principal: Principal::All("*".to_string()),
                    action: Action::Single("s3:GetObject".to_string()),
                    resource: Resource::Single("arn:aws:s3:::bucket/*".to_string()),
                    condition: None,
                },
                Statement {
                    sid: None,
                    effect: Effect::Deny,
                    principal: Principal::Aws(PrincipalValue::Single("user123".to_string())),
                    action: Action::Single("s3:GetObject".to_string()),
                    resource: Resource::Single("arn:aws:s3:::bucket/secret.txt".to_string()),
                    condition: None,
                },
            ],
        };

        // Deny takes precedence
        let decision = policy.evaluate("user123", "s3:GetObject", "arn:aws:s3:::bucket/secret.txt");
        assert_eq!(decision, PolicyDecision::Deny);
    }

    #[test]
    fn test_wildcard_match() {
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("s3:*", "s3:GetObject"));
        assert!(wildcard_match("s3:Get*", "s3:GetObject"));
        assert!(wildcard_match("arn:aws:s3:::bucket/*", "arn:aws:s3:::bucket/file.txt"));
        assert!(!wildcard_match("s3:Get*", "s3:PutObject"));
    }

    #[test]
    fn test_policy_serialization() {
        let policy = BucketPolicy {
            version: "2012-10-17".to_string(),
            statements: vec![Statement {
                sid: Some("AllowPublicRead".to_string()),
                effect: Effect::Allow,
                principal: Principal::All("*".to_string()),
                action: Action::Single("s3:GetObject".to_string()),
                resource: Resource::Single("arn:aws:s3:::bucket/*".to_string()),
                condition: None,
            }],
        };

        let json = policy.to_json().unwrap();
        assert!(json.contains("2012-10-17"));
        assert!(json.contains("AllowPublicRead"));

        // Round trip
        let parsed = BucketPolicy::from_json(&json).unwrap();
        assert_eq!(parsed, policy);
    }
}
