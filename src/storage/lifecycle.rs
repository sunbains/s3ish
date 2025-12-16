use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use super::{ObjectMetadata, StorageError};

/// S3 namespace for XML serialization
const XMLNS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

// ============================================================================
// Data Structures
// ============================================================================

/// Lifecycle configuration for a bucket
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LifecyclePolicy {
    /// List of lifecycle rules
    pub rules: Vec<LifecycleRule>,
}

/// Individual lifecycle rule
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LifecycleRule {
    /// Unique identifier for the rule (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// Object key prefix filter
    pub prefix: String,

    /// Rule status (Enabled or Disabled)
    pub status: RuleStatus,

    /// Expiration settings for current versions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration: Option<Expiration>,

    /// Storage class transitions for current versions
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub transitions: Vec<Transition>,

    /// Expiration settings for noncurrent versions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpiration>,

    /// Transitions for noncurrent versions
    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub noncurrent_version_transitions: Vec<NoncurrentVersionTransition>,

    /// Abort incomplete multipart uploads
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUpload>,
}

/// Rule status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleStatus {
    Enabled,
    Disabled,
}

/// Expiration configuration for current object versions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Expiration {
    /// Expire objects after N days from creation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub days: Option<u32>,

    /// Expire objects on a specific date (ISO 8601 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,

    /// Remove expired delete markers
    #[serde(default)]
    pub expired_object_delete_marker: bool,
}

/// Storage class transition configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Transition {
    /// Days after object creation to transition
    #[serde(skip_serializing_if = "Option::is_none")]
    pub days: Option<u32>,

    /// Specific date to transition (ISO 8601 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,

    /// Target storage class
    pub storage_class: String,
}

/// Expiration for noncurrent (old) versions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NoncurrentVersionExpiration {
    /// Days after version becomes noncurrent
    pub noncurrent_days: u32,
}

/// Transitions for noncurrent versions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NoncurrentVersionTransition {
    /// Days after version becomes noncurrent
    pub noncurrent_days: u32,

    /// Target storage class
    pub storage_class: String,
}

/// Configuration for aborting incomplete multipart uploads
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AbortIncompleteMultipartUpload {
    /// Days after initiation to abort
    pub days_after_initiation: u32,
}

// ============================================================================
// Lifecycle Storage Trait
// ============================================================================

/// Low-level trait for lifecycle policy storage operations.
/// Storage backends implement this to provide lifecycle configuration persistence.
#[async_trait]
pub trait LifecycleStorage: Send + Sync {
    /// Read lifecycle policy for a bucket
    /// Returns None if no policy is configured
    async fn read_lifecycle_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<LifecyclePolicy>, StorageError>;

    /// Write lifecycle policy for a bucket
    async fn write_lifecycle_policy(
        &self,
        bucket: &str,
        policy: LifecyclePolicy,
    ) -> Result<(), StorageError>;

    /// Delete lifecycle policy for a bucket
    /// Returns true if policy existed, false otherwise
    async fn delete_lifecycle_policy(&self, bucket: &str) -> Result<bool, StorageError>;
}

// ============================================================================
// Lifecycle Manager
// ============================================================================

/// High-level manager for lifecycle policy operations
pub struct LifecycleManager<'a, S: ?Sized> {
    storage: &'a S,
}

impl<'a, S: ?Sized + LifecycleStorage> LifecycleManager<'a, S> {
    /// Create a new lifecycle manager
    pub fn new(storage: &'a S) -> Self {
        Self { storage }
    }

    /// Get lifecycle policy for a bucket
    pub async fn get_policy(
        &self,
        bucket: &str,
    ) -> Result<Option<LifecyclePolicy>, StorageError> {
        self.storage.read_lifecycle_policy(bucket).await
    }

    /// Set lifecycle policy for a bucket
    pub async fn put_policy(
        &self,
        bucket: &str,
        policy: LifecyclePolicy,
    ) -> Result<(), StorageError> {
        // Validate policy rules
        self.validate_policy(&policy)?;
        self.storage.write_lifecycle_policy(bucket, policy).await
    }

    /// Delete lifecycle policy for a bucket
    pub async fn delete_policy(&self, bucket: &str) -> Result<bool, StorageError> {
        self.storage.delete_lifecycle_policy(bucket).await
    }

    /// Validate lifecycle policy rules
    fn validate_policy(&self, policy: &LifecyclePolicy) -> Result<(), StorageError> {
        if policy.rules.is_empty() {
            return Err(StorageError::InvalidInput(
                "Lifecycle policy must have at least one rule".to_string(),
            ));
        }

        for rule in &policy.rules {
            // Validate rule has at least one action
            if rule.expiration.is_none()
                && rule.transitions.is_empty()
                && rule.noncurrent_version_expiration.is_none()
                && rule.noncurrent_version_transitions.is_empty()
                && rule.abort_incomplete_multipart_upload.is_none()
            {
                return Err(StorageError::InvalidInput(
                    "Rule must have at least one action".to_string(),
                ));
            }
        }

        Ok(())
    }
}

// ============================================================================
// XML Parsing and Serialization
// ============================================================================

/// Parse S3 XML lifecycle configuration
pub fn parse_lifecycle_xml(xml: &str) -> Result<LifecyclePolicy, StorageError> {
    let mut rules = Vec::new();

    // Find all <Rule> blocks
    let mut cursor = 0;
    while let Some(start) = xml[cursor..].find("<Rule>") {
        let start_pos = cursor + start;
        if let Some(end_offset) = xml[start_pos..].find("</Rule>") {
            let end_pos = start_pos + end_offset + 7; // Include </Rule>
            let rule_xml = &xml[start_pos..end_pos];

            if let Ok(rule) = parse_rule_xml(rule_xml) {
                rules.push(rule);
            }

            cursor = end_pos;
        } else {
            break;
        }
    }

    if rules.is_empty() {
        return Err(StorageError::InvalidInput(
            "No valid rules found in lifecycle configuration".to_string(),
        ));
    }

    Ok(LifecyclePolicy { rules })
}

/// Parse a single <Rule> element
fn parse_rule_xml(xml: &str) -> Result<LifecycleRule, StorageError> {
    let id = extract_tag_content(xml, "ID");
    let prefix = extract_tag_content(xml, "Prefix").unwrap_or_default();
    let status = extract_tag_content(xml, "Status")
        .ok_or_else(|| StorageError::InvalidInput("Missing Status".to_string()))?;

    let status = match status.as_str() {
        "Enabled" => RuleStatus::Enabled,
        "Disabled" => RuleStatus::Disabled,
        _ => {
            return Err(StorageError::InvalidInput(
                "Invalid status, must be Enabled or Disabled".to_string(),
            ))
        }
    };

    // Parse expiration
    let expiration = if xml.contains("<Expiration>") {
        Some(parse_expiration_xml(xml)?)
    } else {
        None
    };

    // Parse transitions
    let transitions = parse_transitions_xml(xml);

    // Parse noncurrent version expiration
    let noncurrent_version_expiration = if xml.contains("<NoncurrentVersionExpiration>") {
        parse_noncurrent_expiration_xml(xml)
    } else {
        None
    };

    // Parse noncurrent version transitions
    let noncurrent_version_transitions = parse_noncurrent_transitions_xml(xml);

    // Parse abort multipart
    let abort_incomplete_multipart_upload = if xml.contains("<AbortIncompleteMultipartUpload>") {
        parse_abort_multipart_xml(xml)
    } else {
        None
    };

    Ok(LifecycleRule {
        id,
        prefix,
        status,
        expiration,
        transitions,
        noncurrent_version_expiration,
        noncurrent_version_transitions,
        abort_incomplete_multipart_upload,
    })
}

/// Extract content between XML tags
fn extract_tag_content(xml: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);

    if let Some(start) = xml.find(&open_tag) {
        let content_start = start + open_tag.len();
        if let Some(end) = xml[content_start..].find(&close_tag) {
            return Some(xml[content_start..content_start + end].to_string());
        }
    }
    None
}

fn parse_expiration_xml(xml: &str) -> Result<Expiration, StorageError> {
    let days = extract_tag_content(xml, "Days").and_then(|s| s.parse().ok());
    let date = extract_tag_content(xml, "Date");
    let expired_object_delete_marker = extract_tag_content(xml, "ExpiredObjectDeleteMarker")
        .map(|s| s == "true")
        .unwrap_or(false);

    Ok(Expiration {
        days,
        date,
        expired_object_delete_marker,
    })
}

fn parse_transitions_xml(xml: &str) -> Vec<Transition> {
    let mut transitions = Vec::new();
    let mut cursor = 0;

    while let Some(start) = xml[cursor..].find("<Transition>") {
        let start_pos = cursor + start;
        if let Some(end_offset) = xml[start_pos..].find("</Transition>") {
            let end_pos = start_pos + end_offset + 13;
            let transition_xml = &xml[start_pos..end_pos];

            let days = extract_tag_content(transition_xml, "Days").and_then(|s| s.parse().ok());
            let date = extract_tag_content(transition_xml, "Date");
            let storage_class = extract_tag_content(transition_xml, "StorageClass");

            if let Some(storage_class) = storage_class {
                transitions.push(Transition {
                    days,
                    date,
                    storage_class,
                });
            }

            cursor = end_pos;
        } else {
            break;
        }
    }

    transitions
}

fn parse_noncurrent_expiration_xml(xml: &str) -> Option<NoncurrentVersionExpiration> {
    extract_tag_content(xml, "NoncurrentDays")
        .and_then(|s| s.parse().ok())
        .map(|noncurrent_days| NoncurrentVersionExpiration { noncurrent_days })
}

fn parse_noncurrent_transitions_xml(xml: &str) -> Vec<NoncurrentVersionTransition> {
    let mut transitions = Vec::new();
    let mut cursor = 0;

    while let Some(start) = xml[cursor..].find("<NoncurrentVersionTransition>") {
        let start_pos = cursor + start;
        if let Some(end_offset) = xml[start_pos..].find("</NoncurrentVersionTransition>") {
            let end_pos = start_pos + end_offset + 30;
            let transition_xml = &xml[start_pos..end_pos];

            let noncurrent_days =
                extract_tag_content(transition_xml, "NoncurrentDays").and_then(|s| s.parse().ok());
            let storage_class = extract_tag_content(transition_xml, "StorageClass");

            if let (Some(noncurrent_days), Some(storage_class)) = (noncurrent_days, storage_class)
            {
                transitions.push(NoncurrentVersionTransition {
                    noncurrent_days,
                    storage_class,
                });
            }

            cursor = end_pos;
        } else {
            break;
        }
    }

    transitions
}

fn parse_abort_multipart_xml(xml: &str) -> Option<AbortIncompleteMultipartUpload> {
    extract_tag_content(xml, "DaysAfterInitiation")
        .and_then(|s| s.parse().ok())
        .map(|days_after_initiation| AbortIncompleteMultipartUpload {
            days_after_initiation,
        })
}

/// Serialize lifecycle policy to S3 XML format
pub fn serialize_lifecycle_xml(policy: &LifecyclePolicy) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="UTF-8"?>"#);
    xml.push_str(&format!(
        r#"<LifecycleConfiguration xmlns="{}">"#,
        XMLNS
    ));

    for rule in &policy.rules {
        xml.push_str("<Rule>");

        if let Some(id) = &rule.id {
            xml.push_str(&format!("<ID>{}</ID>", id));
        }
        xml.push_str(&format!("<Prefix>{}</Prefix>", rule.prefix));

        let status_str = match rule.status {
            RuleStatus::Enabled => "Enabled",
            RuleStatus::Disabled => "Disabled",
        };
        xml.push_str(&format!("<Status>{}</Status>", status_str));

        // Serialize expiration
        if let Some(exp) = &rule.expiration {
            xml.push_str("<Expiration>");
            if let Some(days) = exp.days {
                xml.push_str(&format!("<Days>{}</Days>", days));
            }
            if let Some(date) = &exp.date {
                xml.push_str(&format!("<Date>{}</Date>", date));
            }
            if exp.expired_object_delete_marker {
                xml.push_str("<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>");
            }
            xml.push_str("</Expiration>");
        }

        // Serialize transitions
        for transition in &rule.transitions {
            xml.push_str("<Transition>");
            if let Some(days) = transition.days {
                xml.push_str(&format!("<Days>{}</Days>", days));
            }
            if let Some(date) = &transition.date {
                xml.push_str(&format!("<Date>{}</Date>", date));
            }
            xml.push_str(&format!(
                "<StorageClass>{}</StorageClass>",
                transition.storage_class
            ));
            xml.push_str("</Transition>");
        }

        // Serialize noncurrent version expiration
        if let Some(nc_exp) = &rule.noncurrent_version_expiration {
            xml.push_str("<NoncurrentVersionExpiration>");
            xml.push_str(&format!(
                "<NoncurrentDays>{}</NoncurrentDays>",
                nc_exp.noncurrent_days
            ));
            xml.push_str("</NoncurrentVersionExpiration>");
        }

        // Serialize noncurrent version transitions
        for transition in &rule.noncurrent_version_transitions {
            xml.push_str("<NoncurrentVersionTransition>");
            xml.push_str(&format!(
                "<NoncurrentDays>{}</NoncurrentDays>",
                transition.noncurrent_days
            ));
            xml.push_str(&format!(
                "<StorageClass>{}</StorageClass>",
                transition.storage_class
            ));
            xml.push_str("</NoncurrentVersionTransition>");
        }

        // Serialize abort multipart
        if let Some(abort) = &rule.abort_incomplete_multipart_upload {
            xml.push_str("<AbortIncompleteMultipartUpload>");
            xml.push_str(&format!(
                "<DaysAfterInitiation>{}</DaysAfterInitiation>",
                abort.days_after_initiation
            ));
            xml.push_str("</AbortIncompleteMultipartUpload>");
        }

        xml.push_str("</Rule>");
    }

    xml.push_str("</LifecycleConfiguration>");
    xml
}

// ============================================================================
// Rule Evaluation
// ============================================================================

/// Executor for evaluating and applying lifecycle rules
pub struct LifecycleEvaluator;

impl LifecycleEvaluator {
    /// Evaluate if an object matches a rule's filter
    pub fn matches_rule(object_key: &str, rule: &LifecycleRule) -> bool {
        // Check if object key starts with rule prefix
        object_key.starts_with(&rule.prefix)
    }

    /// Calculate days since object was last modified
    pub fn days_since_modified(last_modified_unix_secs: i64) -> u32 {
        let now = chrono::Utc::now().timestamp();
        let diff = now - last_modified_unix_secs;
        (diff / 86400).max(0) as u32
    }

    /// Determine action to take for an object based on a rule
    pub fn evaluate_action(
        object_meta: &ObjectMetadata,
        rule: &LifecycleRule,
    ) -> Option<LifecycleAction> {
        if rule.status != RuleStatus::Enabled {
            return None;
        }

        let days_old = Self::days_since_modified(object_meta.last_modified_unix_secs);

        // Check expiration (highest priority)
        if let Some(expiration) = &rule.expiration {
            if let Some(expire_days) = expiration.days {
                if days_old >= expire_days {
                    return Some(LifecycleAction::Delete);
                }
            }

            // Handle expired delete markers
            if expiration.expired_object_delete_marker && object_meta.is_delete_marker {
                return Some(LifecycleAction::RemoveDeleteMarker);
            }
        }

        // Check transitions (apply earliest matching transition)
        for transition in &rule.transitions {
            if let Some(transition_days) = transition.days {
                if days_old >= transition_days {
                    let current_class =
                        object_meta.storage_class.as_deref().unwrap_or("STANDARD");
                    if current_class != transition.storage_class {
                        return Some(LifecycleAction::Transition {
                            target_class: transition.storage_class.clone(),
                        });
                    }
                }
            }
        }

        None
    }

    /// Evaluate noncurrent version rules
    pub fn evaluate_noncurrent_action(
        object_meta: &ObjectMetadata,
        rule: &LifecycleRule,
    ) -> Option<LifecycleAction> {
        if rule.status != RuleStatus::Enabled || object_meta.is_latest {
            return None;
        }

        let days_old = Self::days_since_modified(object_meta.last_modified_unix_secs);

        // Check noncurrent version expiration
        if let Some(nc_expiration) = &rule.noncurrent_version_expiration {
            if days_old >= nc_expiration.noncurrent_days {
                return Some(LifecycleAction::DeleteVersion);
            }
        }

        // Check noncurrent version transitions
        for transition in &rule.noncurrent_version_transitions {
            if days_old >= transition.noncurrent_days {
                let current_class = object_meta.storage_class.as_deref().unwrap_or("STANDARD");
                if current_class != transition.storage_class {
                    return Some(LifecycleAction::Transition {
                        target_class: transition.storage_class.clone(),
                    });
                }
            }
        }

        None
    }
}

/// Actions that can be taken by lifecycle rules
#[derive(Debug, Clone, PartialEq)]
pub enum LifecycleAction {
    /// Delete the object
    Delete,
    /// Delete a specific version
    DeleteVersion,
    /// Transition to a different storage class
    Transition { target_class: String },
    /// Remove an expired delete marker
    RemoveDeleteMarker,
    /// Abort multipart upload
    AbortMultipartUpload,
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_parse_lifecycle_xml_basic() {
        let xml = r#"
            <?xml version="1.0" encoding="UTF-8"?>
            <LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Rule>
                    <ID>rule1</ID>
                    <Prefix>logs/</Prefix>
                    <Status>Enabled</Status>
                    <Expiration>
                        <Days>30</Days>
                    </Expiration>
                </Rule>
            </LifecycleConfiguration>
        "#;

        let policy = parse_lifecycle_xml(xml).unwrap();
        assert_eq!(policy.rules.len(), 1);
        assert_eq!(policy.rules[0].id, Some("rule1".to_string()));
        assert_eq!(policy.rules[0].prefix, "logs/");
        assert_eq!(policy.rules[0].status, RuleStatus::Enabled);
        assert_eq!(policy.rules[0].expiration.as_ref().unwrap().days, Some(30));
    }

    #[test]
    fn test_serialize_lifecycle_xml() {
        let policy = LifecyclePolicy {
            rules: vec![LifecycleRule {
                id: Some("rule1".to_string()),
                prefix: "temp/".to_string(),
                status: RuleStatus::Enabled,
                expiration: Some(Expiration {
                    days: Some(7),
                    date: None,
                    expired_object_delete_marker: false,
                }),
                transitions: vec![],
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: vec![],
                abort_incomplete_multipart_upload: None,
            }],
        };

        let xml = serialize_lifecycle_xml(&policy);
        assert!(xml.contains("<ID>rule1</ID>"));
        assert!(xml.contains("<Prefix>temp/</Prefix>"));
        assert!(xml.contains("<Status>Enabled</Status>"));
        assert!(xml.contains("<Days>7</Days>"));
    }

    #[test]
    fn test_rule_matching() {
        let rule = LifecycleRule {
            id: None,
            prefix: "logs/".to_string(),
            status: RuleStatus::Enabled,
            expiration: None,
            transitions: vec![],
            noncurrent_version_expiration: None,
            noncurrent_version_transitions: vec![],
            abort_incomplete_multipart_upload: None,
        };

        assert!(LifecycleEvaluator::matches_rule(
            "logs/2024/01/file.log",
            &rule
        ));
        assert!(LifecycleEvaluator::matches_rule("logs/data.txt", &rule));
        assert!(!LifecycleEvaluator::matches_rule("data/file.txt", &rule));
    }

    #[test]
    fn test_days_calculation() {
        let now = chrono::Utc::now().timestamp();
        let yesterday = now - 86400;
        let week_ago = now - (86400 * 7);

        assert_eq!(LifecycleEvaluator::days_since_modified(yesterday), 1);
        assert_eq!(LifecycleEvaluator::days_since_modified(week_ago), 7);
    }

    #[test]
    fn test_evaluate_expiration_action() {
        let rule = LifecycleRule {
            id: None,
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
        };

        let old_object = ObjectMetadata {
            content_type: "text/plain".to_string(),
            etag: "abc123".to_string(),
            size: 1024,
            last_modified_unix_secs: chrono::Utc::now().timestamp() - (86400 * 31),
            metadata: HashMap::new(),
            storage_class: None,
            server_side_encryption: None,
            version_id: None,
            is_latest: true,
            is_delete_marker: false,
        };

        let action = LifecycleEvaluator::evaluate_action(&old_object, &rule);
        assert_eq!(action, Some(LifecycleAction::Delete));
    }

    #[test]
    fn test_parse_multiple_rules() {
        let xml = r#"
            <?xml version="1.0" encoding="UTF-8"?>
            <LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Rule>
                    <Prefix>logs/</Prefix>
                    <Status>Enabled</Status>
                    <Expiration>
                        <Days>30</Days>
                    </Expiration>
                </Rule>
                <Rule>
                    <Prefix>temp/</Prefix>
                    <Status>Disabled</Status>
                    <Expiration>
                        <Days>7</Days>
                    </Expiration>
                </Rule>
            </LifecycleConfiguration>
        "#;

        let policy = parse_lifecycle_xml(xml).unwrap();
        assert_eq!(policy.rules.len(), 2);
        assert_eq!(policy.rules[0].prefix, "logs/");
        assert_eq!(policy.rules[1].prefix, "temp/");
        assert_eq!(policy.rules[1].status, RuleStatus::Disabled);
    }

    #[test]
    fn test_parse_with_transitions() {
        let xml = r#"
            <LifecycleConfiguration>
                <Rule>
                    <Prefix>archive/</Prefix>
                    <Status>Enabled</Status>
                    <Transition>
                        <Days>30</Days>
                        <StorageClass>GLACIER</StorageClass>
                    </Transition>
                </Rule>
            </LifecycleConfiguration>
        "#;

        let policy = parse_lifecycle_xml(xml).unwrap();
        assert_eq!(policy.rules[0].transitions.len(), 1);
        assert_eq!(policy.rules[0].transitions[0].days, Some(30));
        assert_eq!(
            policy.rules[0].transitions[0].storage_class,
            "GLACIER"
        );
    }
}
