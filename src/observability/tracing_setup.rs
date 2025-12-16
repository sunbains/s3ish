/// Tracing and structured logging configuration
use std::str::FromStr;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Output format for logging
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    /// Human-readable format for development
    Human,
    /// JSON format for production/log aggregation
    Json,
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(OutputFormat::Json),
            "human" => Ok(OutputFormat::Human),
            _ => Ok(OutputFormat::Human), // Default to Human for unknown values
        }
    }
}

/// Initialize tracing subscriber with the specified format
///
/// # Arguments
/// * `format` - The output format (Human or Json)
///
/// # Environment Variables
/// * `RUST_LOG` - Log level filter (e.g., "s3ish=debug,tower=warn")
/// * `LOG_LEVEL` - Default log level if RUST_LOG not set (default: "info")
///
/// # Examples
///
/// ```no_run
/// use s3ish::observability::tracing_setup::{OutputFormat, init_tracing};
///
/// // Human-readable output for development
/// init_tracing(OutputFormat::Human);
///
/// // JSON output for production
/// init_tracing(OutputFormat::Json);
/// ```
pub fn init_tracing(format: OutputFormat) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let log_level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
        EnvFilter::new(format!("s3ish={},tower=warn,axum=info", log_level))
    });

    match format {
        OutputFormat::Human => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(
                    fmt::layer()
                        .with_target(true)
                        .with_thread_ids(true)
                        .with_thread_names(true)
                        .with_file(true)
                        .with_line_number(true)
                        .with_level(true)
                        .with_ansi(true),
                )
                .init();

            tracing::info!(
                format = "human",
                "Tracing initialized with human-readable format"
            );
        }
        OutputFormat::Json => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(
                    fmt::layer()
                        .json()
                        .with_current_span(true)
                        .with_span_list(true)
                        .with_target(true)
                        .with_thread_ids(true)
                        .with_thread_names(true)
                        .with_file(true)
                        .with_line_number(true)
                        .with_level(true),
                )
                .init();

            tracing::info!(
                format = "json",
                "Tracing initialized with JSON format"
            );
        }
    }
}

/// Initialize tracing with automatic format detection from environment
///
/// Reads `LOG_FORMAT` environment variable:
/// * "json" -> JSON format
/// * anything else or not set -> Human-readable format
///
/// # Examples
///
/// ```bash
/// # Human-readable (default)
/// cargo run
///
/// # JSON format
/// LOG_FORMAT=json cargo run
/// ```
pub fn init_tracing_from_env() {
    let format_str = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "human".to_string());
    let format = OutputFormat::from_str(&format_str).unwrap_or(OutputFormat::Human);
    init_tracing(format);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_format_parsing() {
        assert!(matches!(OutputFormat::from_str("json"), Ok(OutputFormat::Json)));
        assert!(matches!(OutputFormat::from_str("JSON"), Ok(OutputFormat::Json)));
        assert!(matches!(
            OutputFormat::from_str("human"),
            Ok(OutputFormat::Human)
        ));
        assert!(matches!(
            OutputFormat::from_str("invalid"),
            Ok(OutputFormat::Human)
        ));
        assert!(matches!(OutputFormat::from_str(""), Ok(OutputFormat::Human)));
    }
}
