//! Typed failures returned by dkplus operations.

/// The result type used by dkplus.
pub type Result<T> = std::result::Result<T, Error>;

/// Operation failures. Match specific variants and include a wildcard arm.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("{table}: missing required setting {setting}")]
    MissingRequiredSetting {
        table: String,
        setting: &'static str,
    },
    #[error("{table}: invalid configuration: {reason}")]
    InvalidConfiguration { table: String, reason: String },
    #[error("ducklake table {table} not found")]
    TableNotFound { table: String },
    #[error("invalid input: {reason}")]
    InvalidInput { reason: String },
    #[error("invalid data: {reason}")]
    InvalidData { reason: String },
    #[error("invalid storage path: {reason}")]
    InvalidStoragePath { reason: String },
    #[error("unsupported column type {data_type}")]
    UnsupportedType { data_type: String },
    #[error("unsupported feature: {reason}")]
    UnsupportedFeature { reason: String },
    #[error("catalog changed: {reason}")]
    CatalogChanged { reason: String },
    #[error("could not commit ducklake snapshot after {attempts} attempts")]
    CommitRetriesExhausted {
        attempts: u32,
        #[source]
        source: Box<Error>,
    },
    #[error("{operation}: {source}")]
    Catalog {
        operation: String,
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("{operation}: {source}")]
    Storage {
        operation: String,
        #[source]
        source: object_store::Error,
    },
    #[error("{operation}: {source}")]
    Io {
        operation: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{operation}: {source}")]
    Arrow {
        operation: String,
        #[source]
        source: datafusion::arrow::error::ArrowError,
    },
    #[error("{operation}: {source}")]
    Parquet {
        operation: String,
        #[source]
        source: datafusion::parquet::errors::ParquetError,
    },
    #[error("{operation}: {source}")]
    Query {
        operation: String,
        #[source]
        source: datafusion::error::DataFusionError,
    },
    #[error("{operation}: {source}")]
    Json {
        operation: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("{operation}: {source}")]
    Url {
        operation: String,
        #[source]
        source: url::ParseError,
    },
    #[error("{operation}: {source}")]
    Tls {
        operation: String,
        #[source]
        source: rustls::Error,
    },
    #[error("{operation}: {source}")]
    Timezone {
        operation: String,
        #[source]
        source: chrono_tz::ParseError,
    },
    #[error("{operation}: {source}")]
    IntegerConversion {
        operation: String,
        #[source]
        source: std::num::TryFromIntError,
    },
    #[error("{operation}: {source}")]
    StatisticWidth {
        operation: String,
        #[source]
        source: std::array::TryFromSliceError,
    },
    #[error("{operation}: {source}")]
    Utf8 {
        operation: String,
        #[source]
        source: std::str::Utf8Error,
    },
    #[error("{operation}: {source}")]
    PathPrefix {
        operation: String,
        #[source]
        source: std::path::StripPrefixError,
    },
    #[error("{operation}: {source}")]
    ObjectPath {
        operation: String,
        #[source]
        source: object_store::path::Error,
    },
}

macro_rules! source_errors {
    ($($variant:ident($source:ty) => $operation:literal),* $(,)?) => {
        $(
            impl From<$source> for Error {
                fn from(source: $source) -> Self {
                    Self::$variant { operation: $operation.into(), source }
                }
            }
        )*

        impl Error {
            fn context(self, context: String) -> Self {
                match self {
                    $(Self::$variant { operation, source } => Self::$variant {
                        operation: format!("{context}: {operation}"),
                        source,
                    },)*
                    Self::InvalidInput { reason } => Self::InvalidInput {
                        reason: format!("{context}: {reason}"),
                    },
                    Self::InvalidData { reason } => Self::InvalidData {
                        reason: format!("{context}: {reason}"),
                    },
                    Self::InvalidStoragePath { reason } => Self::InvalidStoragePath {
                        reason: format!("{context}: {reason}"),
                    },
                    Self::InvalidConfiguration { table, reason } => Self::InvalidConfiguration {
                        table,
                        reason: format!("{context}: {reason}"),
                    },
                    Self::UnsupportedFeature { reason } => Self::UnsupportedFeature {
                        reason: format!("{context}: {reason}"),
                    },
                    Self::CatalogChanged { reason } => Self::CatalogChanged {
                        reason: format!("{context}: {reason}"),
                    },
                    Self::CommitRetriesExhausted { attempts, source } => Self::CommitRetriesExhausted {
                        attempts,
                        source: Box::new((*source).context(context)),
                    },
                    error @ (Self::MissingRequiredSetting { .. }
                        | Self::TableNotFound { .. }
                        | Self::UnsupportedType { .. }) => error,
                }
            }
        }
    };
}

source_errors! {
    Catalog(tokio_postgres::Error) => "catalog operation failed",
    Storage(object_store::Error) => "object-store operation failed",
    Io(std::io::Error) => "I/O operation failed",
    Arrow(datafusion::arrow::error::ArrowError) => "Arrow operation failed",
    Parquet(datafusion::parquet::errors::ParquetError) => "Parquet operation failed",
    Query(datafusion::error::DataFusionError) => "query execution failed",
    Json(serde_json::Error) => "invalid JSON",
    Url(url::ParseError) => "invalid URL",
    Tls(rustls::Error) => "TLS configuration failed",
    Timezone(chrono_tz::ParseError) => "invalid timezone",
    IntegerConversion(std::num::TryFromIntError) => "integer out of range",
    StatisticWidth(std::array::TryFromSliceError) => "invalid statistic byte width",
    Utf8(std::str::Utf8Error) => "invalid UTF-8",
    PathPrefix(std::path::StripPrefixError) => "path is outside root",
    ObjectPath(object_store::path::Error) => "invalid object-store path",
}

impl Error {
    pub(crate) fn for_table(self, table: impl ToString) -> Self {
        match self {
            Self::InvalidInput { reason } => Self::InvalidConfiguration {
                table: table.to_string(),
                reason,
            },
            other => other,
        }
    }
}

// Attach operation details without erasing the concrete error category or source.
pub(crate) trait Context<T> {
    fn context(self, context: impl Into<String>) -> Result<T>;
    fn with_context(self, context: impl FnOnce() -> String) -> Result<T>;
}

impl<T, E: Into<Error>> Context<T> for std::result::Result<T, E> {
    fn context(self, context: impl Into<String>) -> Result<T> {
        self.map_err(|error| error.into().context(context.into()))
    }

    fn with_context(self, context: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|error| error.into().context(context()))
    }
}

impl<T> Context<T> for Option<T> {
    fn context(self, context: impl Into<String>) -> Result<T> {
        self.ok_or_else(|| Error::InvalidInput {
            reason: context.into(),
        })
    }

    fn with_context(self, context: impl FnOnce() -> String) -> Result<T> {
        self.ok_or_else(|| Error::InvalidInput { reason: context() })
    }
}

macro_rules! fail {
    ($variant:ident, $($arg:tt)*) => {
        return Err($crate::Error::$variant { reason: format!($($arg)*) })
    };
}
pub(crate) use fail;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn context_preserves_typed_sources() {
        let original = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let error = Err::<(), _>(original)
            .context("opening policy")
            .with_context(|| "applying update".into())
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("applying update: opening policy")
        );
        let Error::Io { source, .. } = &error else {
            panic!("{error:?}")
        };
        assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(error.source().unwrap().is::<std::io::Error>());

        let error = serde_json::from_str::<Vec<String>>("{")
            .context("decoding sort_cols")
            .unwrap_err();
        assert!(matches!(error, Error::Json { .. }));
        assert!(error.source().unwrap().is::<serde_json::Error>());

        let error = "port=invalid"
            .parse::<tokio_postgres::Config>()
            .context("reading connection configuration")
            .unwrap_err();
        assert!(matches!(error, Error::Catalog { .. }));
        assert!(error.source().unwrap().is::<tokio_postgres::Error>());
    }

    #[test]
    fn optional_context_is_lazy_and_configuration_keeps_table() {
        let value = Some(42)
            .with_context(|| panic!("must not evaluate on success"))
            .unwrap();
        assert_eq!(value, 42);
        let value = Ok::<_, std::io::Error>(42)
            .with_context(|| panic!("must not evaluate on success"))
            .unwrap();
        assert_eq!(value, 42);
        let error = None::<()>
            .context("invalid policy")
            .unwrap_err()
            .for_table("a.b");
        assert!(
            matches!(&error, Error::InvalidConfiguration { table, reason }
            if table == "a.b" && reason == "invalid policy")
        );
        assert!(error.source().is_none());
    }

    #[test]
    fn retry_exhaustion_preserves_last_failure() {
        let catalog = Error::from(
            "port=invalid"
                .parse::<tokio_postgres::Config>()
                .unwrap_err(),
        );
        let error = Error::CommitRetriesExhausted {
            attempts: 6,
            source: Box::new(catalog),
        };
        let Error::CommitRetriesExhausted { source, .. } = &error else {
            panic!("{error:?}");
        };
        assert!(matches!(source.as_ref(), Error::Catalog { .. }));
        assert!(
            error
                .source()
                .unwrap()
                .source()
                .unwrap()
                .is::<tokio_postgres::Error>()
        );
    }

    #[test]
    fn backend_conversions_keep_categories_and_sources() {
        let cases = [
            Error::from(object_store::Error::Generic {
                store: "test",
                source: Box::new(std::io::Error::other("storage failed")),
            }),
            Error::from(datafusion::arrow::error::ArrowError::ComputeError(
                "bad array".into(),
            )),
            Error::from(datafusion::parquet::errors::ParquetError::General(
                "bad file".into(),
            )),
            Error::from(datafusion::error::DataFusionError::Execution(
                "bad query".into(),
            )),
            Error::from(url::Url::parse("://").unwrap_err()),
            Error::from("Invalid/Timezone".parse::<chrono_tz::Tz>().unwrap_err()),
            Error::from(rustls::Error::General("bad TLS".into())),
        ];
        assert!(matches!(cases[0], Error::Storage { .. }));
        assert!(matches!(cases[1], Error::Arrow { .. }));
        assert!(matches!(cases[2], Error::Parquet { .. }));
        assert!(matches!(cases[3], Error::Query { .. }));
        assert!(matches!(cases[4], Error::Url { .. }));
        assert!(matches!(cases[5], Error::Timezone { .. }));
        assert!(matches!(cases[6], Error::Tls { .. }));
        for error in cases {
            let source = error.source().unwrap().to_string();
            let contextual = error.context("operation detail".into());
            assert!(contextual.to_string().contains("operation detail"));
            assert_eq!(contextual.source().unwrap().to_string(), source);
        }
    }
}
