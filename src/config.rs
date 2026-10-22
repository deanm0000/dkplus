use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use object_store::{ObjectStore, azure::MicrosoftAzureBuilder, local::LocalFileSystem};
use std::sync::Arc;

pub const DEFAULT_REPACK_RUN_BYTES: usize = 128 * 1024 * 1024;
pub const DEFAULT_REPACK_NODE_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone)]
pub enum Storage {
    Azure {
        account: String,
        access_key: String,
    },
    /// Every catalog URL must be below this root. Keys are relative to the root.
    Store {
        root: String,
        store: Arc<dyn ObjectStore>,
    },
}

impl Storage {
    pub fn local(root: &std::path::Path) -> Result<Self> {
        let root = root
            .canonicalize()
            .context("resolving local storage root")?;
        let url = url::Url::from_directory_path(&root)
            .map_err(|()| anyhow::anyhow!("invalid local storage root"))?;
        Ok(Self::Store {
            root: url.to_string(),
            store: Arc::new(LocalFileSystem::new_with_prefix(root)?),
        })
    }

    /// Resolve a catalog path into a store and relative key. Local stores accept
    /// both file URLs and absolute filesystem paths below their configured root.
    pub fn resolve(&self, path: &str) -> Result<(Arc<dyn ObjectStore>, String)> {
        match self {
            Self::Azure {
                account,
                access_key,
            } => {
                let rest = path
                    .strip_prefix("az://")
                    .context("expected az:// storage path")?;
                let (container, key) = rest.split_once('/').unwrap_or((rest, ""));
                if container.is_empty() {
                    bail!("Azure storage path has no container");
                }
                let store = MicrosoftAzureBuilder::new()
                    .with_account(account)
                    .with_access_key(access_key)
                    .with_container_name(container)
                    .build()
                    .context("building Azure object store")?;
                Ok((Arc::new(store), key.to_string()))
            }
            Self::Store { root, store } => {
                if !root.ends_with('/') {
                    bail!("object-store root must end with '/'");
                }
                let mut key = if root.starts_with("file://") {
                    let local_root = url::Url::parse(root)?
                        .to_file_path()
                        .map_err(|()| anyhow::anyhow!("invalid local root URL"))?;
                    let local_path = if path.starts_with("file://") {
                        url::Url::parse(path)?
                            .to_file_path()
                            .map_err(|()| anyhow::anyhow!("invalid local catalog URL"))?
                    } else {
                        std::path::PathBuf::from(path)
                    };
                    local_path
                        .strip_prefix(&local_root)
                        .with_context(|| {
                            format!("catalog path {path:?} is outside storage root {root:?}")
                        })?
                        .to_str()
                        .context("catalog key is not UTF-8")?
                        .to_owned()
                } else {
                    path.strip_prefix(root)
                        .with_context(|| {
                            format!("catalog path {path:?} is outside storage root {root:?}")
                        })?
                        .to_owned()
                };
                if key.split('/').any(|part| part == "." || part == "..") {
                    bail!("catalog path contains a relative traversal component");
                }
                if path.ends_with('/') && !key.is_empty() && !key.ends_with('/') {
                    key.push('/');
                }
                Ok((store.clone(), key))
            }
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub postgres: tokio_postgres::Config,
    pub storage: Storage,
    pub repack_run_bytes: usize,
    pub repack_node_bytes: usize,
    /// Receives progress and background errors from all operations.
    pub log: Log,
}

pub type Log = Arc<dyn Fn(&str) + Send + Sync>;

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("repack_run_bytes", &self.repack_run_bytes)
            .field("repack_node_bytes", &self.repack_node_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeType {
    TimestampTz,
    Timestamp,
    Date,
}

impl TimeType {
    pub(crate) fn sql(self) -> &'static str {
        match self {
            Self::TimestampTz => "timestamptz",
            Self::Timestamp => "timestamp",
            Self::Date => "date",
        }
    }
}

/// Resolved policy for one DuckLake table. Create it with [`Table::builder`].
#[derive(Debug, Clone)]
pub struct Table {
    pub cfg: Config,
    pub schema: String,
    pub table: String,
    pub time_column: String,
    pub(crate) time_type: TimeType,
    pub timezone: Tz,
    pub row_group_col: Option<String>,
    pub sort_cols: Vec<String>,
    pub unique_by_cols: Option<Vec<String>>,
    pub dedup_timestamp: Option<String>,
    pub author: Option<String>,
    pub lock_namespace: String,
}

/// Unconfigured placeholder for queued jobs. Resolve a [`Table::builder`] before
/// running operations.
impl Default for Table {
    fn default() -> Self {
        Self {
            cfg: Config {
                postgres: tokio_postgres::Config::new(),
                storage: Storage::Store {
                    root: String::new(),
                    store: Arc::new(object_store::memory::InMemory::new()),
                },
                repack_run_bytes: DEFAULT_REPACK_RUN_BYTES,
                repack_node_bytes: DEFAULT_REPACK_NODE_BYTES,
                log: Arc::new(|message| eprintln!("{message}")),
            },
            schema: "main".into(),
            table: String::new(),
            time_column: "time".into(),
            time_type: TimeType::TimestampTz,
            timezone: chrono_tz::UTC,
            row_group_col: None,
            sort_cols: vec!["time".into()],
            unique_by_cols: None,
            dedup_timestamp: None,
            author: None,
            lock_namespace: String::new(),
        }
    }
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.table)
    }
}

/// Builder for resolving table policy from explicit values and DuckLake tags.
#[derive(Debug, Clone)]
pub struct TableBuilder {
    pub(crate) schema: String,
    pub(crate) table: String,
    pub(crate) time_column: Option<String>,
    pub(crate) timezone: Option<Tz>,
    pub(crate) row_group_col: Option<Option<String>>,
    pub(crate) sort_cols: Option<Vec<String>>,
    pub(crate) unique_by_cols: Option<Option<Vec<String>>>,
    pub(crate) dedup_timestamp: Option<Option<String>>,
    pub(crate) author: Option<String>,
    pub(crate) lock_namespace: Option<String>,
}

impl Table {
    pub fn builder(schema: impl Into<String>, table: impl Into<String>) -> TableBuilder {
        TableBuilder::new(schema, table)
    }
}

impl TableBuilder {
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            table: table.into(),
            time_column: None,
            timezone: None,
            row_group_col: None,
            sort_cols: None,
            unique_by_cols: None,
            dedup_timestamp: None,
            author: None,
            lock_namespace: None,
        }
    }

    pub fn with_time_column(mut self, name: impl Into<String>) -> Self {
        self.time_column = Some(name.into());
        self
    }

    pub fn with_timezone(mut self, timezone: Tz) -> Self {
        self.timezone = Some(timezone);
        self
    }

    pub fn with_row_group_col(mut self, name: impl Into<String>) -> Self {
        self.row_group_col = Some(Some(name.into()));
        self
    }

    pub fn without_row_grouping(mut self) -> Self {
        self.row_group_col = Some(None);
        self
    }

    pub fn with_sort_cols(mut self, names: Vec<String>) -> Self {
        self.sort_cols = Some(names);
        self
    }

    pub fn with_unique_by_cols(mut self, names: Vec<String>) -> Self {
        self.unique_by_cols = Some(Some(names));
        self
    }

    pub fn without_deduplication(mut self) -> Self {
        self.unique_by_cols = Some(None);
        self.dedup_timestamp = Some(None);
        self
    }

    pub fn with_dedup_timestamp(mut self, name: impl Into<String>) -> Self {
        self.dedup_timestamp = Some(Some(name.into()));
        self
    }

    pub fn with_author(mut self, author: impl Into<String>) -> Self {
        self.author = Some(author.into());
        self
    }

    pub fn with_lock_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.lock_namespace = Some(namespace.into());
        self
    }

    pub async fn resolve(self, cfg: &Config) -> Result<Table> {
        crate::resolve_table(cfg, self).await
    }
}

impl Table {
    pub fn validate(&self) -> Result<()> {
        identifier(&self.schema)?;
        identifier(&self.table)?;
        identifier(&self.time_column)?;
        if self.sort_cols.is_empty() {
            bail!("sort columns must not be empty");
        }
        for name in &self.sort_cols {
            identifier(name)?;
        }
        if let Some(name) = &self.row_group_col {
            identifier(name)?;
            if self.sort_cols.first() != Some(name) {
                bail!("row-group column must be the first sort column");
            }
        }
        if let Some(unique) = &self.unique_by_cols {
            if unique.is_empty() {
                bail!("unique columns must not be empty");
            }
            let mut seen = std::collections::HashSet::new();
            for name in unique {
                identifier(name)?;
                if !seen.insert(name) {
                    bail!("unique columns must be distinct");
                }
            }
            if self.dedup_timestamp.is_none() {
                bail!("deduplication requires a dedup timestamp column");
            }
        }
        if let Some(name) = &self.dedup_timestamp {
            identifier(name)?;
        }
        Ok(())
    }

    pub(crate) fn author_for(
        &self,
        operation: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> String {
        self.author.clone().unwrap_or_else(|| {
            format!(
                "{operation}_{}_{}",
                start.format("%Y%m%dT%H%M%SZ"),
                end.format("%Y%m%dT%H%M%SZ")
            )
        })
    }

    pub(crate) fn midnight(&self, date: NaiveDate) -> Result<DateTime<Utc>> {
        let naive = date.and_hms_opt(0, 0, 0).context("invalid midnight")?;
        match self.time_type {
            TimeType::TimestampTz => self
                .timezone
                .from_local_datetime(&naive)
                .single()
                .with_context(|| {
                    format!(
                        "{date} midnight is ambiguous or nonexistent in {}",
                        self.timezone
                    )
                })
                .map(|time| time.with_timezone(&Utc)),
            TimeType::Timestamp | TimeType::Date => Ok(naive.and_utc()),
        }
    }

    pub(crate) fn bound(&self, time: DateTime<Utc>) -> String {
        match self.time_type {
            TimeType::TimestampTz => time.to_rfc3339(),
            TimeType::Timestamp => time.naive_utc().to_string(),
            TimeType::Date => time.date_naive().to_string(),
        }
    }
}

pub(crate) fn identifier(name: &str) -> Result<String> {
    if name.is_empty() || name.contains('\0') {
        bail!("SQL identifier must be nonempty and contain no NUL");
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn midnight_domains_preserve_dst_and_wall_clock_dates() {
        let start = NaiveDate::from_ymd_opt(2026, 3, 8).unwrap();
        let end = start.succ_opt().unwrap();
        let mut table = Table {
            table: "records".into(),
            timezone: chrono_tz::America::Los_Angeles,
            ..Table::default()
        };
        let a = table.midnight(start).unwrap();
        let b = table.midnight(end).unwrap();
        assert_eq!((b - a).num_hours(), 23);
        table.time_type = TimeType::Timestamp;
        let (a, b) = (table.midnight(start).unwrap(), table.midnight(end).unwrap());
        assert_eq!((b - a).num_hours(), 24);
        table.time_type = TimeType::Date;
        assert_eq!(
            (table.midnight(end).unwrap() - table.midnight(start).unwrap()).num_days(),
            1
        );
    }

    #[test]
    fn object_roots_are_explicit_and_cannot_escape() {
        let storage = Storage::Store {
            root: "memory:///data/".into(),
            store: Arc::new(object_store::memory::InMemory::new()),
        };
        assert_eq!(
            storage
                .resolve("memory:///data/table/file.parquet")
                .unwrap()
                .1,
            "table/file.parquet"
        );
        assert!(storage.resolve("memory:///database/file.parquet").is_err());
        assert!(storage.resolve("memory:///data/../file.parquet").is_err());
        assert_eq!(identifier("odd\"name").unwrap(), "\"odd\"\"name\"");
        assert!(identifier("").is_err());
    }
}
