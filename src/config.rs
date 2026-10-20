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

/// Application policy, independent of queue messages and catalog connections.
#[derive(Debug, Clone)]
pub struct Layout {
    pub schema: String,
    pub time_column: String,
    pub time_type: TimeType,
    pub timezone: Tz,
    /// Fixed per table across all writers, independent of the requested window timezone.
    pub lock_timezone: Tz,
    pub row_group_col: Option<String>,
    pub sort_cols: Vec<String>,
    pub dedup_timestamp: String,
    pub author: String,
    /// Retain an existing namespace when replacing a deployed repack worker.
    pub lock_namespace: String,
}

impl Default for Layout {
    fn default() -> Self {
        Self {
            schema: "main".into(),
            time_column: "time".into(),
            time_type: TimeType::TimestampTz,
            timezone: chrono_tz::UTC,
            lock_timezone: chrono_tz::UTC,
            row_group_col: None,
            sort_cols: vec!["time".into()],
            dedup_timestamp: "updated_at".into(),
            author: "dkplus".into(),
            lock_namespace: "dkplus.repack".into(),
        }
    }
}

impl Layout {
    pub fn validate(&self) -> Result<()> {
        identifier(&self.schema)?;
        identifier(&self.time_column)?;
        identifier(&self.dedup_timestamp)?;
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
        Ok(())
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

/// Typed bounds for replacement loads. Repack uses calendar dates plus Layout.
#[derive(Debug, Clone)]
pub enum Window {
    TimestampTz {
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    },
    Timestamp {
        start: chrono::NaiveDateTime,
        end: chrono::NaiveDateTime,
    },
    Date {
        start: NaiveDate,
        end: NaiveDate,
    },
}

impl Window {
    pub(crate) fn bounds(&self, layout: &Layout) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
        let (kind, start, end) = match self {
            Self::TimestampTz { start, end } => (TimeType::TimestampTz, *start, *end),
            Self::Timestamp { start, end } => (TimeType::Timestamp, start.and_utc(), end.and_utc()),
            Self::Date { start, end } => (
                TimeType::Date,
                layout.midnight(*start)?,
                layout.midnight(*end)?,
            ),
        };
        if kind != layout.time_type || start >= end {
            bail!("window type must match the layout and start must precede end");
        }
        Ok((start, end))
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
    fn typed_windows_and_dst_have_explicit_domains() {
        let start = NaiveDate::from_ymd_opt(2026, 3, 8).unwrap();
        let end = start.succ_opt().unwrap();
        let mut layout = Layout {
            timezone: chrono_tz::America::Los_Angeles,
            ..Layout::default()
        };
        let a = layout.midnight(start).unwrap();
        let b = layout.midnight(end).unwrap();
        assert_eq!((b - a).num_hours(), 23);
        assert!(Window::Date { start, end }.bounds(&layout).is_err());
        layout.time_type = TimeType::Timestamp;
        let (a, b) = Window::Timestamp {
            start: start.and_hms_opt(0, 0, 0).unwrap(),
            end: end.and_hms_opt(0, 0, 0).unwrap(),
        }
        .bounds(&layout)
        .unwrap();
        assert_eq!((b - a).num_hours(), 24);
        layout.time_type = TimeType::Date;
        assert!(Window::Date { start, end }.bounds(&layout).is_ok());
        assert!(
            Window::Date {
                start: end,
                end: start
            }
            .bounds(&layout)
            .is_err()
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
