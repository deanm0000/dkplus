use crate::error::Context;
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use object_store::azure::{AzureAccessKey, AzureAuthorizer, AzureCredential};
use object_store::client::{HttpRequest, HttpRequestBody};
use object_store::path::Path;
use reqwest::{Client, Method, Response, StatusCode};
use serde::Deserialize;
use std::time::Duration;
use url::Url;

#[derive(Debug, Default)]
pub struct DirectoryCleanupReport {
    pub candidates: usize,
    pub deleted: usize,
    pub nonempty: usize,
    pub changed: usize,
    pub already_missing: usize,
}

pub(super) struct AzureDirectories {
    account: String,
    credential: AzureCredential,
    endpoint: Url,
    client: Client,
}

#[derive(Deserialize)]
struct PathList {
    paths: Vec<ListedPath>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListedPath {
    name: String,
    #[serde(default)]
    is_directory: DirectoryFlag,
    last_modified: String,
    etag: String,
}

#[derive(Default, Deserialize)]
#[serde(untagged)]
enum DirectoryFlag {
    Bool(bool),
    String(String),
    #[default]
    Missing,
}

impl DirectoryFlag {
    fn is_directory(&self) -> Result<bool> {
        match self {
            Self::Bool(value) => Ok(*value),
            Self::String(value) if value == "true" => Ok(true),
            Self::String(value) if value == "false" => Ok(false),
            Self::Missing => Ok(false),
            Self::String(_) => Err(Error::InvalidData {
                reason: "invalid Azure directory flag".into(),
            }),
        }
    }
}

fn old_directory(
    name: String,
    modified: &str,
    etag: String,
    cutoff: DateTime<Utc>,
) -> Result<Option<(String, String)>> {
    let modified = DateTime::parse_from_rfc2822(modified).map_err(|error| Error::InvalidData {
        reason: format!("invalid Azure directory modification time for {name}: {error}"),
    })?;
    if etag.is_empty() {
        return Err(Error::InvalidData {
            reason: format!("missing Azure directory ETag for {name}"),
        });
    }
    let etag = if etag.starts_with('"') && etag.ends_with('"') {
        etag
    } else {
        format!("\"{etag}\"")
    };
    Ok((modified.with_timezone(&Utc) < cutoff).then_some((name, etag)))
}

impl AzureDirectories {
    pub(super) fn new(account: &str, access_key: &str) -> Result<Self> {
        let credential = AzureCredential::AccessKey(
            AzureAccessKey::try_new(access_key).map_err(std::io::Error::other)?,
        );
        let endpoint = Url::parse(&format!("https://{account}.dfs.core.windows.net/"))?;
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(std::io::Error::other)?;
        Ok(Self {
            account: account.into(),
            credential,
            endpoint,
            client,
        })
    }

    fn url(&self, container: &str, path: Option<&str>) -> Result<Url> {
        let mut url = self.endpoint.clone();
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| Error::InvalidStoragePath {
                    reason: "Azure endpoint cannot contain path segments".into(),
                })?;
            segments.pop_if_empty().push(container);
            if let Some(path) = path {
                segments.extend(path.split('/'));
            }
        }
        Ok(url)
    }

    async fn request(&self, method: Method, url: Url, etag: Option<&str>) -> Result<Response> {
        for attempt in 0..3 {
            let mut request = HttpRequest::new(HttpRequestBody::empty());
            *request.method_mut() = method.clone();
            *request.uri_mut() = url.as_str().parse().map_err(std::io::Error::other)?;
            if let Some(etag) = etag {
                request
                    .headers_mut()
                    .insert("if-match", etag.parse().map_err(std::io::Error::other)?);
            }
            AzureAuthorizer::new(&self.credential, &self.account).authorize(&mut request);
            let response = self
                .client
                .request(method.clone(), url.clone())
                .headers(request.headers().clone())
                .send()
                .await;
            match response {
                Ok(response)
                    if attempt < 2
                        && (response.status().is_server_error()
                            || response.status() == StatusCode::TOO_MANY_REQUESTS
                            || response.status() == StatusCode::REQUEST_TIMEOUT) => {}
                Ok(response) => return Ok(response),
                Err(_) if attempt < 2 => {}
                Err(error) => {
                    return Err(std::io::Error::other(error))
                        .context("requesting Azure temporary directory");
                }
            }
            tokio::time::sleep(Duration::from_millis(250 << attempt)).await;
        }
        unreachable!("the final request always returns")
    }

    pub(super) async fn cleanup(
        &self,
        container: &str,
        root: &Path,
        cutoff: DateTime<Utc>,
        dry_run: bool,
        report: &mut DirectoryCleanupReport,
    ) -> Result<()> {
        let root = root.as_ref();
        let response = self
            .request(Method::HEAD, self.url(container, Some(root))?, None)
            .await?;
        if has_error(&response, StatusCode::NOT_FOUND, "PathNotFound") {
            return Ok(());
        }
        check_status(&response, "checking temporary directory")?;
        if header(&response, "x-ms-resource-type")? != "directory" {
            return Err(Error::InvalidData {
                reason: format!("Azure temporary root {root} is not a directory"),
            });
        }
        let mut candidates = Vec::new();
        if let Some(directory) = old_directory(
            root.to_string(),
            header(&response, "last-modified")?,
            header(&response, "etag")?.to_owned(),
            cutoff,
        )? {
            candidates.push(directory);
        }

        let mut continuation: Option<String> = None;
        loop {
            let mut url = self.url(container, None)?;
            {
                let mut query = url.query_pairs_mut();
                query.append_pair("resource", "filesystem");
                query.append_pair("recursive", "true");
                query.append_pair("directory", root);
                if let Some(token) = &continuation {
                    query.append_pair("continuation", token);
                }
            }
            let response = self.request(Method::GET, url, None).await?;
            if has_error(&response, StatusCode::NOT_FOUND, "PathNotFound") {
                return Ok(());
            }
            check_status(&response, "listing temporary directories")?;
            continuation = response
                .headers()
                .get("x-ms-continuation")
                .map(|value| value.to_str().map(str::to_owned))
                .transpose()
                .map_err(std::io::Error::other)?
                .filter(|value| !value.is_empty());
            let list: PathList = response
                .json()
                .await
                .map_err(std::io::Error::other)
                .context("parsing Azure directory listing")?;
            for entry in list.paths {
                if !entry
                    .name
                    .strip_prefix(root)
                    .is_some_and(|suffix| suffix.starts_with('/'))
                {
                    return Err(Error::InvalidData {
                        reason: format!("Azure returned a path outside temporary root {root}"),
                    });
                }
                if entry.is_directory.is_directory()? {
                    if let Some(directory) =
                        old_directory(entry.name, &entry.last_modified, entry.etag, cutoff)?
                    {
                        candidates.push(directory);
                    }
                }
            }
            if continuation.is_none() {
                break;
            }
        }
        candidates.sort_unstable_by(|a, b| b.0.len().cmp(&a.0.len()));
        report.candidates += candidates.len();
        if dry_run {
            return Ok(());
        }
        for (path, etag) in candidates {
            let mut url = self.url(container, Some(&path))?;
            url.query_pairs_mut().append_pair("recursive", "false");
            let response = self.request(Method::DELETE, url, Some(&etag)).await?;
            if response.status().is_success() {
                report.deleted += 1;
            } else if has_error(&response, StatusCode::NOT_FOUND, "PathNotFound") {
                report.already_missing += 1;
            } else if has_error(&response, StatusCode::CONFLICT, "DirectoryNotEmpty") {
                report.nonempty += 1;
            } else if has_error(
                &response,
                StatusCode::PRECONDITION_FAILED,
                "ConditionNotMet",
            ) {
                report.changed += 1;
            } else {
                check_status(&response, &format!("deleting temporary directory {path}"))?;
            }
        }
        Ok(())
    }
}

fn header<'a>(response: &'a Response, name: &str) -> Result<&'a str> {
    response
        .headers()
        .get(name)
        .ok_or_else(|| Error::InvalidData {
            reason: format!("Azure response missing {name} header"),
        })?
        .to_str()
        .map_err(std::io::Error::other)
        .context("reading Azure response header")
}

fn has_error(response: &Response, status: StatusCode, code: &str) -> bool {
    response.status() == status
        && response
            .headers()
            .get("x-ms-error-code")
            .is_some_and(|value| value == code)
}

fn check_status(response: &Response, operation: &str) -> Result<()> {
    if response.status().is_success() {
        return Ok(());
    }
    let code = response
        .headers()
        .get("x-ms-error-code")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");
    Err(Error::InvalidData {
        reason: format!(
            "{operation}: Azure returned HTTP {} ({code})",
            response.status()
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        extract::{OriginalUri, State},
        http::{HeaderMap, StatusCode as AxumStatus},
        response::{IntoResponse, Response as AxumResponse},
        routing::{any, get},
    };
    use std::sync::{Arc, Mutex};

    type Calls = Arc<Mutex<Vec<(String, String)>>>;

    async fn listing(OriginalUri(uri): OriginalUri) -> AxumResponse {
        let query = uri.query().unwrap();
        assert!(query.contains("recursive=true"));
        assert!(query.contains("directory=t%2F_tmp"));
        if query.contains("continuation=") {
            axum::Json(serde_json::json!({"paths": [
                    {"name":"t/_tmp/changed", "isDirectory":true, "lastModified":"Mon, 01 Jan 2024 00:00:00 GMT", "etag":"abc"},
                    {"name":"t/_tmp/new", "isDirectory":"true", "lastModified":"Tue, 02 Jan 2024 00:00:00 GMT", "etag":"abc"},
                    {"name":"t/_tmp/file", "lastModified":"Mon, 01 Jan 2024 00:00:00 GMT", "etag":"abc"}
                ]})).into_response()
        } else {
            ([
                    ("x-ms-continuation", "page+2"),
                ], axum::Json(serde_json::json!({"paths": [
                    {"name":"t/_tmp/empty", "isDirectory":"true", "lastModified":"Mon, 01 Jan 2024 00:00:00 GMT", "etag":"abc"},
                    {"name":"t/_tmp/nonempty", "isDirectory":"true", "lastModified":"Mon, 01 Jan 2024 00:00:00 GMT", "etag":"abc"},
                    {"name":"t/_tmp/missing", "isDirectory":"true", "lastModified":"Mon, 01 Jan 2024 00:00:00 GMT", "etag":"abc"}
                ]}))).into_response()
        }
    }

    async fn paths(
        State(calls): State<Calls>,
        method: Method,
        OriginalUri(uri): OriginalUri,
        headers: HeaderMap,
    ) -> AxumResponse {
        assert!(headers.contains_key("authorization"));
        if method == Method::HEAD {
            return (
                AxumStatus::OK,
                [
                    ("x-ms-resource-type", "directory"),
                    ("last-modified", "Mon, 01 Jan 2024 00:00:00 GMT"),
                    ("etag", "\"abc\""),
                ],
            )
                .into_response();
        }
        assert_eq!(method, Method::DELETE);
        assert_eq!(uri.query(), Some("recursive=false"));
        assert_eq!(headers["if-match"], "\"abc\"");
        let name = uri.path().to_string();
        calls
            .lock()
            .unwrap()
            .push((method.to_string(), name.clone()));
        if name.ends_with("nonempty") || name.ends_with("_tmp") {
            (
                AxumStatus::CONFLICT,
                [("x-ms-error-code", "DirectoryNotEmpty")],
            )
                .into_response()
        } else if name.ends_with("changed") {
            (
                AxumStatus::PRECONDITION_FAILED,
                [("x-ms-error-code", "ConditionNotMet")],
            )
                .into_response()
        } else if name.ends_with("missing") {
            (AxumStatus::NOT_FOUND, [("x-ms-error-code", "PathNotFound")]).into_response()
        } else {
            AxumStatus::OK.into_response()
        }
    }

    #[tokio::test]
    async fn cleans_only_old_directories_nonrecursively_and_handles_races() {
        let calls = Calls::default();
        let app = Router::new()
            .route("/lake", get(listing))
            .route("/lake/{*path}", any(paths))
            .with_state(calls.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut azure = AzureDirectories::new("test", "a2V5").unwrap();
        azure.endpoint = endpoint;
        let cutoff = "2024-01-02T00:00:00Z".parse().unwrap();
        let root = Path::from("t/_tmp");
        let mut dry = DirectoryCleanupReport::default();
        azure
            .cleanup("lake", &root, cutoff, true, &mut dry)
            .await
            .unwrap();
        assert_eq!(dry.candidates, 5);
        assert!(calls.lock().unwrap().is_empty());
        let mut report = DirectoryCleanupReport::default();
        azure
            .cleanup("lake", &root, cutoff, false, &mut report)
            .await
            .unwrap();
        assert_eq!(report.candidates, 5);
        assert_eq!(report.deleted, 1);
        assert_eq!(report.nonempty, 2);
        assert_eq!(report.changed, 1);
        assert_eq!(report.already_missing, 1);
        assert_eq!(calls.lock().unwrap().len(), 5);
        server.abort();
    }

    #[tokio::test]
    async fn unexpected_azure_errors_are_not_silenced() {
        let app = Router::new().fallback(|| async { AxumStatus::FORBIDDEN });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut azure = AzureDirectories::new("test", "a2V5").unwrap();
        azure.endpoint = endpoint;
        let error = azure
            .cleanup(
                "lake",
                &Path::from("t/_tmp"),
                Utc::now(),
                false,
                &mut DirectoryCleanupReport::default(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("403"));
        server.abort();
    }

    #[tokio::test]
    async fn transient_errors_are_retried_and_then_reported() {
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = Router::new().fallback({
            let count = count.clone();
            move || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    AxumStatus::SERVICE_UNAVAILABLE
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let mut azure = AzureDirectories::new("test", "a2V5").unwrap();
        azure.endpoint = endpoint;
        let error = azure
            .cleanup(
                "lake",
                &Path::from("t/_tmp"),
                Utc::now(),
                false,
                &mut DirectoryCleanupReport::default(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("503"));
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 3);
        server.abort();
    }
}
