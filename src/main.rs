use dkplus::tags::Updates;
use dkplus::{Error, Result};
use serde_json::Value;
use std::collections::BTreeMap;
use std::error::Error as _;
use std::process::ExitCode;

const USAGE: &str = "\
Usage:
  dkplus <postgres-connection-string> <schema.table> <key> <value> [<key> <value> ...]
  dkplus <postgres-connection-string> --file <file.json>

Keys may omit the dkplus. prefix. Scalar values are strings; list settings
(sort_cols, unique_by_cols) use JSON arrays. Use null for nullable settings.
File format: {\"schema.table\": {\"time_column\": \"observed\", \"timezone\": \"UTC\"}}
All changes are validated and committed together in one catalog snapshot.";

enum Input {
    File(String),
    Inline(Updates),
}

fn parse(args: &[String]) -> Result<(&str, Input)> {
    if args.len() < 3 {
        return Err(Error::InvalidInput {
            reason: USAGE.into(),
        });
    }
    if args[1] == "--file" {
        if args.len() != 3 {
            return Err(Error::InvalidInput {
                reason: format!("--file requires exactly one file name\n\n{USAGE}"),
            });
        }
        return Ok((&args[0], Input::File(args[2].clone())));
    }
    if args.len() < 4 || args.len() % 2 != 0 {
        return Err(Error::InvalidInput {
            reason: format!("provide a table and complete key/value pairs\n\n{USAGE}"),
        });
    }
    let mut settings = BTreeMap::new();
    for pair in args[2..].chunks_exact(2) {
        let name = pair[0].strip_prefix("dkplus.").unwrap_or(&pair[0]);
        let value = if ["sort_cols", "unique_by_cols"].contains(&name) {
            serde_json::from_str(&pair[1]).map_err(|source| Error::Json {
                operation: format!("{name} requires a JSON array or null"),
                source,
            })?
        } else if pair[1] == "null" && name != "time_column" && name != "timezone" {
            Value::Null
        } else {
            Value::String(pair[1].clone())
        };
        if settings.insert(pair[0].clone(), value).is_some() {
            return Err(Error::InvalidInput {
                reason: format!("duplicate setting {:?}", pair[0]),
            });
        }
    }
    Ok((
        &args[0],
        Input::Inline(BTreeMap::from([(args[1].clone(), settings)])),
    ))
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error}");
            // Display already includes the immediate source; print any deeper causes.
            let mut cause = error.source().and_then(|source| source.source());
            while let Some(source) = cause {
                eprintln!("Caused by: {source}");
                cause = source.source();
            }
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        println!("{USAGE}");
        return Ok(());
    }
    let (connection, input) = parse(&args)?;
    let updates = match input {
        Input::Inline(updates) => updates,
        Input::File(path) => {
            let file = std::fs::File::open(&path).map_err(|source| Error::Io {
                operation: format!("opening {path:?}"),
                source,
            })?;
            serde_json::from_reader(file).map_err(|source| Error::Json {
                operation: format!("reading policy JSON from {path:?}"),
                source,
            })?
        }
    };
    let postgres = connection.parse().map_err(|source| Error::Catalog {
        operation: "invalid Postgres connection string".into(),
        source,
    })?;
    let snapshot = dkplus::tags::update(&postgres, updates).await?;
    println!("Policy applied; catalog snapshot {snapshot}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_both_invocation_forms() {
        let inline = args(&[
            "connection",
            "ciso.rtprices",
            "time_column",
            "utcbegin",
            "row_group_col",
            "node",
        ]);
        let (_, Input::Inline(updates)) = parse(&inline).unwrap() else {
            panic!()
        };
        assert_eq!(updates["ciso.rtprices"]["time_column"], "utcbegin");
        assert_eq!(updates["ciso.rtprices"]["row_group_col"], "node");
        let file = args(&["connection", "--file", "somefile.json"]);
        assert!(matches!(parse(&file).unwrap().1, Input::File(path) if path == "somefile.json"));
    }

    #[test]
    fn parses_lists_and_null_and_rejects_incomplete_input() {
        let input = args(&[
            "connection",
            "a.b",
            "sort_cols",
            "[\"node\",\"time\"]",
            "row_group_col",
            "null",
        ]);
        let (_, Input::Inline(updates)) = parse(&input).unwrap() else {
            panic!()
        };
        assert_eq!(
            updates["a.b"]["sort_cols"],
            serde_json::json!(["node", "time"])
        );
        assert!(updates["a.b"]["row_group_col"].is_null());
        for values in [
            vec![],
            vec!["connection", "a.b"],
            vec!["connection", "a.b", "key"],
            vec!["connection", "--file", "file", "extra"],
            vec!["connection", "a.b", "sort_cols", "node,time"],
            vec!["connection", "a.b", "time_column", "a", "time_column", "b"],
        ] {
            assert!(parse(&args(&values)).is_err(), "{values:?}");
        }
    }
}
