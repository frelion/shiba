use core::str::FromStr;
use std::time::Duration;

use libpq::connection::Info;
use postgres::{Client, Config, NoTls};

use crate::IngressError;

pub(crate) fn open_apply(
    conninfo: &str,
    statement_timeout: Duration,
) -> Result<(Client, String), IngressError> {
    let (mut config, database) = parse_apply(conninfo)?;
    config.application_name("shiba-governed-apply");
    let mut client = config.connect(NoTls)?;
    let millis = statement_timeout.as_millis().to_string();
    client.query_one(
        "SELECT pg_catalog.set_config('statement_timeout', $1, false)",
        &[&format!("{millis}ms")],
    )?;
    Ok((client, database))
}

fn parse_apply(conninfo: &str) -> Result<(Config, String), IngressError> {
    let config = Config::from_str(conninfo)?;
    if config.get_connect_timeout().is_none_or(Duration::is_zero) {
        return Err(IngressError::Governance(
            "apply connect_timeout is required",
        ));
    }
    let database = config
        .get_dbname()
        .ok_or(IngressError::Governance("apply dbname is required"))?
        .to_owned();
    Ok((config, database))
}

pub(crate) fn replication_database(conninfo: &str) -> Result<String, IngressError> {
    let options = Info::from(conninfo).map_err(IngressError::Libpq)?;
    let value = |keyword: &str| {
        options
            .iter()
            .find(|option| option.keyword == keyword)
            .and_then(|option| option.val.as_deref())
    };
    value("connect_timeout")
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or(IngressError::Governance(
            "replication connect_timeout is required",
        ))?;
    if value("replication") != Some("database") {
        return Err(IngressError::Governance("replication=database is required"));
    }
    value("dbname")
        .map(str::to_owned)
        .ok_or(IngressError::Governance("replication dbname is required"))
}

#[cfg(test)]
mod tests {
    use super::parse_apply;

    #[test]
    fn apply_conninfo_requires_explicit_database_and_timeout() {
        assert!(parse_apply("host=/tmp dbname=test connect_timeout=1").is_ok());
        assert!(parse_apply("host=/tmp dbname=test").is_err());
        assert!(parse_apply("host=/tmp connect_timeout=1").is_err());
    }
}
