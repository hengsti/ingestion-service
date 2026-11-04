use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use secrecy::{ExposeSecret, SecretString};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct TestInfluxConfig {
    url: String,
    org: String,
    bucket: String,
    token: SecretString,
}

fn test_influx_config() -> Result<Option<TestInfluxConfig>> {
    let names = [
        "INFLUX_TEST_URL",
        "INFLUX_TEST_ORG",
        "INFLUX_TEST_BUCKET",
        "INFLUX_TEST_TOKEN",
    ];
    let values = names.map(|name| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    });

    if values.iter().all(Option::is_none) {
        eprintln!("skipping: set all INFLUX_TEST_* variables to run the isolated InfluxDB test");
        return Ok(None);
    }

    let [url, org, bucket, token] = values;
    Ok(Some(TestInfluxConfig {
        url: url.context("INFLUX_TEST_URL must be set")?,
        org: org.context("INFLUX_TEST_ORG must be set")?,
        bucket: bucket.context("INFLUX_TEST_BUCKET must be set")?,
        token: SecretString::new(token.context("INFLUX_TEST_TOKEN must be set")?),
    }))
}

#[tokio::test]
async fn test_influx_same_timestamp_merges_field_sets_when_explicitly_configured() -> Result<()> {
    let Some(config) = test_influx_config()? else {
        return Ok(());
    };

    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let measurement = format!("ingest_timestamp_test_{}_{}", std::process::id(), unique);
    let run_tag = format!("run_{unique}");
    let timestamp_ms = i64::try_from(unique / 1_000_000)?;
    let start = DateTime::<Utc>::from_timestamp_millis(timestamp_ms - 60_000)
        .context("could not construct Influx query start time")?
        .to_rfc3339();
    let stop = DateTime::<Utc>::from_timestamp_millis(timestamp_ms + 60_000)
        .context("could not construct Influx query stop time")?
        .to_rfc3339();
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let base_url = config.url.trim_end_matches('/');
    let write_url = format!(
        "{base_url}/api/v2/write?org={}&bucket={}&precision=ms",
        urlencoding::encode(&config.org),
        urlencoding::encode(&config.bucket)
    );
    let query_url = format!(
        "{base_url}/api/v2/query?org={}",
        urlencoding::encode(&config.org)
    );

    for fields in ["first=1i", "second=true"] {
        client
            .post(&write_url)
            .header(
                "Authorization",
                format!("Token {}", config.token.expose_secret()),
            )
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(format!(
                "{measurement},run={run_tag} {fields} {timestamp_ms}"
            ))
            .send()
            .await?
            .error_for_status()
            .context("InfluxDB test write failed")?;
    }

    let flux = format!(
        "from(bucket: \"{}\") \
         |> range(start: time(v: \"{start}\"), stop: time(v: \"{stop}\")) \
         |> filter(fn: (r) => r._measurement == \"{measurement}\" and r.run == \"{run_tag}\") \
         |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")",
        config.bucket
    );
    let csv = client
        .post(&query_url)
        .header(
            "Authorization",
            format!("Token {}", config.token.expose_secret()),
        )
        .header("Content-Type", "application/vnd.flux")
        .body(flux)
        .send()
        .await?
        .error_for_status()
        .context("InfluxDB test query failed")?
        .text()
        .await?;

    let rows = csv
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect::<Vec<_>>();
    let header_index = rows
        .iter()
        .position(|line| {
            line.split(',').any(|column| column == "first")
                && line.split(',').any(|column| column == "second")
        })
        .context("InfluxDB query did not return both merged field columns")?;
    let header = rows[header_index].split(',').collect::<Vec<_>>();
    let first_index = header
        .iter()
        .position(|column| *column == "first")
        .context("InfluxDB query omitted the first field column")?;
    let second_index = header
        .iter()
        .position(|column| *column == "second")
        .context("InfluxDB query omitted the second field column")?;
    let row = rows
        .iter()
        .skip(header_index + 1)
        .find(|line| line.split(',').any(|column| column == run_tag))
        .context("InfluxDB query returned no merged point row")?
        .split(',')
        .collect::<Vec<_>>();

    assert_eq!(row.get(first_index), Some(&"1"));
    assert_eq!(row.get(second_index), Some(&"true"));
    Ok(())
}
