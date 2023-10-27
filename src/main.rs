use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize, Deserialize, Debug)]
struct TopQuery {
    rows_written: i32,
    rows_read: i32,
    query: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct NamespaceStats {
    rows_read_count: u64,
    rows_written_count: u64,
    storage_bytes_used: u64,
    write_requests_delegated: u64,
    replication_index: u64,
    top_queries: Vec<TopQuery>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    block_reads: bool,
    block_writes: bool,
    block_reason: Option<String>,
    max_db_size: Option<String>,
}


struct Server {
    base_url: String,
    client: reqwest::blocking::Client,
}

impl Server {
    fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: reqwest::blocking::Client::new(),
        }
    }

    fn create_namespace(&self, namespace: &str) -> Option<()> {
        let url =
            format!("{}/v1/namespaces/{}/create", self.base_url, namespace);
        let res = self.client.post(url).json(&json!({})).send().ok()?;

        if !res.status().is_success() {
            println!("Res: {:#?}", res.json::<serde_json::Value>());
            return None;
        }

        Some(())
    }

    fn delete_namespace(&self, namespace: &str) -> Option<()> {
        let url = format!("{}/v1/namespaces/{}", self.base_url, namespace);
        let res = self.client.delete(url).send().ok()?;

        if !res.status().is_success() {
            println!("Res: {:#?}", res.json::<serde_json::Value>());
            return None;
        }

        Some(())
    }

    fn fork_namespace(&self, from: &str, to: &str) -> Option<()> {
        let url =
            format!("{}/v1/namespaces/{}/fork/{}", self.base_url, from, to);
        let res = self.client.post(url).send().ok()?;

        if !res.status().is_success() {
            println!("Res: {:#?}", res.json::<serde_json::Value>());
            return None;
        }

        Some(())
    }

    fn namespace_stats(&self, namespace: &str) -> Option<NamespaceStats> {
        let url =
            format!("{}/v1/namespaces/{}/stats", self.base_url, namespace);
        let res = self.client.get(url).send().ok()?;

        if !res.status().is_success() {
            println!("Res: {:#?}", res.json::<serde_json::Value>());
            return None;
        }

        let res = res.json::<NamespaceStats>().ok()?;

        Some(res)
    }

    fn get_namespace_config(&self, namespace: &str) -> Option<Config> {
        let url =
            format!("{}/v1/namespaces/{}/config", self.base_url, namespace);
        let res = self.client.get(url).send().ok()?;

        if !res.status().is_success() {
            println!("Res: {:#?}", res.json::<serde_json::Value>());
            return None;
        }

        // println!("Res: {:#?}", res.json::<serde_json::Value>());
        let res = res.json::<Config>().ok()?;

        Some(res)
    }

    fn set_namespace_config(&self, namespace: &str, config: &Config) -> Option<()> {
        let url =
            format!("{}/v1/namespaces/{}/config", self.base_url, namespace);
        let res = self.client.post(url).json(config).send().ok()?;

        if !res.status().is_success() {
            println!("Res: {:#?}", res.json::<serde_json::Value>());
            return None;
        }

        None
    }

        // .route(
        //     "/v1/namespaces/:namespace/config",
        //     get(handle_get_config).post(handle_post_config),
        // )
        //
        // .route("/v1/diagnostics", get(handle_diagnostics))
}

fn main() {
    let server = Server::new("http://127.0.0.1:8081".to_string());
    let stats = server.namespace_stats("db1");
    println!("{:#?}", stats);

    let mut config = server.get_namespace_config("db1").unwrap();
    println!("Config: {:#?}", config);

    config.max_db_size = Some("500.0 PB".to_string());

    server.set_namespace_config("db1", &config);

    let config = server.get_namespace_config("db1").unwrap();
    println!("Config: {:#?}", config);

    // server.fork_namespace("db1", "db3");
    // server.delete_namespace("db3");
    // server.create_namespace("db3");
}
