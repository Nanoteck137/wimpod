use clap::{Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::io::Write;

#[derive(Clone, ValueEnum, Debug)]
enum PrintFormat {
    Normal,
    Json,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    base_url: String,

    #[arg(value_enum, long, short, default_value_t = PrintFormat::Normal)]
    format: PrintFormat,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Stats {
        namespace: String,
        #[arg(long, short)]
        include_top_queries: bool,
    },

    CreateNamespace {
        name: String,
    },

    DeleteNamespace {
        name: String,
    },

    Fork {
        from: String,
        to: String,
    },
}

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

#[derive(Serialize, Deserialize, Debug)]
struct ServerError {
    error: String,
}

impl Server {
    fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: reqwest::blocking::Client::new(),
        }
    }

    fn create_namespace(&self, namespace: &str) -> Result<(), ServerError> {
        let url =
            format!("{}/v1/namespaces/{}/create", self.base_url, namespace);
        let res = self.client.post(url).json(&json!({})).send().unwrap();

        if !res.status().is_success() {
            let error = res.json::<ServerError>().unwrap();
            return Err(error);
        }

        Ok(())
    }

    fn delete_namespace(&self, namespace: &str) -> Result<(), ServerError> {
        let url = format!("{}/v1/namespaces/{}", self.base_url, namespace);
        let res = self.client.delete(url).send().unwrap();

        if !res.status().is_success() {
            let error = res.json::<ServerError>().unwrap();
            return Err(error);
        }

        Ok(())
    }

    fn fork_namespace(&self, from: &str, to: &str) -> Result<(), ServerError> {
        let url =
            format!("{}/v1/namespaces/{}/fork/{}", self.base_url, from, to);
        let res = self.client.post(url).send().unwrap();

        if !res.status().is_success() {
            let error = res.json::<ServerError>().unwrap();
            return Err(error);
        }

        Ok(())
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

    fn set_namespace_config(
        &self,
        namespace: &str,
        config: &Config,
    ) -> Option<()> {
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

fn print_stats(stats: &NamespaceStats, format: PrintFormat) {
    match format {
        PrintFormat::Normal => {
            println!("Rows Read: {}", stats.rows_read_count);
            println!("Rows Written: {}", stats.rows_written_count);
            println!("Storage Used (B): {}", stats.storage_bytes_used);
            println!(
                "Write Requests Delegated: {}",
                stats.write_requests_delegated
            );
            println!("Replication Index: {}", stats.replication_index);
            if !stats.top_queries.is_empty() {
                println!("Top Queries (RR = Rows Read : RW = Rows Written):");
                for (i, query) in stats.top_queries.iter().enumerate() {
                    println!(
                        "{}: RR: {} RW: {} Query: {}",
                        i, query.rows_read, query.rows_written, query.query
                    );
                }
            }
        }

        PrintFormat::Json => {
            let j = serde_json::to_string_pretty(
                &json!({ "success": true, "stats": &stats }),
            )
            .expect("Failed to convert stats to json");
            write_str(&j);
        }
    }
}

fn write_str(s: &str) {
    let stdout = std::io::stdout();
    let mut lock = stdout.lock();

    // NOTE(patrik): Just exit when an error occurs because
    // I got a problem with broken pipes when piping to an
    // program that doesn't exist
    if let Err(_) = writeln!(lock, "{}", s) {
        std::process::exit(0);
    }
}

fn print_server_error(err: ServerError, format: PrintFormat) {
    match format {
        PrintFormat::Normal => {
            eprintln!("Error: {}", err.error);
        }

        PrintFormat::Json => {
            let j = serde_json::to_string_pretty(&err)
                .expect("Failed to convert err to json");

            write_str(&j);
        }
    }

    std::process::exit(-1);
}

fn print_success(format: PrintFormat) {
    match format {
        PrintFormat::Normal => {
            println!("Success");
        },
        PrintFormat::Json => {
            let j = serde_json::to_string_pretty(&json!({ "success": true }))
                .expect("Failed to convert result to json");
            write_str(&j);
        }
    }
}

fn main() {
    let args = Args::parse();

    let server = Server::new(args.base_url);

    match args.command {
        Commands::Stats {
            namespace,
            include_top_queries,
        } => {
            let mut stats = server
                .namespace_stats(&namespace)
                .expect("Failed to retrive namespace stats");

            if !include_top_queries {
                stats.top_queries.clear();
            }

            print_stats(&stats, args.format);
        }

        Commands::CreateNamespace { name } => {
            match server.create_namespace(&name) {
                Ok(_) => print_success(args.format),
                Err(e) => print_server_error(e, args.format),
            }
        }

        Commands::DeleteNamespace { name } => {
            match server.delete_namespace(&name) {
                Ok(_) => print_success(args.format),
                Err(e) => print_server_error(e, args.format),
            }
        }

        Commands::Fork { from, to } => {
            match server.fork_namespace(&from, &to) {
                Ok(_) => print_success(args.format),
                Err(e) => print_server_error(e, args.format),
            }
        }
    }

    // let stats = server.namespace_stats("db1");
    // println!("{:#?}", stats);
    //
    // let mut config = server.get_namespace_config("db1").unwrap();
    // println!("Config: {:#?}", config);
    //
    // config.max_db_size = Some("500.0 PB".to_string());
    //
    // server.set_namespace_config("db1", &config);
    //
    // let config = server.get_namespace_config("db1").unwrap();
    // println!("Config: {:#?}", config);

    // server.fork_namespace("db1", "db3");
    // server.delete_namespace("db3");
    // server.create_namespace("db3");
}
