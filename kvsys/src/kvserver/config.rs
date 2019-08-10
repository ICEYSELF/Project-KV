use clap::{ArgMatches, value_t};
use log::info;

const DEFAULT_FILENAME: &str = "data.kv";
const DEFAULT_LISTEN_PORT: u16 = 1926;
const DEFAULT_THREADS: u16 = 4;

pub struct KVServerConfig {
    pub db_file: String,
    pub listen_port: u16,
    pub threads: u16
}

impl KVServerConfig {
    pub fn from_default() -> Self {
        KVServerConfig {
            db_file: DEFAULT_FILENAME.to_owned(),
            listen_port: DEFAULT_LISTEN_PORT,
            threads: DEFAULT_THREADS }
    }

    pub fn from_arg_matches(matches: ArgMatches) -> Self {
        let db_file = value_t!(matches, "dbfile", String).unwrap_or_else(|_| {
                info!("no valid dbfile provided from commandline, using default file name '{}'", DEFAULT_FILENAME);
                DEFAULT_FILENAME.to_owned()
            });
        let listen_port = value_t!(matches, "port", u16).unwrap_or_else(|_| {
                info!("no valid listen port provided from commandline, using default port {}", DEFAULT_LISTEN_PORT);
                DEFAULT_LISTEN_PORT
            });
        let threads = value_t!(matches, "threads", u16).unwrap_or_else(|_| {
                info!("no valid thread pool size provided from commandline, using default size {}", DEFAULT_THREADS);
                DEFAULT_THREADS
            });
        KVServerConfig { db_file, listen_port, threads }
    }
}
