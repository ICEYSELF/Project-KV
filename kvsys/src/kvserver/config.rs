use clap::ArgMatches;
use log::{info, warn};

pub struct KVServerConfig {
    pub db_file: String,
    pub listen_port: u16,
    pub threads: u16
}

impl KVServerConfig {
    pub fn from_arg_matches(matches: ArgMatches) -> Self {
        let db_file =
            matches
                .value_of("dbfile")
                .unwrap_or_else(|| {
                    info!("no dbfile provided from commandline, using default file name 'data.kv'");
                    "data.kv"
                });
        let listen_port =
            matches
                .value_of("port")
                .unwrap_or_else(|| {
                    info!("no port provided from commandline, using default port 1926");
                    "1926"
                })
                .parse().unwrap_or_else(|_| {
                    warn!("port provided from commandline was invalid, using default port 1926");
                    1926
                });
        let threads =
            matches
                .value_of("threads")
                .unwrap_or_else(|| {
                    info!("no thread pool size provided from commandline, using default value 16");
                    "16"
                })
                .parse().unwrap_or_else(|_| {
                    warn!("thread pool size provided from commandline was invalid, using default value 16");
                    16
                });
        KVServerConfig { db_file: db_file.to_owned(), listen_port, threads }
    }
}
