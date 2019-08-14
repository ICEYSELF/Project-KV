//! The configuration info for a server
//!
//! a `KVServerConfig` can be constructed with either default value (for test use) or a
//! `clap::ArgMatches` (for CLI program use). The configuration can then be passed and used.

use clap::{ArgMatches, value_t};
use log::info;

const DEFAULT_FILENAME: &str = "data.kv";
const DEFAULT_LISTEN_PORT: u16 = 1926;
const DEFAULT_THREADS: u16 = 4;

/// Configuration info needed for running a KV server, see its field for futher information
pub struct KVServerConfig {
    pub db_file: String,
    pub listen_port: u16,
    pub threads: u16
}

impl KVServerConfig {
    /// Creates a `KVServerConfig` using default value
    pub fn from_default() -> Self {
        KVServerConfig {
            db_file: DEFAULT_FILENAME.to_owned(),
            listen_port: DEFAULT_LISTEN_PORT,
            threads: DEFAULT_THREADS }
    }

    /// Creates a `KVServerConfig` from command line arguments (`clap::ArgMatches`).
    ///
    /// This function requires three formal parameters from commandline: `dbfile` of type `String`
    /// for database file name, `port` of type `u16` for listening port and `threads` of type `u16`
    /// for thread pool size. If there are some formal parameters missing from the command line
    /// argument, or the arguments provided from command line does not satisfy the type
    /// requirements, this function will generate some `Info` level log, and use default values to
    /// fill in these parameters.
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
