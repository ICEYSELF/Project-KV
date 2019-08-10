use clap::{Arg, App};
use kvsys::kvserver::{KVServerConfig, run_server};
use env_logger;

fn main() {
    env_logger::init();

    let matches = App::new("Project-KV Server Program")
        .version("0.1")
        .author("ICEY <icey@icey.tech>")
        .about("The official server program making use of Project-KV kvstorage library")
        .arg(Arg::with_name("port")
            .short("p")
            .long("port")
            .value_name("PORT")
            .help("Choose the port the server should listen to")
            .takes_value(true))
        .arg(Arg::with_name("dbfile")
            .short("f")
            .long("filename")
            .value_name("FILE")
            .help("Choose the file the server should write to or read from")
            .takes_value(true))
        .get_matches();

    let config = KVServerConfig::from_arg_matches(matches);

    run_server(config);
}
