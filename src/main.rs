use std::sync::Arc;

use libnaadandb::{
    server::{NaadanServer, ServerConfig},
    storage::storage_engine::NaadanStorageEngine,
};
use log::info;

#[tokio::main]
async fn main() {
    env_logger::init();
    info!("Configuring naadanDB server");
    // TODO: Read comfig from file or command line.
    let dbserver_config = ServerConfig { port: 2222 };

    let server_instance = NaadanServer::setup(
        dbserver_config,
        Arc::new(Box::new(NaadanStorageEngine::init(4098))),
    );

    // Start the DB server
    info!("Starting naadanDB server");
    server_instance.start().await;
}
