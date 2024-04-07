use libnaadandb::server::{NaadanServer, ServerConfig};
use log::{info};

#[tokio::main]
async fn main() {
    env_logger::init();
    info!("Configuring naadanDB server");
    // TODO: Read comfig from file or command line.
    let dbserver_config = ServerConfig { port: 2222 };

    let server_instance = NaadanServer::setup(dbserver_config);

    // Start the DB server
    info!("Starting naadanDB server");
    server_instance.start().await;
}
