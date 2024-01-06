// TODO: setup a TCP server to accept DB connection and handle queries

pub struct ServerConfig {
    port: u16,
}

pub struct NaadanServer {
    config: ServerConfig,

}

impl NaadanServer {
    pub fn setup(serverConfig: ServerConfig) -> Self{
        NaadanServer {
            config: serverConfig
        }
    }

    pub fn start() {
        // TODO: start TCP server
    }

    pub fn stop() {}
}
