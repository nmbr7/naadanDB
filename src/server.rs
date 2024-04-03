use std::{
    io::{BufRead, BufReader},
    str::from_utf8,
};

use log::{debug, error, info, trace};
use rand::Rng;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

use std::sync::Arc;

use crate::{
    catalog::NaadanCatalog,
    query_engine::{self, NaadanQuery, NaadanQueryEngine},
    server,
    storage_engine::{self, OurStorageEngine, RowData, StorageEngine},
};

#[derive(Debug)]
pub struct ServerConfig {
    pub port: u16,
}

#[derive(Debug)]
pub struct NaadanServer {
    pub config: ServerConfig,
    pub storage_engine: Arc<Mutex<Box<dyn StorageEngine + Send>>>,
}

impl NaadanServer {
    pub fn setup(server_config: ServerConfig) -> Self {
        NaadanServer {
            config: server_config,
            storage_engine: Arc::new(Mutex::new(Box::new(OurStorageEngine::init(1024)))),
        }
    }

    pub async fn start(self: Self) {
        trace!("Starting naadanDB server");

        let ip_port = format!("0.0.0.0:{}", self.config.port);
        let listener = TcpListener::bind(ip_port).await.unwrap();

        let server_instance = Arc::new(self);

        let mut rng = rand::thread_rng();

        // let n = rng.gen_range(0..100);

        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let n = rng.gen_range(0..100);
            let server_instance_clone = server_instance.clone();
            tokio::spawn(async move {
                match Self::process_request(server_instance_clone, socket, n).await {
                    Ok(()) => debug!("Query successfully completed"),
                    Err(_) => error!("Query execution failed."),
                }
            });
        }
    }

    async fn process_request(
        server_instance: Arc<NaadanServer>,
        mut socket: TcpStream,
        _n: usize,
    ) -> Result<(), bool> {
        info!("Got new request from IP: {:?}", socket.peer_addr());

        let buffer: &mut Vec<u8> = &mut Vec::new();
        let mut result = vec![];
        let sql_statement = match get_query_from_request(&mut socket, buffer).await {
            Ok(value) => value,
            Err(value) => return Err(value),
        };

        debug!("Read query data: {}", sql_statement);

        // Init and parse the query string to AST
        let sql_query = match NaadanQuery::init(sql_statement.to_string()) {
            Ok(res) => res,
            Err(err) => {
                let _ = socket.write(err.to_string().as_bytes()).await;
                return Err(false);
            }
        };

        {
            // Create a reference to the shared storage engine
            let storage_engine_instance = server_instance.storage_engine.clone();
            let query_engine = NaadanQueryEngine::init(storage_engine_instance).await;

            // Create logical plan for the query from the AST
            let lp_result = query_engine.prepare_logical_plan(&sql_query).await;

            let logical_plan = match lp_result {
                Ok(val) => val,
                Err(err) => {
                    socket.write(err.to_string().as_bytes()).await.unwrap();
                    return Err(false);
                }
            };
            debug!("Logical Plan is : {:?}", logical_plan);

            // Prepare the physical plan
            let pp_result = query_engine.prepare_exec_plan(&logical_plan);
            let physical_plan = match pp_result {
                Ok(val) => val,
                Err(err) => {
                    socket.write(err.to_string().as_bytes()).await.unwrap();
                    return Err(false);
                }
            };

            // Execute the queries using the physical plan
            let res = query_engine.execute(physical_plan);
            match res {
                Ok(val) => {
                    result = val;
                }
                Err(err) => result = format!("Exec error {:?}", err).into(),
            }
        }

        socket.write(&result).await.unwrap();

        Ok(())
    }

    pub fn stop(self: Self) {
        trace!("Stopping naadanDB server")
    }
}

async fn get_query_from_request<'a>(
    socket: &'a mut TcpStream,
    buffer: &'a mut Vec<u8>,
) -> Result<&'a str, bool> {
    let mut count = 0;
    loop {
        let bytes = socket.read_buf(buffer).await.unwrap();
        if 0 == bytes {
            if buffer.is_empty() {
                return Err(true);
            } else {
                return Err(false);
            }
        }

        count += bytes;

        //debug!("{:?}", from_utf8(buffer));
        if buffer.as_slice()[(count - 4)..count] == b"EOF\n".to_vec() {
            break;
        }
    }

    Ok(from_utf8(&buffer[..(count - 4)]).unwrap())
}
