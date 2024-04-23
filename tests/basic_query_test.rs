use std::sync::Arc;

use libnaadandb::{
    query::{query_engine::NaadanQueryEngine, NaadanQuery},
    storage::storage_engine::NaadanStorageEngine,
    storage::StorageEngine,
};
use tokio::{process::Command, sync::Mutex};

type ArcStorageEngine = Arc<Mutex<Box<dyn StorageEngine>>>;

async fn clean_db_files() {
    Command::new("rm")
        .args(&["/tmp/DB_*"])
        .status()
        .await
        .unwrap();
}

fn create_storage_instance() -> ArcStorageEngine {
    let storage: ArcStorageEngine = Arc::new(Mutex::new(Box::new(NaadanStorageEngine::init(100))));

    storage
}

async fn reset_storage_and_process_queries(queries: &[&str]) {
    clean_db_files().await;
    let storage = create_storage_instance();
    load_db_data(storage.clone()).await;

    process_queries(queries, storage).await;
}

async fn process_queries(queries: &[&str], storage: ArcStorageEngine) {
    for query in queries {
        process_query(query.to_string(), storage.clone()).await;
    }
}

async fn process_query(query: String, storage: ArcStorageEngine) {
    // Parse the provided SQL query.
    let sql_query = NaadanQuery::init(query).unwrap();

    // Init a new query engine instance with reference to the global shared storage engine.
    let query_engine = NaadanQueryEngine::init(storage).await;

    // Process the sql query Logical_Plan -> Physical_Plan -> Execute.
    let query_results = query_engine.process_query(sql_query).await;

    println!("********************************************");

    for query_result in query_results {
        match query_result {
            Ok(val) => println!("{}", val.to_string()),
            Err(err) => println!("Query execution failed: {}", err),
        }
    }
    println!("********************************************");
}

/// Load the base setup data in the DB
async fn load_db_data(storage: ArcStorageEngine) {
    let queries = [
        "Create table test1 (id int, name varchar(255))",
        "Insert into test1 values(1,'rom'),(2,'rob')",
    ];

    for query in queries {
        process_query(query.to_string(), storage.clone()).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_crud() {
    clean_db_files().await;

    let storage = create_storage_instance();

    let queries = [
        "Create table test1 (id int, name varchar(255))",
        "Insert into test1 values(1,'rom'),(2,'rob')",
        "Select * from test1",
    ];

    process_queries(queries.as_slice(), storage).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update() {
    let queries = [
        "update test1 set name = 'Tommy'",
        "Select * from test1",
        "Select * from test1",
        "update test1 set id = 4",
        "Select * from test1",
    ];

    reset_storage_and_process_queries(queries.as_slice()).await;
}
