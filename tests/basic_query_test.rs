use std::sync::Arc;

use libnaadandb::{
    query::{query_engine::NaadanQueryEngine, NaadanQuery},
    storage::storage_engine::NaadanStorageEngine,
    storage::StorageEngine,
};
use tokio::sync::Mutex;

async fn process_query(query: String, storage: Arc<Mutex<Box<dyn StorageEngine>>>) {
    let sql_query = NaadanQuery::init(query).unwrap();

    // Init a new query engine instance with reference to the global shared storage engine
    let query_engine = NaadanQueryEngine::init(storage).await;

    // Process the sql query.
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

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_crud() {
    let storage: Arc<Mutex<Box<dyn StorageEngine>>> =
        Arc::new(Mutex::new(Box::new(NaadanStorageEngine::init(100))));

    let mut query = "create table test1 (id int, name varchar(255))";
    process_query(query.to_string(), storage.clone()).await;

    query = "insert into test1 values(1,'rom'),(2,'rob')";
    process_query(query.to_string(), storage.clone()).await;

    query = "select * from test1";
    process_query(query.to_string(), storage.clone()).await;
}
