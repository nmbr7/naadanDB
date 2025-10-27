use std::{fs::File, io::Write, sync::Arc};

use libnaadandb::{
    query::{query_engine::NaadanQueryEngine, NaadanQuery},
    server::SessionContext,
    storage::storage_engine::NaadanStorageEngine,
    transaction::TransactionManager,
};
use tokio::process::Command;

type ArcStorageEngine = Arc<Box<NaadanStorageEngine>>;

async fn clean_db_files() {
    Command::new("rm")
        .args(&["/tmp/DB_*"])
        .status()
        .await
        .unwrap();
}

fn create_storage_instance() -> ArcStorageEngine {
    let storage: ArcStorageEngine = Arc::new(Box::new(NaadanStorageEngine::init(100)));

    storage
}

async fn process_queries(
    queries: &[&str],
    transaction_manager: Arc<Box<TransactionManager<NaadanStorageEngine>>>,
) {
    let mut session_context = SessionContext::new();
    for query in queries {
        process_query(
            &mut session_context,
            query.to_string(),
            transaction_manager.clone(),
        )
        .await;
    }
}

async fn process_query(
    session_context: &mut SessionContext,
    query: String,
    transaction_manager: Arc<Box<TransactionManager<NaadanStorageEngine>>>,
) {
    let mut file = File::options()
        .create(true)
        .write(true)
        .append(true)
        .open("/tmp/Naadan_db_test.log")
        .unwrap();

    let mut file_log_string: String;

    // Parse the provided SQL query.
    let sql_query = NaadanQuery::init(query.clone()).unwrap();
    // Init a new query engine instance with reference to the global shared storage engine.
    let query_engine = NaadanQueryEngine::init(transaction_manager).await;

    file_log_string = format!("Started Query: [{}]", query,);

    file.write_all(format!("{}\n", file_log_string).as_bytes())
        .unwrap();
    file.flush().unwrap();

    // Process the sql query Logical_Plan -> Physical_Plan -> Execute.
    let query_results = query_engine.process_query(session_context, sql_query).await;

    for query_result in query_results {
        match query_result {
            Ok(val) => {
                //query.truncate(usize::pow(2, 8));
                file_log_string = format!(
                    "Query: [{}]..... execution succeeded with result: {}",
                    query,
                    val.to_string()
                );
            }
            Err(err) => {
                file_log_string = format!("Query: [{}] execution failed: {} ", query, err);
            }
        }

        file.write_all(format!("Finished {}\n", file_log_string).as_bytes())
            .unwrap();
        file.flush().unwrap();
    }
}

/// Load the base setup data in the DB
async fn load_db_data_batch(
    transaction_manager: Arc<Box<TransactionManager<NaadanStorageEngine>>>,
) {
    let mut queries: Vec<String> =
        vec!["Create table test1 (id int, ii int, name varchar, b int)".to_string()];

    let mut val: Vec<String> = vec![];
    for no in 1..=10_000 {
        val.push(format!(
            "({},{},'{}Test',1234)",
            no,
            no + 1,
            (no % 10).to_string()
        ));
    }

    queries.push(format!(
        "Insert into test1 (id, ii, name, b) values{}",
        val.join(",")
    ));
    let str_array: Vec<&str> = queries.iter().map(|s| s.as_str()).collect();

    process_queries(str_array.as_slice(), transaction_manager.clone()).await;
}

/// Load the base setup data in the DB
async fn load_db_data_batch_with_size(
    count: usize,
    transaction_manager: Arc<Box<TransactionManager<NaadanStorageEngine>>>,
) {
    let mut queries: Vec<String> =
        vec!["Create table test1 (id int, score int, name varchar, rate int)".to_string()];

    let mut val: Vec<String> = vec![];
    for no in 1..=count {
        val.push(format!(
            "({},{},'{}',1234)",
            no,
            no + 1,
            (no % 10).to_string()
        ));
    }

    queries.push(format!(
        "Insert into test1 (id,score,name,rate) values{}",
        val.join(",")
    ));
    let str_array: Vec<&str> = queries.iter().map(|s| s.as_str()).collect();

    process_queries(str_array.as_slice(), transaction_manager.clone()).await;
}

async fn load_db_data_seq(transaction_manager: Arc<Box<TransactionManager<NaadanStorageEngine>>>) {
    let mut queries: Vec<String> = vec!["Create table test1 (id int, name varchar)".to_string()];

    for no in 1..200 {
        queries.push(format!("Insert into test1 (id,name) values({},'ro')", no));
    }

    let str_array: Vec<&str> = queries.iter().map(|s| s.as_str()).collect();

    process_queries(str_array.as_slice(), transaction_manager.clone()).await;
}

/// Load data for JOIN test scenarios with two related tables
async fn load_join_test_data(
    transaction_manager: Arc<Box<TransactionManager<NaadanStorageEngine>>>,
) {
    let mut queries: Vec<String> = vec![
        "Create table users (id int, name varchar, department_id int)".to_string(),
        "Create table departments (id int, dept_name varchar, location varchar)".to_string(),
    ];

    // Insert department data
    for dept_id in 1..=5 {
        queries.push(format!(
            "Insert into departments (id, dept_name, location) values({}, 'Dept{}', 'Location{}')",
            dept_id, dept_id, dept_id
        ));
    }

    // Insert user data with foreign key references to departments
    for user_id in 1..=20 {
        let dept_id = ((user_id - 1) % 5) + 1; // Distribute users across departments
        queries.push(format!(
            "Insert into users (id, name, department_id) values({}, 'User{}', {})",
            user_id, user_id, dept_id
        ));
    }

    let str_array: Vec<&str> = queries.iter().map(|s| s.as_str()).collect();
    process_queries(str_array.as_slice(), transaction_manager.clone()).await;
}

// ******************** Test Cases ******************** //

/// Basic test -- FixMe
#[tokio::test(flavor = "multi_thread")]
async fn basic_create_insert_select_test() {
    clean_db_files().await;

    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    let queries = vec![
        "Create table test1239 (id int, name varchar)",
        "Insert into test1239 values(1,'rom'),(2,'rob')",
        "Select * from test1",
    ];

    process_queries(queries.as_slice(), transaction_manager).await;
}

/// Basic txn test
#[tokio::test(flavor = "multi_thread")]
async fn txn_basic_test() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(10, transaction_manager.clone()).await;

    let queries = [
        "update test1 set name = 'To'",
        "BEGIN",
        "update test1 set id = 4",
        "update test1 set id = 6",
        "COMMIT",
        "Select * from test1",
    ];

    process_queries(queries.as_slice(), transaction_manager).await;
}

/// Basic test update query with and without predicate
#[tokio::test(flavor = "multi_thread")]
async fn update_query_basic_test() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(30, transaction_manager.clone()).await;

    let queries = [
        "BEGIN",
        "update test1 set rate = 7777",
        "update test1 set rate = 77696",
        "update test1 set name = 'LatestDate'",
        "update test1 set rate = 2147483647",
        "Select * from test1",
        "update test1 set name = 'La'",
        "COMMIT",
        "Select * from test1",
    ];

    process_queries(queries.as_slice(), transaction_manager.clone()).await;

    load_db_data_batch_with_size(200, transaction_manager.clone()).await;

    let queries = [
        "BEGIN",
        "update test1 set rate = 9898",
        "update test1 set name = 'newestDate'",
        "update test1 set rate = 333347",
        "COMMIT",
        "Select * from test1",
        "update test1 set rate = 77 where id > 150",
        "Select * from test1",
    ];

    process_queries(queries.as_slice(), transaction_manager.clone()).await;
}

/// Random updates and read in parallel txns
#[tokio::test(flavor = "multi_thread")]
async fn txn_parallel_update_random() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(215, transaction_manager.clone()).await;

    let transaction1 = transaction_manager.clone();
    let t1 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "update test1 set name = 'InitCh' where id > 200",
            "Select * from test1",
            "BEGIN",
            "update test1 set rate = 77696 where id > 150",
            "update test1 set score = 83647 where id > 180",
            "update test1 set name = 'Laaaaaaaaaaaaaaaaaaa' where id > 150",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction1.clone()).await;
    });

    let transaction2 = transaction_manager.clone();
    let t2 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "BEGIN",
            "update test1 set name = 'JK' where id < 140",
            "update test1 set score = 234568 where id < 140",
            "Select * from test1",
            "update test1 set name = 'Fin' where id < 140",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction2.clone()).await;
    });

    let _ = t1.await;
    let _ = t2.await;
}

/// Select query with basic predicate
#[tokio::test(flavor = "multi_thread")]
async fn select_query_predicate() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(10, transaction_manager.clone()).await;

    let queries = [
        "Select * from test1",
        "Select * from test1 where score > 5",
        "Select * from test1 where id != 7",
    ];

    process_queries(queries.as_slice(), transaction_manager.clone()).await;
}

/// Transaction Test - Success case - parallel write
///      Run 2 transactions and update independent rows
///      The final select should show consistent result for the updated rows
#[tokio::test(flavor = "multi_thread")]
async fn txn_parallel_update_no_conflict() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(1_000, transaction_manager.clone()).await;

    let transaction1 = transaction_manager.clone();
    let t1 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "BEGIN",
            "update test1 set rate = 77696 where id > 500",
            "update test1 set score = 83647 where id > 500",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction1.clone()).await;
    });

    let transaction2 = transaction_manager.clone();
    let t2 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "BEGIN",
            "update test1 set score = 234568 where id < 500",
            "update test1 set name = 'Fin' where id < 500",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction2.clone()).await;
    });

    let _ = t1.await;
    let _ = t2.await;
}

/// Transaction Test - Success case - parallel write
///     Run 2 transactions and update same rows
///     Second transaction will fail and need to be re-run
///     The final select should show consistent result for the updated rows
#[tokio::test(flavor = "multi_thread")]
async fn txn_parallel_update_conflict() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    load_db_data_batch_with_size(1_000, transaction_manager.clone()).await;

    let transaction1 = transaction_manager.clone();
    let t1 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "BEGIN",
            "update test1 set rate = 77696",
            "update test1 set name = 'LA'",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction1.clone()).await;
    });

    let transaction2 = transaction_manager.clone();
    let t2 = tokio::spawn(async move {
        let queries = [
            "Select * from test1",
            "BEGIN",
            "update test1 set name = 'JK'",
            "update test1 set score = 234568",
            "COMMIT",
            "Select * from test1",
        ];

        process_queries(queries.as_slice(), transaction2.clone()).await;
    });

    let _ = t1.await;
    let _ = t2.await;
}

/// Multi-table test with inserts and filtered selects
/// Creates 5 tables with different schemas, inserts 50 entries each, and tests various select operations
#[tokio::test(flavor = "multi_thread")]
async fn multi_table_insert_select_with_filters_test() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    // Create 5 different tables with various data types
    let table_creation_queries = vec![
        // Table 1: Employee table
        "Create table employees (id int, name varchar, age int, salary int)",
        // Table 2: Products table
        "Create table products (product_id int, product_name varchar, price int, category_id int)",
        // Table 3: Orders table
        "Create table orders (order_id int, customer_name varchar, total_amount int, order_date varchar)",
        // Table 4: Students table
        "Create table students (student_id int, student_name varchar, grade int, subject varchar)",
        // Table 5: Inventory table
        "Create table inventory (item_id int, item_name varchar, quantity int, warehouse_id int)",
    ];

    // Create all tables
    process_queries(
        table_creation_queries.as_slice(),
        transaction_manager.clone(),
    )
    .await;

    // Insert 50 entries into employees table
    let mut employee_inserts = Vec::new();
    for i in 1..=50 {
        employee_inserts.push(format!(
            "Insert into employees (id, name, age, salary) values({}, 'Employee{}', {}, {})",
            i,
            i,
            25 + (i % 40),
            30000 + (i * 1000)
        ));
    }

    // Insert 50 entries into products table
    let mut product_inserts = Vec::new();
    let categories = vec!["Electronics", "Clothing", "Books", "Home", "Sports"];
    for i in 1..=50 {
        product_inserts.push(format!(
            "Insert into products (product_id, product_name, price, category_id) values({}, 'Product{}', {}, {})",
            i, i, 100 + (i * 50), (i % 5) + 1
        ));
    }

    // Insert 50 entries into orders table
    let mut order_inserts = Vec::new();
    for i in 1..=50 {
        order_inserts.push(format!(
            "Insert into orders (order_id, customer_name, total_amount, order_date) values({}, 'Customer{}', {}, '2024-01-{}')",
            i, i, 500 + (i * 100), (i % 28) + 1
        ));
    }

    // Insert 50 entries into students table
    let mut student_inserts = Vec::new();
    let subjects = vec!["Math", "Science", "English", "History", "Art"];
    for i in 1..=50 {
        student_inserts.push(format!(
            "Insert into students (student_id, student_name, grade, subject) values({}, 'Student{}', {}, '{}')",
            i, i, 80 + (i % 20), subjects[(i-1) % subjects.len()]
        ));
    }

    // Insert 50 entries into inventory table
    let mut inventory_inserts = Vec::new();
    for i in 1..=50 {
        inventory_inserts.push(format!(
            "Insert into inventory (item_id, item_name, quantity, warehouse_id) values({}, 'Item{}', {}, {})",
            i, i, 10 + (i * 5), (i % 3) + 1
        ));
    }

    // Convert to string slices for processing
    let employee_queries: Vec<&str> = employee_inserts.iter().map(|s| s.as_str()).collect();
    let product_queries: Vec<&str> = product_inserts.iter().map(|s| s.as_str()).collect();
    let order_queries: Vec<&str> = order_inserts.iter().map(|s| s.as_str()).collect();
    let student_queries: Vec<&str> = student_inserts.iter().map(|s| s.as_str()).collect();
    let inventory_queries: Vec<&str> = inventory_inserts.iter().map(|s| s.as_str()).collect();

    // Process all insert queries
    process_queries(employee_queries.as_slice(), transaction_manager.clone()).await;
    process_queries(product_queries.as_slice(), transaction_manager.clone()).await;
    process_queries(order_queries.as_slice(), transaction_manager.clone()).await;
    process_queries(student_queries.as_slice(), transaction_manager.clone()).await;
    process_queries(inventory_queries.as_slice(), transaction_manager.clone()).await;

    // Test SELECT * from all tables
    let select_all_queries = vec![
        "Select * from employees",
        "Select * from products",
        "Select * from orders",
        "Select * from students",
        "Select * from inventory",
    ];

    process_queries(select_all_queries.as_slice(), transaction_manager.clone()).await;

    // Test SELECT with various WHERE conditions
    let filtered_select_queries = vec![
        // Employee filters
        "Select * from employees where age > 40",
        "Select * from employees where salary > 35000",
        "Select * from employees where id <= 10",
        // Product filters
        "Select * from products where price > 1000",
        "Select * from products where category_id = 1",
        "Select * from products where product_id >= 25",
        // Order filters
        "Select * from orders where total_amount > 2000",
        "Select * from orders where order_id < 20",
        // Student filters
        "Select * from students where grade > 90",
        "Select * from students where student_id <= 25",
        // Inventory filters
        "Select * from inventory where quantity > 100",
        "Select * from inventory where warehouse_id = 2",
        "Select * from inventory where item_id >= 40",
    ];

    process_queries(
        filtered_select_queries.as_slice(),
        transaction_manager.clone(),
    )
    .await;

    // Test some specific value filters
    let specific_value_queries = vec![
        "Select * from employees where id = 15",
        "Select * from products where product_id = 30",
        "Select * from orders where order_id = 25",
        "Select * from students where student_id = 20",
        "Select * from inventory where item_id = 35",
    ];

    process_queries(
        specific_value_queries.as_slice(),
        transaction_manager.clone(),
    )
    .await;
}

/// Select Test with 'join' - Basic JOIN test
#[tokio::test(flavor = "multi_thread")]
async fn select_query_join_basic() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    // Load test data with two related tables
    load_join_test_data(transaction_manager.clone()).await;

    let queries = [
        "Select * from users",
        "Select * from departments",
        "Select users.id, users.name, departments.dept_name from users JOIN departments ON users.department_id = departments.id",
        "Select u.name, d.dept_name, d.location from users u JOIN departments d ON u.department_id = d.id",
    ];

    process_queries(queries.as_slice(), transaction_manager.clone()).await;
}

/// Select Test with 'join with predicate' - JOIN with WHERE conditions
#[tokio::test(flavor = "multi_thread")]
async fn select_query_join_with_predicate() {
    clean_db_files().await;
    let transaction_manager = Arc::new(Box::new(TransactionManager::init(
        create_storage_instance(),
    )));

    // Load test data with two related tables
    load_join_test_data(transaction_manager.clone()).await;

    let queries = [
        "Select * from users",
        "Select * from departments",
        "Select users.id, users.name, departments.dept_name from users JOIN departments ON users.department_id = departments.id WHERE users.id > 10",
        "Select u.name, d.dept_name from users u JOIN departments d ON u.department_id = d.id WHERE d.id <= 3",
        "Select u.id, u.name, d.dept_name, d.location from users u JOIN departments d ON u.department_id = d.id WHERE u.id > 5 AND d.id < 4",
    ];

    process_queries(queries.as_slice(), transaction_manager.clone()).await;
}

// TODO: Select Test with 'limit'

// TODO: Select Test with 'order by'

// TODO: Select Test with 'group by'

// TODO: Select Test with all the common expressions
//       - join, Predicate, group by, order by, limit

// TODO: Transaction Test - Failure case - complete rollback
//       Insert 5 rows in a table
//       Select all and assert
//       Run 2 transactions and update 2 same rows
//       Rollback 1 transaction or kill the client
//       The final select should show consistent result for the updated rows

// TODO: Transaction Test - Success case - parallel read and write
//       Insert 5 rows in a table
//       Select all and assert
//       Run 4 transactions 2 of them updating 2 rows and other 2 reading the same updated rows
//       The 2 reads should return the row data as of the transaction start time
//       The final select should show consistent result for the updated rows

// TODO: Transaction Test - Success Case - Bank account balance case
//       Insert 100 bank accounts and balance
//       Run 100 transactions moving money from 1 account to another or randomly
//       Final sum of all account balance should be the same as that at the start.
