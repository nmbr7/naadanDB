<h1 align="center">
naadanDB
</h1>
<h4 align="center">
  - Oru naadan Database -
  <br>
</h4>
<p align="center"> This project is intended for learning and experimenting with DB internals and shouldn't be considered for production use. </p>

<br>
  
# Project Goals
1. Learn and experiment with various database design techniques.
2. Implement a database which can efficiently handles hybrid (OLTP & OLAP) workloads.
3. Create a detailed documentation describing pros & cons of different techniques.

# Build

Run `cargo build` to build the project.

## Running the UTs

Run `cargo test -- --nocapture` to run the UTs with console output.

- Tests for the individual components will be available in the respective modules
- Integration tests are available in the `/tests` folder

# Utilities

- CLI - [naadanCLI](https://github.com/nmbr7/naadanCLI)

# Design

## Query Flow

<img width="1108" alt="image" src="https://github.com/nmbr7/naadanDB/assets/19748270/93520ffe-a20c-4219-beab-b0b517061650">

Most of the core components are written from scratch except for the parser, for which we use the [sqlparser](https://crates.io/crates/sqlparser) crate.

## Components

- Server Layer
  - [x] Tokio async handler
- Query Layer
  - [x] Parser
  - [x] Logical Planner
  - [x] Physical Planner
  - [x] Executor
- Transaction Layer
  - [ ] MVCC with in-memory version chaining
  - [ ] Support Serializable Isolation level
- Storage Layer
  - [x] Row store format
  - [ ] B-tree index
  - [x] Binary format for Page structure (No extra serialization required)

# Benchmark

- TODO

# Features TODO

### SQL level

- Join
- Predicates
- Group by, Order by, Limit
- Stored procedures
- Cursors
- Triggers
- Partitioning
- Replication

### DB Design level

- DB Configurability support
- Target Webassembly runtime
- Proper recovery support
- Lock free data structures
- Optimistic latching
- Stream processing
- Column store
- JIT query execution
- Distributed operation
