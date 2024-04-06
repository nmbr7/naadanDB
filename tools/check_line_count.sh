#!/bin/sh

## Usage
## Run this from the project  root folder


## Classic bash unix wc
# wc -l `find src -type f`

## Cargo loc
## Install: `cargo install loc`
loc --files src
