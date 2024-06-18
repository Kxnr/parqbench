# ParqBench

[//]: # ([![dependency status]&#40;https://deps.rs/repo/github/emilk/eframe_template/status.svg&#41;]&#40;https://deps.rs/repo/github/emilk/eframe_template&#41;)
[//]: # ([![Build Status]&#40;https://github.com/emilk/eframe_template/workflows/CI/badge.svg&#41;]&#40;https://github.com/emilk/eframe_template/actions?workflow=CI&#41;)

A simple utility for viewing parquet files, built on egui and arrow.

## About

ParqBench supports loading and querying data from local files, files on WSL (if on windows), and 
Azure blob storage. Queries support the range of expressions supported by [datafusion](https://docs.rs/datafusion/latest/datafusion/)
and can combine data from multiple tables. Tables may either be from a single file or a directory of
files, so long as all files in the directory share the same schema.

## Installation

Portable binaries for Windows and Linux are available on the [releases page](https://github.com/Kxnr/parqbench/releases).

## Contribution

I revisit this project intermittently, if there's something you'd like to see please open an issue
describing the current and desired behaviors.
