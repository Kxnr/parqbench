# ParqBench

[//]: # ([![dependency status]&#40;https://deps.rs/repo/github/emilk/eframe_template/status.svg&#41;]&#40;https://deps.rs/repo/github/emilk/eframe_template&#41;)
[//]: # ([![Build Status]&#40;https://github.com/emilk/eframe_template/workflows/CI/badge.svg&#41;]&#40;https://github.com/emilk/eframe_template/actions?workflow=CI&#41;)

A simple, cross-platform, utility for viewing parquet files, build on egui and arrow.

## TODO List

- [ ] CI for builds/releases
- [ ] load partitioned dataset
- [ ] tab layout/tree
- [x] add controls and metadata to side panels
- [ ] parse pandas format metadata
- [x] open with hooks (cmd line args)
- [x] notification for errors
- [ ] support all filetypes supported by datafusion
- [x] Update datafusion and egui/eframe
- [ ] Auto resize columns
- [ ] revisit &str vs String usages
- [ ] Ui for basic query operations
- [ ] source configuration in query pane
- [x] rich metadata with parquet crate

## Installation

Generic, portable binaries for Windows and Linux are available on the [releases](https://github.com/Kxnr/parqbench/releases).

ParqBench is tested for Linux (Manjaro 22.0, kernel 5.15.60) and Windows 10 (21H2). Releases are built with:

`cargo build --release --target x86_64-pc-windows-gnu`
`cargo build --release --target x86_64-unknown-linux-gnu`

The builds are self-contained, portable, executables. The resulting binaries are placed in `target/<target>`.
