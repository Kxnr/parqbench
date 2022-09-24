# ParqBench

[//]: # ([![dependency status]&#40;https://deps.rs/repo/github/emilk/eframe_template/status.svg&#41;]&#40;https://deps.rs/repo/github/emilk/eframe_template&#41;)
[//]: # ([![Build Status]&#40;https://github.com/emilk/eframe_template/workflows/CI/badge.svg&#41;]&#40;https://github.com/emilk/eframe_template/actions?workflow=CI&#41;)

A simple, cross-platform, utility for viewing parquet files, build on egui and arrow.

## TODO List

- [ ] load partitioned dataset
- [ ] tab layout/tree
- [ ] add controls and metadata to side panels
- [ ] parse pandas format metadata
- [ ] open with hooks (cmd line args)
- [ ] allow configuration of layout
- [ ] notification for errors
- [ ] support all filetypes supported by arrow
- [ ] CI for builds/releases
- [x] Update datafusion and egui/eframe
- [ ] Auto resize columns

## Installation

Generic, portable binaries for Windows and Linux are available on the [releases](https://github.com/Kxnr/parqbench/releases).

ParqBench is tested for Linux (Manjaro 22.0, kernel 5.15.60) and Windows 10 (21H2). Releases are built with:

`cargo build --release --target x86_64-pc-windows-gnu`
`cargo build --release --target x86_64-unknown-linux-gnu`

The builds are self contained, portable, executables. The resulting binaries are placed in `target/<target>`.
