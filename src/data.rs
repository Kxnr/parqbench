use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::execution::config::SessionConfig;
use datafusion::logical_expr::col as col_expr;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use object_store::local::LocalFileSystem;
use regex::Regex;
use smol::future::Boxed;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use url::Url;

use anyhow::{anyhow, bail};

pub type DataResult = anyhow::Result<Data>;
pub type DataFuture = Boxed<DataResult>;
pub type DataSourceListing = HashMap<String, Arc<dyn TableProvider>>;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum SortState {
    NotSorted,
    Ascending,
    Descending,
}

#[derive(Clone, Debug)]
pub enum Query {
    TableName(String),
    Sql(String),
}

// #[derive(Default)]
pub struct DataSource {
    ctx: SessionContext,
}

impl Default for DataSource {
    fn default() -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().execution.parquet.enable_page_index = true;
        config.options_mut().execution.parquet.pushdown_filters = true;
        config.options_mut().execution.parquet.pruning = true;
        config.options_mut().execution.parquet.reorder_filters = true;
        config.options_mut().execution.parquet.skip_metadata = false;
        config.options_mut().execution.collect_statistics = false;
        config.options_mut().catalog.information_schema = true;
        dbg!(&config);

        Self {
            ctx: SessionContext::new_with_config(config),
        }
    }
}

#[derive(Clone)]
pub struct Data {
    // TOOD: arc context into this struct?
    pub data: RecordBatch,
    pub query: Query,
    pub sort_state: Option<(String, SortState)>,
}

fn get_read_options(filename: &str) -> anyhow::Result<ParquetReadOptions<'_>> {
    // TODO: use this to decide the format to load the file in, with user configurable extensions
    Path::new(filename)
        .extension()
        .and_then(OsStr::to_str)
        .map(|s| ParquetReadOptions {
            file_extension: s,
            ..Default::default()
        })
        .ok_or(anyhow!(
            "Could not parse filename, does this file have an extension?"
        ))
}

type Prefix = PathBuf;
type RelativePath = PathBuf;

fn split_file_prefix(filename: &str) -> anyhow::Result<(Option<Prefix>, RelativePath)> {
    // if this filename has a prefix, split it and return a path relative to the prefix, which lets
    // us register an object store for the given prefix. Currently only handles UNC prefixes.
    // Returns Err if the path could not be canonicalized.

    let unc_prefix_regex = Regex::new(r"\\\\\?\\UNC\\([A-Za-z0-9_.$●-]+)\\([A-Za-z0-9_.$●-]+)\\")
        .expect("Hardcoded regex");
    let canonicalized_path = dbg!(std::fs::canonicalize(filename))?;
    if let Some(prefix) = unc_prefix_regex.find(
        canonicalized_path
            .to_str()
            .expect("Converting canonicalized path to string should work"),
    ) {
        let prefix = Path::new(prefix.as_str());
        let relative_path = canonicalized_path
            .strip_prefix(prefix)
            .expect("Prefix derived by regex from this path.");
        Ok((Some(prefix.to_owned()), relative_path.to_owned()))
    } else {
        Ok((None, canonicalized_path))
    }
}

fn concat_record_batches(batches: Vec<RecordBatch>) -> anyhow::Result<RecordBatch> {
    // NOTE: this may be rather inefficient, but there's a tradeoff for having random access to
    // NOTE: slice out a chunk of rows for display down the road, worth investigating whether
    // NOTE: there's a cleaner way here

    concat_batches(
        &batches.first().ok_or(anyhow!("Data is empty."))?.schema(),
        batches.iter(),
    )
    .map_err(|err| anyhow!(err))
}

impl DataSource {
    pub async fn list_tables(&self) -> DataSourceListing {
        // TODO: is there anything to be done to simplify this arrow?
        let mut tables = HashMap::new();
        for catalog_name in self.ctx.catalog_names() {
            let catalog = self.ctx.catalog(&catalog_name);
            if let Some(catalog) = catalog {
                for schema_name in catalog.schema_names() {
                    let schema = catalog.schema(&schema_name);
                    if let Some(schema) = schema {
                        for table_name in schema.table_names() {
                            // FIXME: this may perform I/O when using a remote source
                            if let Some(table) = schema
                                .table(&table_name)
                                .await
                                .expect("Failure to load registered table.")
                            {
                                tables.insert(table_name, table);
                            }
                        }
                    }
                }
            }
        }
        tables
    }

    pub async fn add_data_source(&mut self, filename: String) -> anyhow::Result<String> {
        // TODO: pass in data source name
        // TODO: multiple formats, partitioned datasets
        let (prefix, file_name) = split_file_prefix(shellexpand::full(&filename)?.borrow())?;
        if let Some(prefix) = prefix {
            let prefix_url = Url::from_directory_path(&prefix)
                .map_err(|_| anyhow!("Could not create url from unc prefix.".to_owned()))?;
            let object_store = LocalFileSystem::new_with_prefix(prefix)?;
            // TODO: check if object store exists
            self.ctx
                .register_object_store(&prefix_url, Arc::new(object_store));
        }

        let table_name = file_name
            .file_stem()
            .map_or(file_name.to_str(), |v| v.to_str())
            .expect("Could not convert filename to default table name");

        self.ctx
            .register_parquet(
                &table_name,
                &file_name
                    .to_str()
                    .expect("Could not convert filename to str"),
                get_read_options(&filename)?,
            )
            .await?;

        Ok(table_name.to_owned())
    }

    pub async fn query(&self, query: Query) -> anyhow::Result<Data> {
        let df = match &query {
            Query::TableName(table) => self.ctx.table(table).await?,
            Query::Sql(query) => self.ctx.sql(&query).await?,
        };

        let data = df.collect().await?;

        Ok(Data {
            // TODO: will record batches have the same schema, or should these really be
            // TODO: separate data entries?
            data: concat_record_batches(data)?,
            query,
            sort_state: None,
        })
    }
}

impl Data {
    pub async fn sort(self, col: String, sort: SortState) -> anyhow::Result<Self> {
        // TODO: should this be a clone of the exising context?
        // TODO: make successive queries able to be registered to the context, so that comple
        // TODO: queries can be constructed?
        let ctx = SessionContext::new();
        let mut df = ctx.read_batch(self.data)?;
        df = match &sort {
            // consider null "less" than real values, so they can get surfaced
            SortState::Ascending => df.sort(vec![col_expr(&col).sort(true, false)])?,
            SortState::Descending => df.sort(vec![col_expr(&col).sort(false, true)])?,
            _ => df,
        };

        let data = df.collect().await?;

        Ok(Data {
            // TODO: will record batches have the same schema, or should these really be
            // TODO: separate data entries?
            data: concat_record_batches(data)?,
            query: self.query,
            sort_state: Some((col, sort)),
        })
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.data.schema()
    }

    // TODO: querying from a df isn't entirely straightforward, but would be nice. It seems like
    // TODO: the easiest way is going to be to use an in memory table provider or similar. This may
    // TODO: end up needing to expose filter/select rather than sql, unless there's a good way to
    // TODO: build Expr instances. Notably because there's not really a table name here unless we
    // TODO: assign one.
    // NOTE: register_batch exists
    // pub async fn query(&mut self, query: Query) -> anyhow::Result<Self> {
    //     // TODO: can this be chained, rather than mut-assigned?
    //     let ctx = SessionContext::new();
    //     let df = ctx.read_batch(self.data.clone())?;
    //     let df = match &query {
    //         Query::Sql(query) => ctx.sql(&query).await?,
    //         _ => Err(anyhow!("In memory tables only support SQL queries."))?,
    //     };

    //     let data = df.collect().await?;

    //     Ok(Data {
    //         // TODO: will record batches have the same schema, or should these really be
    //         // TODO: separate data entries?
    //         data: concat_record_batches(data),
    //         query,
    //         sort_state: None,
    //     })
    // }
}
