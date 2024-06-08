#[macro_use]
extern crate derive_builder;

use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::logical_expr::col as col_expr;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::ffi::OsStr;
use std::future::Future;
use std::path::Path;

use anyhow::anyhow;

pub type DataResult = anyhow::Result<Data>;
pub type DataFuture = Box<dyn Future<Output = DataResult> + Unpin + Send + 'static>;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum SortState {
    NotSorted,
    Ascending,
    Descending,
}

#[derive(Clone)]
pub enum Query {
    TableName(String),
    Sql(String),
}

#[derive(Default)]
pub struct DataSource {
    ctx: SessionContext,
    // TODO: create a separate container to hold data for separate tabs
}

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

fn concat_record_batches(batches: Vec<RecordBatch>) -> RecordBatch {
    // NOTE: this may be rather inefficient, but there's a tradeoff for having random access to
    // NOTE: slice out a chunk of rows for display down the road, worth investigating whether
    // NOTE: there's a cleaner way here
    concat_batches(&batches[0].schema(), batches.iter()).unwrap()
}

impl DataSource {
    pub async fn add_data_source(&mut self, filename: String) -> anyhow::Result<String> {
        // TODO: pass in data source name
        // TODO: multiple formats, partitioned datasets
        let file_name = shellexpand::full(&filename)?.to_string();

        let table_name = Path::new(&file_name)
            .file_stem()
            .map_or(file_name.as_str(), |v| {
                v.to_str()
                    // TODO: ? might help here to make this a recoverable error
                    .expect("Could not convert filename to default table name")
            });

        self.ctx
            .register_parquet(&table_name, &file_name, get_read_options(&filename)?)
            .await?;

        Ok(table_name.to_owned())
    }

    pub async fn query(&mut self, query: Query) -> anyhow::Result<Data> {
        // TODO: can this be chained, rather than mut-assigned?
        let df = match &query {
            Query::TableName(table) => self.ctx.table(table).await?,
            Query::Sql(query) => self.ctx.sql(&query).await?,
        };

        let data = df.collect().await?;

        Ok(Data {
            // TODO: will record batches have the same schema, or should these really be
            // TODO: separate data entries?
            data: concat_record_batches(data),
            query,
            sort_state: None,
        })
    }
}

impl Data {
    // TODO: support actions, like timezone conversion on columns
    pub async fn sort(self, col: String, sort: SortState) -> anyhow::Result<Self> {
        // TODO: would be nice to do this without cloning, though there's always a tradeoff here--
        // if this is done in place, there's no data to display in the meantime
        let ctx = SessionContext::new();
        let mut df = ctx.read_batch(self.data.clone())?;
        df = match &sort {
            SortState::Ascending => df.sort(vec![col_expr(&col).sort(true, false)])?,
            SortState::Descending => df.sort(vec![col_expr(&col).sort(false, false)])?,
            _ => df,
        };

        let data = df.collect().await?;

        Ok(Data {
            // TODO: will record batches have the same schema, or should these really be
            // TODO: separate data entries?
            data: concat_record_batches(data),
            query: self.query,
            sort_state: Some((col, sort)),
        })
    }

    // TODO: querying from a df isn't entirely straightforward, but would be nice. It seems like
    // TODO: the easiest way is going to be to use an in memory table provider or similar. This may
    // TODO: end up needing to expose filter/select rather than sql, unless there's a good way to
    // TODO: build Expr instances. Notably because there's not really a table name here unless we
    // TODO: assign one.
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
