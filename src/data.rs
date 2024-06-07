use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::col;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::ffi::IntoStringError;
use std::ffi::OsStr;
use std::future::Future;
use std::path::Path;
use std::str::FromStr;

pub type DataResult = Result<ParquetData, String>;
pub type DataFuture = Box<dyn Future<Output = DataResult> + Unpin + Send + 'static>;

fn get_read_options(filename: &str) -> Option<ParquetReadOptions<'_>> {
    Path::new(filename)
        .extension()
        .and_then(OsStr::to_str)
        .map(|s| ParquetReadOptions {
            file_extension: s,
            ..Default::default()
        })
}

#[derive(Debug, Clone)]
pub struct TableName {
    pub name: String,
}

impl Default for TableName {
    fn default() -> Self {
        Self {
            name: "main".to_string(),
        }
    }
}

impl FromStr for TableName {
    type Err = IntoStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { name: s.to_owned() })
    }
}

impl ToString for TableName {
    fn to_string(&self) -> String {
        self.name.to_owned()
    }
}

#[derive(Clone, Debug, Default)]
pub struct DataFilters {
    pub sort: Option<SortState>,
    pub table_name: TableName,
    pub query: Option<String>,
}

#[derive(Clone)]
pub struct ParquetData {
    pub filename: String,
    pub data: RecordBatch,
    pub filters: DataFilters,
    pub schema: DFSchema,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum SortState {
    NotSorted(String),
    Ascending(String),
    Descending(String),
}

trait DataSource {
    // TODO: persist context with registered data sources, which we then load with `table`, rather
    // TODO: than calling read* methods directly
    async fn get_dataframe(filename: &str) -> Result<DataFrame, String>;
}

fn concat_record_batches(batches: Vec<RecordBatch>) -> RecordBatch {
    // NOTE: this may be rather inefficient, but there's a tradeoff for having random access to
    // NOTE: slice out a chunk of rows for display down the road, worth investigating whether
    // NOTE: there's a cleaner way here
    concat_batches(&batches[0].schema(), batches.iter()).unwrap()
}

impl DataSource for ParquetData {
    async fn get_dataframe(filename: &str) -> Result<DataFrame, String> {
        let ctx = SessionContext::new();
        ctx.read_parquet(
            filename,
            get_read_options(filename).ok_or(
                "Could not set read options. Does this file have a valid extension?".to_string(),
            )?,
        )
        .await
        .or_else(|err| Err(format!("{}", err)))
    }
}

impl ParquetData {
    pub async fn load(filename: String) -> Result<Self, String> {
        let filename = shellexpand::full(&filename)
            .map_err(|err| err.to_string())?
            .to_string();

        dbg!(&filename);

        match ParquetData::get_dataframe(&filename).await {
            Ok(df) => {
                let schema = df.schema().clone();
                // TODO: unsafe unwrap
                let data = df.collect().await.expect("Could not execute plan");
                Ok(ParquetData {
                    filename,
                    data: concat_record_batches(data),
                    filters: DataFilters::default(),
                    schema,
                })
            }
            Err(msg) => Err(format!("{}", msg)),
        }
    }

    pub async fn load_with_query(filename: String, filters: DataFilters) -> Result<Self, String> {
        let filename = shellexpand::full(&filename)
            .map_err(|err| err.to_string())?
            .to_string();

        dbg!(&filename);

        let ctx = SessionContext::new();
        ctx.register_parquet(
            filters.table_name.to_string().as_str(),
            &filename,
            get_read_options(&filename).ok_or(
                "Could not set read options. Does this file have a valid extension?".to_string(),
            )?,
        )
        .await
        .ok();

        match &filters.query {
            Some(query) => match ctx.sql(query.as_str()).await {
                Ok(df) => {
                    let schema = df.schema().clone();
                    match df.collect().await {
                        Ok(data) => {
                            let data = ParquetData {
                                filename: filename.to_owned(),
                                data: concat_record_batches(data),
                                filters,
                                schema,
                            };
                            data.sort(None).await
                        }
                        Err(msg) => Err(msg.to_string()),
                    }
                }
                Err(msg) => Err(msg.to_string()), // two classes of error, sql and file
            },
            None => Err("No query provided".to_string()),
        }
    }

    pub async fn sort(self, filters: Option<DataFilters>) -> Result<Self, String> {
        let filters = match filters {
            Some(filters) => filters,
            None => self.filters.clone(),
        };

        match filters.sort.as_ref() {
            Some(sort) => {
                let (_col, ascending) = match sort {
                    SortState::Ascending(col) => (col, true),
                    SortState::Descending(col) => (col, false),
                    _ => panic!(""),
                };
                match ParquetData::get_dataframe(&self.filename)
                    .await?
                    .sort(vec![col(_col).sort(ascending, false)])
                {
                    Ok(df) => {
                        let schema = df.schema().clone();

                        match df.collect().await {
                            Ok(data) => Ok(ParquetData {
                                filename: self.filename,
                                data: concat_record_batches(data),
                                filters,
                                schema,
                            }),
                            Err(_) => Err("Error sorting data".to_string()),
                        }
                    }
                    Err(_) => Err("Could not sort data with given filters".to_string()),
                }
            }
            None => Ok(ParquetData { filters, ..self }),
        }
    }
}
