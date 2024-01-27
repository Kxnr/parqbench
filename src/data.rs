use datafusion::{
    arrow::record_batch::RecordBatch,
    arrow::compute::concat_batches,
    common::DFSchema,
    dataframe::DataFrame,
    logical_expr::col,
    prelude::{
        ParquetReadOptions,
        SessionContext,
    },
};

use std::{
    ffi::IntoStringError,
    future::Future,
    ops::Deref,
    str::FromStr,
    sync::Arc,
    path::Path,
    ffi::OsStr,
};

pub type DataResult = Result<ParquetData, String>;
pub type DataFuture = Box<dyn Future<Output = DataResult> + Unpin + Send + 'static>;

fn get_read_options(filename: &str) -> Option<ParquetReadOptions<'_>> {
    Path::new(filename).extension().and_then(OsStr::to_str).map(|s| {
        ParquetReadOptions {file_extension: s, ..Default::default()}
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
    dataframe: Arc<DataFrame>,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum SortState {
    NotSorted(String),
    Ascending(String),
    Descending(String),
}

/// Concatenates an array of RecordBatch into one batch
///
/// <https://docs.rs/datafusion/latest/datafusion/common/arrow/compute/kernels/concat/fn.concat_batches.html>
///
/// <https://docs.rs/datafusion/latest/datafusion/physical_plan/coalesce_batches/fn.concat_batches.html>
fn concat_record_batches(batches: &[RecordBatch]) -> RecordBatch {
    concat_batches(&batches[0].schema(), batches).unwrap()
}

impl ParquetData {
    pub async fn load(filename: String) -> Result<Self, String> {
        let filename = shellexpand::full(&filename)
            .map_err(|err| err.to_string())?
            .to_string();

        dbg!(&filename);

        let ctx = SessionContext::new();
        match ctx
            .read_parquet(&filename, get_read_options(&filename).ok_or("Could not set read options. Does this file have a valid extension?".to_string())?)
            .await
        {
            Ok(df) => {
                let data = df.clone().collect().await.unwrap();
                let data = concat_record_batches(&data);
                Ok(ParquetData {
                    filename,
                    data,
                    dataframe: df.into(),
                    filters: DataFilters::default(),
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
            get_read_options(&filename).ok_or("Could not set read options. Does this file have a valid extension?".to_string())?,
        )
        .await
        .ok();

        match &filters.query {
            Some(query) => match ctx.sql(query.as_str()).await {
                Ok(df) => match df.clone().collect().await {
                    Ok(data) => {
                        let data = concat_record_batches(&data);
                        let data = ParquetData {
                            filename: filename.to_owned(),
                            data,
                            dataframe: df.into(),
                            filters,
                        };
                        data.sort(None).await
                    }
                    Err(msg) => Err(msg.to_string()),
                },
                Err(msg) => Err(msg.to_string()), // two classes of error, sql and file
            },
            None => Err("No query provided".to_string()),
        }
    }

    pub async fn sort(self, opt_filters: Option<DataFilters>) -> Result<Self, String> {
        match opt_filters {
            Some(filters) => {
                match filters.sort.as_ref() {
                    Some(sort) => {
                        let (_col, ascending) = match sort {
                            SortState::Ascending(col) => (col, true),
                            SortState::Descending(col) => (col, false),
                            _ => panic!(""),
                        };

                        // https://doc.rust-lang.org/std/sync/struct.Arc.html
                        // Arc<T> automatically dereferences to T (via the Deref trait)
                        let df: DataFrame = self.dataframe.deref().clone();

                        let sorted = df.sort(vec![col(_col).sort(ascending, false)]);

                        match sorted {
                            Ok(df) => match df.clone().collect().await {
                                Ok(data) => Ok(ParquetData {
                                    filename: self.filename,
                                    data: concat_record_batches(&data),
                                    dataframe: df.into(),
                                    filters,
                                }),
                                Err(_) => Err("Error sorting data".to_string()),
                            },
                            Err(_) => Err("Could not sort data with given filters".to_string()),
                        }
                    }
                    None => Ok(self),
                }
            },
            None => Ok(self),
        }
    }

    pub fn metadata(&self) -> &DFSchema {
        self.dataframe.schema()
    }
}
