use std::ffi::IntoStringError;
use std::str::FromStr;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::col;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TableName {
    pub name: String
}

impl Default for TableName {
    fn default() -> Self {
        Self { name: "main".to_string() }
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

#[derive(Clone, Debug)]
pub struct DataFilters {
    pub sort: Option<SortState>,
    pub table_name: TableName,
    pub query: Option<String>,
}

// TODO: serialize this for the query pane
impl Default for DataFilters {
    fn default() -> Self {
        Self {
            sort: None,
            table_name: TableName::default(),
            query: None,
        }
    }
}

#[derive(Clone)]
pub struct ParquetData {
    pub filename: String,
    pub data: RecordBatch,
    pub filters: DataFilters,
    dataframe: Arc<DataFrame>,
}

#[derive(PartialEq, Clone, Debug)]
pub enum SortState {
    NotSorted(String),
    Ascending(String),
    Descending(String),
}

fn concat_record_batches(batches: Vec<RecordBatch>) -> RecordBatch {
    RecordBatch::concat(&batches[0].schema(), &batches).unwrap()
}

impl ParquetData {
    pub async fn load(filename: String) -> Result<Self, String> {
        let ctx = SessionContext::new();
        match ctx
            .read_parquet(&filename, ParquetReadOptions::default())
            .await
        {
            Ok(df) => {
                let data = df.collect().await.unwrap();
                let data = concat_record_batches(data);
                Ok(ParquetData {
                    filename,
                    data,
                    dataframe: df,
                    filters: DataFilters::default(),
                })
            }
            Err(_) => Err("Could not load file.".to_string()),
        }
    }

    pub async fn load_with_query(filename: String, filters: DataFilters) -> Result<Self, String> {
        let ctx = SessionContext::new();
        ctx.register_parquet(
            filters.table_name.to_string().as_str(),
            filename.as_str(),
            ParquetReadOptions::default(),
        )
        .await
        .ok();

        match &filters.query {
            Some(query) => match ctx.sql(query.as_str()).await {
                Ok(df) => match df.collect().await {
                    Ok(data) => {
                        let data = concat_record_batches(data);
                        let data = ParquetData {
                            filename: filename.to_owned(),
                            data,
                            dataframe: df,
                            filters: filters,
                        };
                        data.sort(None).await
                    }
                    Err(_) => Err("Could not query file".to_string()),
                },
                Err(_) => Err("Could not query file".to_string()), // two classes of error, sql and file
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
                match self.dataframe.sort(vec![col(_col).sort(ascending, false)]) {
                    Ok(df) => match df.collect().await {
                        Ok(data) => Ok(ParquetData {
                            filename: self.filename,
                            data: concat_record_batches(data),
                            dataframe: df,
                            filters,
                        }),
                        Err(_) => Err("Error sorting data".to_string()),
                    },
                    Err(_) => Err("Could not sort data with given filters".to_string()),
                }
            }
            None => Ok(ParquetData { filters, ..self }),
        }
    }
}
