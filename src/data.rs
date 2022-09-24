use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::col;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::sync::Arc;

#[derive(Clone)]
pub struct DataFilters {
    pub sort: Option<SortState>,
    pub table_name: String,
    pub query: Option<String>,
}

// TODO: serialize this for the query pane
impl Default for DataFilters {
    fn default() -> Self {
        Self {
            sort: None,
            table_name: "main".to_string(),
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

#[derive(PartialEq, Clone)]
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

    pub async fn load_with_query(filename: &str, filters: DataFilters) -> Result<Self, String> {
        let ctx = SessionContext::new();
        ctx.register_parquet(
            filters.table_name.as_str(),
            filename,
            ParquetReadOptions::default(),
        )
        .await
        .ok();

        match filters.query.as_ref() {
            // TODO: better to hang onto context rather than df, to sidestep re-loading?
            // TODO: as is, easier to load subset
            Some(query) => match ctx.sql(query.as_str()).await {
                Ok(df) => match df.collect().await {
                    Ok(data) => {
                        let data = concat_record_batches(data);
                        let data = ParquetData {
                            filename: filename.to_owned(),
                            data,
                            dataframe: df,
                            filters: filters.clone(),
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
