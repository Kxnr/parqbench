use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::{concat, ParquetReadOptions, SessionContext};
use datafusion::logical_expr::col;

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

pub struct ParquetData {
    pub filename: String,
    pub data: RecordBatch,
    pub filters: DataFilters,
    dataframe: Arc<DataFrame>
}

#[derive(PartialEq)]
#[derive(Clone)]
pub enum SortState {
    NotSorted(String),
    Ascending(String),
    Descending(String)
}



fn concat_record_batches(batches: Vec<RecordBatch>) -> RecordBatch {
    RecordBatch::concat(&batches[0].schema(), &batches).unwrap()
}


impl ParquetData {
    pub async fn load(filename: String) -> Result<Self, String> {
        let ctx = SessionContext::new();
        match ctx.read_parquet(&filename, ParquetReadOptions::default()).await {
            Ok(df) => {
                let data = df.collect().await.unwrap();
                let data = concat_record_batches(data);
                Ok(ParquetData { filename: filename, data: data, dataframe: df, filters: DataFilters::default()})
            },
            Err(_) => {
                Err("Could not load file.".to_string())
            }
        }
    }

    pub async fn load_with_query(filename: &str, filters: DataFilters) -> Result<Self, String> {
        let ctx = SessionContext::new();
        ctx.register_parquet(filters.table_name.as_str(),
                             filename,
                             ParquetReadOptions::default()).await.ok();

        match filters.query.as_ref() {
            // TODO: better to hang onto context rather than df, to sidestep re-loading?
            // TODO: as is, easier to load subset
            Some(query) => match ctx.sql(query.as_str()).await {
                Ok(df) => {
                    match df.collect().await {
                        Ok(data) => {
                            let data = concat_record_batches(data);
                            let data = ParquetData {filename: filename.to_owned(), data: data, dataframe: df, filters: filters.clone()};
                            data.sort(None).await
                        },
                        Err(err) => Err("Could not query file".to_string())
                    }
                },
                Err(err) => Err("Could not query file".to_string())   // two classes of error, sql and file
            },
            None => Err("No query provided".to_string())
        }
    }

    pub async fn sort(self, filters: Option<DataFilters>) -> Result<Self, String> {
        let filters = match filters {
            Some(filters) => filters,
            None => self.filters.clone()
        };

        match filters.sort.as_ref() {
            Some(sort) => {
                let (_col, ascending) = match sort {
                    SortState::Ascending(col) => {(col, true)},
                    SortState::Descending(col) => {(col, false)},
                    _ => panic!()
                };
                match self.dataframe.sort(vec![col(_col).sort(ascending, false)]) {
                    Ok(df) => {
                        Ok(ParquetData { filename: self.filename, data: self.data, dataframe: df, filters: filters})
                    }
                    Err(_) => Err("Could not sort data with given filters".to_string())
                }
            },
            None => {
                Ok(ParquetData { filename: self.filename, data: self.data, dataframe: self.dataframe, filters: filters})
            }
        }
    }
}
