use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use datafusion::dataframe::DataFrame;
use datafusion::prelude::{concat, ParquetReadOptions, SessionContext};
use datafusion::logical_expr::col;

pub struct SortDescriptor {
    column: String,
    ascending: bool
}

pub struct DataFilters {
    sort: Option<SortDescriptor>,
    table_name: String,
    query: Option<String>,
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
    dataframe: Arc<DataFrame>
}


fn concat_record_batches(batches: Vec<RecordBatch>) -> RecordBatch {
    RecordBatch::concat(&batches[0].schema(), &batches).unwrap()
}


impl ParquetData {
    pub async fn load(filename: &str) -> Result<Self> {
        let ctx = SessionContext::new();
        match ctx.read_parquet(filename, ParquetReadOptions::default()).await {
            Ok(df) => {
                let data = df.collect().await.unwrap();
                let data = concat_record_batches(data);
                Ok(ParquetData { filename: filename.to_string(), data: data, dataframe: df })
            },
            Err(_) => {
                Err(todo!)
            }
        }
    }

    pub async fn load_with_query(filename: &str, filters: &DataFilters) -> Result<Self> {
        let ctx = SessionContext::new();
        ctx.register_parquet(filters.table_name.as_str(),
                             filename,
                             ParquetReadOptions::default()).await.ok();

        match filters.query.as_ref() {
            Some(query) => match ctx.sql(query.as_str()).await {
                Ok(data) => {
                    todo!("dataframe");
                    let data = concat_record_batches(data);
                    Ok(ParquetData {filename: filename.to_owned(), data: data, df: df})
                },
                Err(err) => Err(err)   // two classes of error, sql and file
            },
            None => Err(!todo("define error types"))
        }
        // TODO: if sort is not none, attempt to sort here

        // match ctx.sql(filters.query.as_ref().unwrap().as_str()).await.ok() {
        //     Some(df) => {
        //         if let Some(data) = df.collect().await.ok() {
        //             let data = concat_record_batches(data);
        //             Some(ParquetData { filename: filename.to_string(), data: data, dataframe: df })
        //         } else {
        //             // FIXME: syntax errors wipe data
        //             None
        //         }
        //     },
        //     None => {
        //         None
        //     }
        // }
    }

    pub async fn sort(self, filters: &DataFilters) -> Self {
        // FIXME: unsafe unwrap
        let df = self.dataframe.sort(vec![col(filters.sort.as_ref().unwrap().as_str()).sort(filters.ascending, false)]);

        // FIXME: panic on bad query
        match df.ok() {
            Some(df) => {
                ParquetData { filename: self.filename, data: self.data, dataframe: df }
            },
            None => {
                self
            }
        }
    }

}
