use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::execution::config::SessionConfig;
use datafusion::logical_expr::col as col_expr;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use object_store::local::LocalFileSystem;
use regex::Regex;
use smol::future::Boxed;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;
use url::Url;

use anyhow::anyhow;

pub type DataResult = anyhow::Result<Data>;
pub type DataFuture = Boxed<DataResult>;
pub type DataSourceListing = BTreeMap<String, Arc<dyn TableProvider>>;

const UNC_REGEX: &str = r"\\\\\?\\UNC\\([A-Za-z0-9_.$●-]+)\\([A-Za-z0-9_.$●-]+)\\";

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
    cached_schemas: DataSourceListing,
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
            cached_schemas: BTreeMap::new(),
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

fn unc_path_to_url(path: &Path) -> anyhow::Result<Url> {
    let url =
        Url::from_directory_path(path).map_err(|_| anyhow!("Could not create Url from path."))?;

    let mut scheme = url
        .host()
        .expect("UNC format always has a host")
        .to_string()
        .to_lowercase();
    scheme.retain(|c| c.is_alphabetic());

    let mut share = url
        .path_segments()
        .and_then(|mut splt| splt.next())
        .expect("UNC format always has a share")
        .to_lowercase();
    share.retain(|c| c.is_alphabetic());
    let mut path = url
        .path_segments()
        .expect("UNC path always has at least one component");
    path.next();

    Ok(Url::parse(&format!(
        "{}://{}/{}",
        scheme,
        share,
        path.collect::<Vec<&str>>().join("/")
    ))?)
}

fn make_url_from_path(path: &str) -> anyhow::Result<Url> {
    let path = shellexpand::full(path)?.into_owned();
    let unc_prefix_regex = Regex::new(UNC_REGEX).expect("Hardcoded regex");

    let url = match std::fs::canonicalize(Path::new(&path)) {
        Ok(path) => {
            match unc_prefix_regex.find(path.to_str().expect("Could not convert path to string.")) {
                Some(_) => unc_path_to_url(&path)?,
                None => Url::from_directory_path(path)
                    .map_err(|_| anyhow!("Could not parse directory"))?,
            }
        }
        Err(_) => Url::parse(path.borrow())?,
    };
    Ok(url)
}

fn concat_record_batches(batches: Vec<RecordBatch>) -> anyhow::Result<RecordBatch> {
    concat_batches(
        &batches.first().ok_or(anyhow!("Data is empty."))?.schema(),
        batches.iter(),
    )
    .map_err(|err| anyhow!(err))
}

impl DataSource {
    pub async fn list_tables(&mut self) -> &DataSourceListing {
        // TODO: is there anything to be done to simplify this arrow?
        for catalog_name in self.ctx.catalog_names() {
            let catalog = self.ctx.catalog(&catalog_name);
            if let Some(catalog) = catalog {
                for schema_name in catalog.schema_names() {
                    let schema = catalog.schema(&schema_name);
                    if let Some(schema) = schema {
                        for table_name in schema.table_names() {
                            if !self.cached_schemas.contains_key(&table_name) {
                                match schema
                                    .table(&table_name)
                                    .await
                                    .expect("Failure to load registered table.")
                                {
                                    Some(table) => {
                                        self.cached_schemas.insert(table_name, table);
                                    }
                                    _ => {}
                                };
                            }
                        }
                    }
                }
            }
        }
        &self.cached_schemas
    }

    fn add_object_store_for_url(&mut self, url: &Url) -> anyhow::Result<()> {
        match url.scheme() {
            "wsl" => {
                let prefix = format!(
                    r"\\?\UNC\wsl$\{}",
                    url.host().expect("WSL url must have host.")
                );
                let object_store = LocalFileSystem::new_with_prefix(prefix)?;
                self.ctx.register_object_store(
                    &Url::parse(&url[url::Position::BeforeScheme..url::Position::AfterHost])?,
                    Arc::new(object_store),
                );
            }
            _ => {}
        };

        Ok(())
    }

    pub async fn add_data_source(&mut self, table_path: String) -> anyhow::Result<String> {
        // TODO: pass in data source name
        // TODO: register ListingTables rather than particular formats
        let url = make_url_from_path(&table_path)?;
        self.add_object_store_for_url(&url);

        let table_name = url
            .path_segments()
            .map_or(Some(url.to_string()), |mut p| {
                p.next().map(|s| s.to_owned())
            })
            .expect("Could not convert filename to default table name");

        dbg!("registering source");
        // TODO: register listing table rather than
        self.ctx
            .register_parquet(
                &table_name.to_lowercase(),
                &url.to_string(),
                get_read_options(&table_path)?,
            )
            .await?;

        Ok(table_name.to_owned())
    }

    pub async fn query(&self, query: Query) -> anyhow::Result<Data> {
        let df = match &query {
            Query::TableName(table) => self.ctx.table(table.to_lowercase()).await?,
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
