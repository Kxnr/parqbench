use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::execution::config::SessionConfig;
use datafusion::logical_expr::col as col_expr;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::local::LocalFileSystem;
use regex::Regex;
use smol::future::Boxed;
use std::borrow::Borrow;
use std::collections::BTreeMap;
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

pub struct TableDescriptor {
    url: Url,
    extension: Option<String>,
    account: Option<String>,
    table_name: Option<String>,
    load_metadata: bool,
}

impl TableDescriptor {
    pub fn new(url: &str) -> anyhow::Result<Self> {
        let ext = Path::new(url)
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string());

        Ok(Self {
            url: make_url_from_path(url)?,
            extension: ext,
            account: None,
            table_name: None,
            load_metadata: true,
        })
    }

    pub fn with_extension(mut self, extension: &str) -> Self {
        self.extension = Some(extension.to_owned());
        self
    }

    pub fn with_account(mut self, account: &str) -> Self {
        self.account = Some(account.to_owned());
        self
    }

    pub fn with_load_metadata(mut self, flag: bool) -> Self {
        self.load_metadata = flag;
        self
    }

    pub fn with_table_name(mut self, table_name: &str) -> Self {
        self.table_name = Some(table_name.to_owned());
        self
    }
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
    pub sort_state: Option<(String, SortState)>,
}

fn get_read_options(table: &TableDescriptor) -> ParquetReadOptions<'_> {
    // TODO: use this to decide the format to load the file in, with user configurable extensions
    match table.extension.as_ref() {
        Some(ext) => ParquetReadOptions {
            file_extension: ext,
            skip_metadata: Some(!table.load_metadata),
            ..Default::default()
        },
        _ => ParquetReadOptions {
            skip_metadata: Some(!table.load_metadata),
            ..Default::default()
        },
    }
}

fn filesystem_path_to_url(path: &Path) -> anyhow::Result<Url> {
    if path.is_file() {
        Url::from_file_path(path)
    } else {
        Url::from_directory_path(path)
    }
    .map_err(|_| anyhow!("Could not create Url from path."))
}

fn unc_path_to_url(path: &Path) -> anyhow::Result<Url> {
    let url = filesystem_path_to_url(path)?;
    let mut scheme = url
        .host()
        .expect("UNC format always has a host")
        .to_string();
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

    Ok(dbg!(Url::parse(&format!(
        "{}://{}/{}",
        scheme,
        share,
        path.collect::<Vec<&str>>().join("/")
    )))?)
}

fn make_url_from_path(path: &str) -> anyhow::Result<Url> {
    let path = shellexpand::full(path)?.into_owned();
    let unc_prefix_regex = Regex::new(UNC_REGEX).expect("Hardcoded regex");

    let url = match std::fs::canonicalize(Path::new(&path)) {
        Ok(path) => {
            match unc_prefix_regex.find(path.to_str().expect("Could not convert path to string.")) {
                Some(_) => unc_path_to_url(&path)?,
                None => filesystem_path_to_url(&path)?,
            }
        }
        Err(_) => Url::parse(path.borrow())?,
    };

    Ok(dbg!(url))
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
                            #[allow(clippy::map_entry)]
                            if !self.cached_schemas.contains_key(&table_name) {
                                if let Some(table) = schema
                                    .table(&table_name)
                                    .await
                                    .expect("Failure to load registered table.")
                                {
                                    self.cached_schemas.insert(table_name, table);
                                };
                            }
                        }
                    }
                }
            }
        }
        &self.cached_schemas
    }

    fn add_object_store_for_table(&mut self, table: &TableDescriptor) -> anyhow::Result<()> {
        match table.url.scheme() {
            "wsl" | "wsllocalhost" => {
                let prefix = format!(
                    r"\\?\UNC\wsl.localhost\{}\",
                    table.url.host().expect("WSL url must have host.")
                );
                let object_store = LocalFileSystem::new_with_prefix(prefix)?;
                self.ctx.register_object_store(
                    &dbg!(Url::parse(
                        &table.url[url::Position::BeforeScheme..url::Position::AfterHost]
                    ))?,
                    Arc::new(object_store),
                );
            }
            "az" | "azure" | "abfs" | "abfss" => {
                let object_store = MicrosoftAzureBuilder::new()
                    .with_url(table.url.to_string())
                    .with_account(
                        table
                            .account
                            .as_ref()
                            .ok_or(anyhow!("Account required for Azure table"))?,
                    )
                    .with_use_azure_cli(true)
                    .build()?;
                dbg!("adding azure store");
                self.ctx.register_object_store(
                    &dbg!(Url::parse(
                        &table.url[url::Position::BeforeScheme..url::Position::AfterHost]
                    ))?,
                    Arc::new(object_store),
                );
            }
            _ => {}
        };

        Ok(())
    }

    pub fn rename_data_source(
        &mut self,
        from_name: &str,
        to_name: &str,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let table = self.delete_data_source(from_name)?;
        // will be added back to cache when accessed, don't need to add now
        self.ctx
            .register_table(to_name, table)
            .map_err(|err| anyhow!(err))
            .and_then(|table| table.ok_or(anyhow!("Error registering table")))
    }

    pub fn delete_data_source(&mut self, source: &str) -> anyhow::Result<Arc<dyn TableProvider>> {
        if let Some(table) = self.ctx.deregister_table(source)? {
            self.cached_schemas.remove(source);
            Ok(table)
        } else {
            Err(anyhow!("Error retrieving table"))
        }
    }

    pub async fn add_data_source(&mut self, source: TableDescriptor) -> anyhow::Result<String> {
        self.add_object_store_for_table(&source)?;

        // TODO: get &str directly, rather than using String
        let table_name = match source.table_name {
            Some(ref table_name) => table_name.clone(),
            _ => Path::new(&source.url.path())
                .file_stem()
                .and_then(|s| s.to_str())
                .expect("Could not convert filename to default table name")
                .to_lowercase(),
        };

        let read_options = get_read_options(&source);

        // TODO: register listing table rather than

        self.ctx
            .register_parquet(&table_name, source.url.as_ref(), read_options)
            .await?;

        Ok(table_name.to_owned())
    }

    pub async fn query(&self, query: Query) -> anyhow::Result<Data> {
        let df = match &query {
            Query::TableName(table) => self.ctx.table(table.to_lowercase()).await?,
            Query::Sql(query) => self.ctx.sql(query).await?,
        };

        let data = df.collect().await?;

        Ok(Data {
            // TODO: will record batches have the same schema, or should these really be
            // TODO: separate data entries?
            data: concat_record_batches(data)?,
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
            sort_state: Some((col, sort)),
        })
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.data.schema()
    }
}
