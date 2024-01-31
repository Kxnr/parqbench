use crate::TableName;
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
#[structopt()]
pub struct Args {
    #[structopt()]
    pub filename: Option<String>,

    #[structopt(short, long, requires("filename"))]
    pub query: Option<String>,

    #[structopt(short, long, requires_all(&["filename", "query"]))]
    pub table_name: Option<TableName>,
}

impl Args {
    pub fn build() -> Args {
        Args::from_args()
    }
}
