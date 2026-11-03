use anyhow::Result;
use backy_extract::fuse;
use structopt::StructOpt;

#[tokio::main()]
async fn main() -> Result<()> {
    env_logger::init();
    fuse::App::from_args().run().await
}
