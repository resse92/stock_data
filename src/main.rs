use anyhow::Result;
use clap::{Parser, Subcommand};
use dotenvy::dotenv;
use rust_kline_fetcher::sync_daily::{run_sync_daily, SyncDailyArgs};

#[derive(Debug, Parser)]
#[command(name = "kline-sync", version, about = "A股K线同步命令")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 按日期区间下载日线并写入 S3 parquet
    SyncDaily(SyncDailyArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Commands::SyncDaily(args) => run_sync_daily(args).await,
    }
}
