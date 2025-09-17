use std::fs;
use std::env;
use std::time::Instant;
use datafusion::prelude::*;

#[tokio::main(flavor = "current_thread")]
async fn main() -> datafusion::error::Result<()> {
  
  let config = SessionConfig::new();
  let ctx = SessionContext::new_with_config(config);
  
  let home_dir = std::env::var("HOME").unwrap();
  let files = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"];
  for file in files {
    ctx.register_parquet(file, format!("{}/src/database/tpch/1/{}.parquet", home_dir, file), ParquetReadOptions::new()).await?;
  }

  let args: Vec<String> = env::args().collect();
  if args.len() != 2 {
    println!("Usage: <file_path>");
    return Ok(());
  }

//   println!("Reading query from {}", args[1]);

  let query = match &args[1].starts_with("/") {
    true => fs::read_to_string(&args[1])?,
    false => args[1].clone(),
  };

  let time = Instant::now();
  let df = ctx.sql(&query).await?;

  let rows = df.collect().await?;
  let num_rows: usize = rows.iter().map(|r| r.num_rows()).sum();
  
  let elapsed = time.elapsed();
  println!("Query took {}ms and returned {} rows", elapsed.as_millis(), num_rows);

  // df.show().await?;
  Ok(())
}

