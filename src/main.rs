mod metadata;
mod scraping;

use metadata::request_service_metadata;
use std::error::Error;
use std::fs::{create_dir, File};
use std::io::{Read, Seek, SeekFrom, Write};
use chrono::{Utc};
use tokio::task::JoinHandle;
use std::env;
use std::path::Path;

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let start = Utc::now();
    let result = request_service_metadata(
        "https://maps.isgs.illinois.edu/arcgis/rest/services/ILOIL/Wells/MapServer/8",
    ).await?;
    let mut fetch_worker_handles: Vec<JoinHandle<Result<File, Box<dyn Error + Sync + Send>>>> = vec![];
    let queries = result.queries()?;

    for query in queries {
        let fields = result.fields.clone();
        let geo_type = result.geo_type.clone();
        let handle = tokio::spawn(async move {
            let client = reqwest::Client::new();
            let temp_file = scraping::fetch_query(
                &client,
                &query,
                &fields,
                &geo_type,
                5,
            ).await?;
            Ok(temp_file)
        });
        fetch_worker_handles.push(handle);
    }

    let output_path_sting = format!("{}/output_files", env::current_dir()?.display());
    let output_path = Path::new(output_path_sting.as_str());
    if !output_path.is_dir() {
        create_dir(output_path)?;
    }
    let output_filename = format!("{}/{}.csv", output_path.display(), result.name);
    let mut output_file = File::create(output_filename)?;
    let header_line = result.fields.iter()
        .map(|field|
            if field.codes.is_some() {
                vec![field.name.clone(), format!("{}_DESC", field.name)]
            } else {
                vec![field.name.clone()]
            }
        )
        .flatten()
        .collect::<Vec<String>>()
        .iter()
        .map(|name| scraping::handle_csv_value(name))
        .collect::<Vec<String>>()
        .join(",");
    writeln!(&mut output_file, "{}", header_line)?;

    for handle in fetch_worker_handles {
        let result = handle.await?;
        if let Err(error) = result {
            return Err(error)
        }
        let mut temp_file = result.unwrap();
        temp_file.seek(SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        if let Ok(_) = temp_file.read_to_end(&mut buffer) {
            output_file.write(&mut buffer)?;
        }
        output_file.sync_all()?;
    }

    let end = Utc::now();
    println!("Took {} ms", end.signed_duration_since(start).num_milliseconds());
    Ok(())
}
