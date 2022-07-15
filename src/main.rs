mod metadata;
mod scraping;

use metadata::request_service_metadata;
use std::error::Error;
use std::fs::{create_dir, File};
use std::io::{BufReader, Seek, SeekFrom, Write};
use tokio::task::JoinHandle;
use std::{env, io};
use std::path::Path;
use std::time::Instant;
use clap::Parser;
use console::{style};
use indicatif::{ProgressBar, ProgressStyle, HumanDuration};
use conv::*;
use geojson::{GeoJson};

#[derive(Parser,Debug)]
#[clap(author = "Steven Thomson", version = "0.0.1", about, long_about = None)]
struct ProgramArguments {
    #[clap(short, long, value_parser)]
    url: String,
    #[clap(short, long, value_parser, default_value_t = false)]
    accept_scrape: bool,
    #[clap(short ='r', long, value_parser, default_value_t = 5)]
    query_retires: i32,
    #[clap(short = 's', long, value_parser)]
    output_spatial_reference: Option<i32>,
    #[clap(short = 'd', long, value_parser, default_value_t = false)]
    format_date: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let args = ProgramArguments::parse();
    let result = request_service_metadata(
        args.url.as_str(),
        args.output_spatial_reference,
    ).await?;
    result.write_to_console()?;

    if !args.accept_scrape {
        print!("Proceed with scrape (y/n): ");
        io::stdout().flush()?;
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                if input.to_uppercase().trim() != "Y" {
                    println!("Got response of, {:?}", input.as_bytes());
                    println!("Decided to not scrape. Exiting program");
                    return Ok(())
                }
            },
            Err(_) => {
                println!("Error while reading user input. Exiting program");
                return Ok(())
            }
        }
    }
    let start = Instant::now();
    let mut fetch_worker_handles: Vec<JoinHandle<Result<File, Box<dyn Error + Sync + Send>>>> = vec![];
    let queries = result.queries()?;
    let query_count = queries.len();

    println!("{} Spawning fetch workers", style("[1/4]").bold().dim());
    for query in queries {
        let fields = result.fields.clone();
        let retries = 10;//args.query_retires.clone();
        let handle = tokio::spawn(async move {
            let client = reqwest::Client::new();
            let temp_file = scraping::fetch_query(
                &client,
                &query,
                &fields,
                retries,
            ).await?;
            Ok(temp_file)
        });
        fetch_worker_handles.push(handle);
    }

    println!("{} Creating output file", style("[2/4]").bold().dim());
    let output_path_sting = format!("{}/output_files", env::current_dir()?.display());
    let output_path = Path::new(output_path_sting.as_str());
    if !output_path.is_dir() {
        create_dir(output_path)?;
    }
    let output_filename = format!("{}/{}.geojson", output_path.display(), result.name);
    let mut output_file = File::create(output_filename)?;

    println!("{} Collecting fetch worker output", style("[3/3]").bold().dim());
    let progress_style = ProgressStyle::with_template(
        "{bar:80.cyan/blue} {pos:>7}/{len:7} {msg}"
    )?.progress_chars("##-");
    let progress_max = u64::value_from(query_count)?;
    let query_progress = ProgressBar::new(progress_max);
    query_progress.set_style(progress_style);
    query_progress.inc(0);
    let mut progress = 0;

    for handle in fetch_worker_handles {
        let result = handle.await?;
        progress += 1;
        query_progress.inc(1);
        query_progress.set_message(format!("Query #{}", progress));
        if let Err(error) = result {
            println!("Error from temp file fetch");
            return Err(error)
        }
        let mut temp_file = result.unwrap();
        temp_file.seek(SeekFrom::Start(0))?;
        let buffered_reader = BufReader::new(temp_file);
        let geojson = GeoJson::from_reader(buffered_reader)?;
        if let GeoJson::FeatureCollection(collection) = geojson {
            if output_file.stream_position()? == 0 {
                write!(output_file, "{{\"type\":\"FeatureCollection\",")?;
                if let Some(members) = collection.foreign_members {
                    if let Some(crs) = members.get("crs") {
                        write!(output_file, "\"crs\":{},", crs)?;
                    }
                }
                write!(output_file, "\"features\":[")?;
            }
            for feature in collection.features {
                write!(output_file, "{},", feature.to_string())?;
            }
        }
        output_file.seek(SeekFrom::Current(-1))?;
        write!(output_file, "]}}")?;
        output_file.sync_all()?;
    }
    query_progress.finish_and_clear();

    println!("Done! Took {}", HumanDuration(start.elapsed()));
    Ok(())
}
