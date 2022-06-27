mod metadata;
mod tests;

use metadata::request_service_metadata;
use std::error::Error;
use chrono::{Utc};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let start = Utc::now();
    let result = request_service_metadata(
        "https://maps.isgs.illinois.edu/arcgis/rest/services/ILOIL/Wells/MapServer/8",
    ).await?;
    let end = Utc::now();
    println!("Result:\n{:#?}", result);
    println!("Took {} ms", end.signed_duration_since(start).num_milliseconds());
    Ok(())
}
