use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{Write};
use std::time::Duration;
use geojson::{Feature, FeatureCollection, GeoJson, JsonObject};
use reqwest::{Client, StatusCode};
use serde_json::{Value};
use crate::metadata::{ServiceField, RestServiceFieldType};

#[derive(Debug, PartialEq)]
pub(crate) enum RestServiceScrapingError {
    InvalidResponse(StatusCode),
    InvalidJsonResponse(String),
    TooManyRetires(i32),
}

impl Display for RestServiceScrapingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RestServiceScrapingError::InvalidResponse(status_code) => {
                write!(f, "Status Code: {}", status_code.as_str())
            }
            RestServiceScrapingError::InvalidJsonResponse(message) => {
                write!(f, "Raw JSON:\n{}", message)
            }
            RestServiceScrapingError::TooManyRetires(max_tries) => {
                write!(f, "Exceeded max tries of {}", max_tries)
            }
        }
    }
}

impl Error for RestServiceScrapingError {}

pub(crate) fn convert_json_value(json_value: &Value) -> String {
    match json_value {
        Value::Null => "".to_owned(),
        Value::Bool(boolean) => boolean.to_string().to_uppercase(),
        Value::Number(num) => {
            let number = if num.is_f64() {
                num.as_f64().map(|f| f.to_string()).unwrap_or_default()
            } else if num.is_i64() {
                num.as_i64().map(|i| i.to_string()).unwrap_or_default()
            } else {
                num.as_u64().map(|u| u.to_string()).unwrap_or_default()
            };
            number
        }
        Value::String(string) => string.to_owned(),
        _ => json_value.to_string(),
    }
}

fn transform_properties(
    fields: &Vec<ServiceField>,
    properties: &JsonObject,
) -> JsonObject {
    let mut result = properties.clone();
    for field in fields {
        if field.field_type == RestServiceFieldType::Geometry {
            continue
        }
        let value = convert_json_value(&properties[field.name.as_str()]);
        result.insert(field.name.to_owned(), Value::String(value));
        if let Some(domain) = field.is_coded() {
            let coded_value = domain.get(field.name.as_str())
                .map(|value| value.to_owned())
                .unwrap_or(String::from(""));
            result.insert(format!("{}_DESC", field.name), Value::String(coded_value));
        }
    }
    result
}

async fn try_query(
    client: &Client,
    query: &String,
) -> Result<FeatureCollection, Box<dyn Error + Send + Sync>> {
    let response = client.get(query)
        .send()
        .await?;
    if response.status() != 200 {
        return Err(Box::new(RestServiceScrapingError::InvalidResponse(response.status())))
    }
    let geo_json: GeoJson = response.json()
        .await?;
    match geo_json {
        GeoJson::Geometry(_) => Err(
            Box::new(
                RestServiceScrapingError::InvalidJsonResponse(
                    "Expected a FeatureCollection but got a Geometry".to_owned()
                )
            )
        ),
        GeoJson::Feature(_) => Err(
            Box::new(
                RestServiceScrapingError::InvalidJsonResponse(
                    "Expected a FeatureCollection but got a Feature".to_owned()
                )
            )
        ),
        GeoJson::FeatureCollection(collection) => Ok(collection)
    }
}

async fn decode_fetch_error(
    attempts: &mut i32,
    error: Box<dyn Error + Send + Sync>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Request had an error...");
    match error.downcast_ref::<RestServiceScrapingError>() {
        Some(scraping_error) => {
            match scraping_error {
                RestServiceScrapingError::InvalidResponse(code) => {
                    *attempts += 1;
                    println!("Error Status Code: {}", code);
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    println!("Trying request again");
                    Ok(())
                }
                RestServiceScrapingError::InvalidJsonResponse(res) => {
                    *attempts += 1;
                    println!("Error JSON: {}", res);
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    println!("Trying request again");
                    Ok(())
                }
                _ => Err(error)
            }
        }
        None => {
            Err(error)
        }
    }
}

async fn loop_until_successful(
    client: &Client,
    query: &String,
    max_tries: i32,
) -> Result<FeatureCollection, Box<dyn Error + Send + Sync>> {
    let mut attempts = 0;
    let result = loop {
        match try_query(client, query).await {
            Err(error) => {
                match decode_fetch_error(&mut attempts, error).await {
                    Err(decode_error) => return Err(decode_error),
                    Ok(_) => {},
                }
            }
            Ok(obj) => break obj
        }
        if attempts >= max_tries {
            return Err(Box::new(RestServiceScrapingError::TooManyRetires(max_tries)))
        }
    };
    Ok(result)
}

pub(crate) async fn fetch_query(
    client: &Client,
    query: &String,
    fields: &Vec<ServiceField>,
    max_tries: i32,
) -> Result<File, Box<dyn Error + Send + Sync>> {
    let mut file = tempfile::tempfile()?;

    let feature_collection = loop_until_successful(
        client,
        query,
        max_tries,
    ).await?;
    let features: Vec<Feature> = feature_collection.features.into_iter()
        .map(|feature| {
            let new_properties = if let Some(properties) = &feature.properties {
                transform_properties(fields, &properties)
            } else {
                JsonObject::new()
            };
            Feature {
                bbox: feature.bbox.to_owned(),
                geometry: feature.geometry.to_owned(),
                id: feature.id.to_owned(),
                properties: Some(new_properties),
                foreign_members: None,
            }
        })
        .collect();
    let feature_collection = FeatureCollection {
        bbox: feature_collection.bbox,
        features,
        foreign_members: if let Some(member) = feature_collection.foreign_members {
            if let Some(crs) = member.get("crs") {
                let mut foreign_members = JsonObject::new();
                foreign_members.insert("crs".to_owned(), crs.to_owned());
                Some(foreign_members)
            } else {
                None
            }
        } else {
            None
        },
    };
    write!(&mut file, "{}", feature_collection.to_string())?;
    file.flush()?;
    Ok(file)
}
