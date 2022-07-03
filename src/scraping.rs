use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{Write};
use std::time::Duration;
use reqwest::{Client, StatusCode};
use serde_json::{json, Map, Value};
use crate::metadata::{RestServiceField, RestServiceFieldType, RestServiceGeometryType};

#[derive(Debug, PartialEq)]
pub(crate) enum RestServiceScrapingError {
    MissingKey(String, String),
    InvalidResponse(StatusCode),
    InvalidJsonResponse(String),
    ErrorJsonResponse(String),
    UnknownJsonResponse(String),
    TooManyRetires(i32),
    InvalidFeature(String),
}

impl Display for RestServiceScrapingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RestServiceScrapingError::MissingKey(key, raw_json) => {
                write!(f, "Key: {}\nRaw JSON:\n{}", key, raw_json)
            }
            RestServiceScrapingError::InvalidResponse(status_code) => {
                write!(f, "Status Code: {}", status_code.as_str())
            }
            RestServiceScrapingError::InvalidJsonResponse(raw_json) => {
                write!(f, "Raw JSON:\n{}", raw_json)
            }
            RestServiceScrapingError::ErrorJsonResponse(raw_json) => {
                write!(f, "Raw JSON:\n{}", raw_json)
            }
            RestServiceScrapingError::UnknownJsonResponse(raw_json) => {
                write!(f, "Raw JSON:\n{}", raw_json)
            }
            RestServiceScrapingError::TooManyRetires(max_tries) => {
                write!(f, "Exceeded max tries of {}", max_tries)
            }
            RestServiceScrapingError::InvalidFeature(raw_json) => {
                write!(f, "Raw JSON:\n{}", raw_json)
            }
        }
    }
}

impl Error for RestServiceScrapingError {}

fn convert_json_value(json_value: &Value) -> Result<String, RestServiceScrapingError> {
    match json_value {
        Value::Null => Ok("".to_owned()),
        Value::Bool(boolean) => Ok(boolean.to_string().to_uppercase()),
        Value::Number(num) => {
            let number = if num.is_f64() {
                num.as_f64().unwrap().to_string()
            } else if num.is_i64() {
                num.as_i64().unwrap().to_string()
            } else {
                num.as_u64().unwrap().to_string()
            };
            Ok(number)
        }
        Value::String(string) => Ok(string.to_owned()),
        Value::Array(arr) => Ok(json!(arr).to_string()),
        Value::Object(obj) => Ok(json!(obj).to_string()),
    }
}

fn convert_json_field(
    field: &RestServiceField,
    json_value: &Value,
) -> Result<Vec<String>, RestServiceScrapingError> {
    if let Some(codes) = &field.codes {
        let code = convert_json_value(json_value)?;
        let value = codes.get(&code);
        if let Some(description) = value {
            Ok(vec![code, description.to_owned()])
        } else {
            Ok(vec![code, "".to_owned()])
        }
    } else {
        Ok(vec![convert_json_value(json_value)?])
    }
}

fn extract_geometry(
    feature: &Map<String, Value>,
    default_keys: Option<Vec<String>>,
) -> Map<String, Value> {
    let geometry = feature["geometry"].as_object();
    if let Some(obj) = geometry {
        return obj.to_owned()
    }
    let mut default_map = serde_json::Map::new();
    for key in default_keys.unwrap_or_default() {
        default_map.insert(key, Value::String("".to_owned()));
    }
    default_map
}

fn convert_geometry(
    geo_type: &RestServiceGeometryType,
    feature: &Map<String, Value>,
) -> Result<Vec<String>, RestServiceScrapingError> {
    match geo_type {
        RestServiceGeometryType::Point => {
            let geometry = extract_geometry(
                feature,
                Some(vec!["x".to_owned(), "y".to_owned()])
            );
            Ok(
                vec![
                    convert_json_value(&geometry["x"])?,
                    convert_json_value(&geometry["y"])?,
                ]
            )
        }
        RestServiceGeometryType::Multipoint => {
            let geometry = extract_geometry(
                feature,
                Some(vec!["points".to_owned()])
            );
            Ok(vec![convert_json_value(&geometry["points"])?])
        }
        RestServiceGeometryType::Polyline => {
            let geometry = extract_geometry(
                feature,
                Some(vec!["paths".to_owned()])
            );
            Ok(vec![convert_json_value(&geometry["paths"])?])
        }
        RestServiceGeometryType::Polygon => {
            let geometry = extract_geometry(
                feature,
                Some(vec!["rings".to_owned()])
            );
            Ok(vec![convert_json_value(&geometry["rings"])?])
        }
        RestServiceGeometryType::Envelope => {
            let geometry = extract_geometry(
                feature,
                Some(
                    vec![
                        "xmin".to_owned(),
                        "ymin".to_owned(),
                        "xmax".to_owned(),
                        "ymax".to_owned(),
                        "zmin".to_owned(),
                        "zmax".to_owned(),
                        "mmin".to_owned(),
                        "mmax".to_owned(),
                    ]
                )
            );
            let x_min = &geometry["xmin"].as_f64();
            let y_min = &geometry["ymin"].as_f64();
            let x_max = &geometry["xmax"].as_f64();
            let y_max = &geometry["ymax"].as_f64();
            let z_min = &geometry["zmin"].as_f64();
            let z_max = &geometry["zmax"].as_f64();
            let m_min = &geometry["mmin"].as_f64();
            let m_max = &geometry["mmax"].as_f64();
            let mut bounds_map = serde_json::Map::new();
            if let Some(val) = x_min {
                bounds_map.insert("xmin".to_owned(), Value::from(val.to_owned()));
            }
            if let Some(val) = y_min {
                bounds_map.insert("ymin".to_owned(), Value::from(val.to_owned()));
            }
            if let Some(val) = x_max {
                bounds_map.insert("xmax".to_owned(), Value::from(val.to_owned()));
            }
            if let Some(val) = y_max {
                bounds_map.insert("ymax".to_owned(), Value::from(val.to_owned()));
            }
            if let Some(val) = z_min {
                bounds_map.insert("zmin".to_owned(), Value::from(val.to_owned()));
            }
            if let Some(val) = z_max {
                bounds_map.insert("zmax".to_owned(), Value::from(val.to_owned()));
            }
            if let Some(val) = m_min {
                bounds_map.insert("mmin".to_owned(), Value::from(val.to_owned()));
            }
            if let Some(val) = m_max {
                bounds_map.insert("mmax".to_owned(), Value::from(val.to_owned()));
            }
            Ok(vec![convert_json_value(&Value::from(bounds_map))?])
        }
        RestServiceGeometryType::None => Ok(vec![])
    }
}

fn handle_record(
    fields: &Vec<RestServiceField>,
    geo_type: &RestServiceGeometryType,
    feature: &Map<String, Value>,
) -> Result<Vec<String>, RestServiceScrapingError> {
    let attributes = feature["attributes"]
        .as_object()
        .ok_or(
            RestServiceScrapingError::MissingKey(
                "attributes".to_owned(),
                format!("{:?}", feature),
            )
        )?;
    let mut record: Vec<String> = vec![];
    for field in fields {
        if field.field_type == RestServiceFieldType::Geometry {
            continue
        }
        let mut values = convert_json_field(
            &field,
            &attributes[field.name.as_str()]
        )?;
        record.append(&mut values);
    }
    for geometry_field in convert_geometry(geo_type, feature)? {
        record.push(geometry_field)
    }
    Ok(record)
}

pub(crate) fn handle_csv_value(value: &String) -> String {
    if value.chars().any(|chr| chr == '\r' || chr == '\n' || chr == ',' || chr == '"') {
        return format!("\"{}\"", value.replace("\"", "\"\""));
    }
    value.to_owned()
}

async fn try_query(
    client: &Client,
    query: &String,
) -> Result<Map<String, Value>, Box<dyn Error + Send + Sync>> {
    let response = client.get(query)
        .send()
        .await?;
    if response.status() != 200 {
        return Err(Box::new(RestServiceScrapingError::InvalidResponse(response.status())))
    }
    let json_response = response.json::<Value>()
        .await?;
    let json_object = json_response
        .as_object()
        .ok_or(
            Box::new(
                RestServiceScrapingError::InvalidJsonResponse(json_response.to_string())
            )
        )?;
    if !json_object.contains_key("features") {
        let erroneous_json = json_response.to_string();
        return if json_object.contains_key("error") {
            Err(Box::new(RestServiceScrapingError::ErrorJsonResponse(erroneous_json)))
        } else {
            Err(Box::new(RestServiceScrapingError::UnknownJsonResponse(erroneous_json)))
        }
    }
    Ok(json_object.to_owned())
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
                RestServiceScrapingError::ErrorJsonResponse(res) => {
                    *attempts += 1;
                    println!("Error JSON: {}", res);
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    println!("Trying request again");
                    Ok(())
                }
                RestServiceScrapingError::UnknownJsonResponse(res) => {
                    *attempts += 1;
                    println!("Error JSON: {}", res);
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    println!("Trying request again");
                    Ok(())
                }
                _ => return Err(error)
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
) -> Result<Map<String, Value>, Box<dyn Error + Send + Sync>> {
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
    fields: &Vec<RestServiceField>,
    geo_type: &RestServiceGeometryType,
    max_tries: i32,
) -> Result<File, Box<dyn Error + Send + Sync>> {
    let mut file = tempfile::tempfile()?;

    let json_response_object = loop_until_successful(
        client,
        query,
        max_tries,
    ).await?;
    let features = json_response_object["features"].as_array().unwrap();
    for feature_value in features {
        let feature = feature_value.as_object()
            .ok_or(
                Box::new(
                    RestServiceScrapingError::InvalidFeature(feature_value.to_string())
                )
            )?;
        let record = handle_record(fields, geo_type, feature)?;
        let record_transformed = record.iter()
            .map(|value| handle_csv_value(value))
            .collect::<Vec<String>>()
            .join(",");
        writeln!(&mut file, "{}", record_transformed)?;
    }
    file.sync_all()?;
    Ok(file)
}
