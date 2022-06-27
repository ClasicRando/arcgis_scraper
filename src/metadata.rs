use std::error::Error;
use serde_json::{json, Value};
use reqwest::Url;

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum RestServiceFieldType {
    Blob,
    Date,
    Double,
    Float,
    Geometry,
    GlobalID,
    GUID,
    Integer,
    OID,
    Raster,
    Single,
    SmallInteger,
    String,
    XML,
}

impl RestServiceFieldType {
    pub(crate) fn from_str(field_type: &str) -> Result<RestServiceFieldType, &str> {
        match field_type {
            "esriFieldTypeBlob" => Ok(RestServiceFieldType::Blob),
            "esriFieldTypeDate" => Ok(RestServiceFieldType::Date),
            "esriFieldTypeDouble" => Ok(RestServiceFieldType::Double),
            "esriFieldTypeFloat" => Ok(RestServiceFieldType::Float),
            "esriFieldTypeGeometry" => Ok(RestServiceFieldType::Geometry),
            "esriFieldTypeGlobalID" => Ok(RestServiceFieldType::GlobalID),
            "esriFieldTypeGUID" => Ok(RestServiceFieldType::GUID),
            "esriFieldTypeInteger" => Ok(RestServiceFieldType::Integer),
            "esriFieldTypeOID" => Ok(RestServiceFieldType::OID),
            "esriFieldTypeRaster" => Ok(RestServiceFieldType::Raster),
            "esriFieldTypeSingle" => Ok(RestServiceFieldType::Single),
            "esriFieldTypeSmallInteger" => Ok(RestServiceFieldType::SmallInteger),
            "esriFieldTypeString" => Ok(RestServiceFieldType::String),
            "esriFieldTypeXML" => Ok(RestServiceFieldType::XML),
            _ => Err("Could not decode the field type")
        }
    }
}

#[derive(Debug, Clone)]
pub struct RestServiceField {
    name: String,
    field_type: RestServiceFieldType,
    alias: String,
}

impl RestServiceField {
    fn new(
        name: &str,
        field_type: &str,
        alias: &str,
    ) -> Result<RestServiceField, Box<dyn Error>> {
        let field_type_enum = RestServiceFieldType::from_str(field_type)?;
        let result = RestServiceField {
            name: name.to_string(),
            field_type: field_type_enum,
            alias: alias.to_string(),
        };
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct RestServiceMetadata {
    url: String,
    name: String,
    source_count: Option<u64>,
    max_record_count: Option<u64>,
    stats_enabled: bool,
    pagination_enabled: bool,
    server_type: String,
    geo_type: String,
    fields: Vec<RestServiceField>,
    oid_field: Option<RestServiceField>,
    max_min_oid: Option<(u64, u64)>,
    incremental_oid: Option<bool>,
    spatial_reference: Option<u64>,
}

fn field_is_not_shape(field: &Value) -> bool {
    field["name"].as_str().unwrap_or_default() != "Shape"
}

fn field_is_not_geometry(field: &Value) -> bool {
    field["type"].as_str().unwrap_or_default() != "esriFieldTypeGeometry"
}

fn parse_fields(fields_json: &Vec<Value>, geo_type: &str) -> Vec<RestServiceField> {
    let mut fields: Vec<RestServiceField> = fields_json.iter()
        .filter(|field| field_is_not_shape(field) && field_is_not_geometry(field))
        .flat_map(|field| RestServiceField::new(
            field["name"].as_str().unwrap_or_default(),
            field["type"].as_str().unwrap_or_default(),
            field["alias"].as_str().unwrap_or_default(),
        ))
        .collect();
    match geo_type {
        "esriGeometryPoint" => {
            fields.push(RestServiceField {
                name: String::from("X"),
                field_type: RestServiceFieldType::Double,
                alias: String::from("X"),
            });
            fields.push(RestServiceField {
                name: String::from("Y"),
                field_type: RestServiceFieldType::Double,
                alias: String::from("Y"),
            });
        }
        "esriGeometryMultipoint" => fields.push(RestServiceField {
            name: String::from("POINTS"),
            field_type: RestServiceFieldType::String,
            alias: String::from("Points"),
        }),
        "esriGeometryPolygon" => fields.push(RestServiceField {
            name: String::from("RINGS"),
            field_type: RestServiceFieldType::String,
            alias: String::from("Rings"),
        }),
        _ => {},
    };
    fields
}

fn advanced_options(metadata_json: &Value) -> (bool, bool) {
    metadata_json["advancedQueryCapabilities"]
        .as_object()
        .map_or(
            (
                metadata_json["supportsStatistics"].as_bool().unwrap_or_default(),
                metadata_json["supportsPagination"].as_bool().unwrap_or_default()
            ),
            |advanced_query| {
                let stats = advanced_query["supportsStatistics"]
                    .as_bool()
                    .unwrap_or(false);
                let pagination = advanced_query["supportsPagination"]
                    .as_bool()
                    .unwrap_or(false);
                (stats, pagination)
            }
        )
}

async fn get_service_count(
    client: &reqwest::Client,
    url: &str,
) -> Result<Option<u64>, Box<dyn Error>> {
    let count_url = Url::parse_with_params(
        format!("{}/query", url).as_str(),
        [("where", "1=1"), ("returnCountOnly", "true"), ("f", "json")],
    )?;
    let count_json: Value = client.get(count_url)
        .send()
        .await?
        .json()
        .await?;
    Ok(count_json["count"].as_u64())
}

async fn get_service_metadata(
    client: &reqwest::Client,
    url: &str,
) -> Result<Value, Box<dyn Error>> {
    let metadata_url = Url::parse_with_params(
        url,
        [("f", "json")],
    )?;
    let metadata_json: Value = client.get(metadata_url)
        .send()
        .await?
        .json()
        .await?;
    Ok(metadata_json)
}

fn out_statistics_parameter(oid_field_name: String) -> String {
    json!([
        {
            "statisticType": "max",
            "onStatisticField": oid_field_name,
            "outStatisticFieldName": "MAX_VALUE",
        },
        {
            "statisticType": "min",
            "onStatisticField": oid_field_name,
            "outStatisticFieldName": "MIN_VALUE",
        },
    ]).to_string()
}

async fn get_service_max_min(
    client: &reqwest::Client,
    url: &str,
    oid_field_name: String,
    stats_enabled: bool,
) -> Result<Option<(u64, u64)>, Box<dyn Error>> {
    let result = if stats_enabled {
        get_service_max_min_stats(&client, url, oid_field_name).await?
    } else {
        get_service_max_min_oid(&client, url).await?
    };
    Ok(result)
}

async fn get_service_max_min_oid(
    client: &reqwest::Client,
    url: &str,
) -> Result<Option<(u64, u64)>, Box<dyn Error>> {
    let max_min_url = Url::parse_with_params(
        format!("{}/query", url).as_str(),
        [("where","1=1"),("returnIdsOnly","true"),("f","json")],
    )?;
    let max_min_json: Value = client.get(max_min_url)
        .send()
        .await?
        .json()
        .await?;
    let max_min_oid = max_min_json["objectIds"]
        .as_array()
        .map(|object_ids_json| {
            let max = object_ids_json[object_ids_json.len() - 1].as_u64().unwrap_or_default();
            let min = object_ids_json[0].as_u64().unwrap_or_default();
            (max, min)
        });
    Ok(max_min_oid)
}

async fn get_service_max_min_stats(
    client: &reqwest::Client,
    url: &str,
    oid_field_name: String,
) -> Result<Option<(u64, u64)>, Box<dyn Error>> {
    let out_statistics = out_statistics_parameter(oid_field_name);
    let max_min_url = Url::parse_with_params(
        format!("{}/query", url).as_str(),
        [("outStatistics", out_statistics.as_str()), ("f", "json")],
    )?;
    let max_min_json: Value = client.get(max_min_url)
        .header("User-Agent", "Reqwest Rust Test")
        .send()
        .await?
        .json()
        .await?;
    let max_min_oid = max_min_json["features"]
        .as_array()
        .and_then(|features| if features.len() > 0 { Some(&features[0]) } else { None })
        .and_then(|feature| feature["attributes"].as_object())
        .map(|attributes| (
            attributes["MAX_VALUE"].as_u64().unwrap_or_default(),
            attributes["MIN_VALUE"].as_u64().unwrap_or_default()
        ));
    Ok(max_min_oid)
}

pub(crate) async fn request_service_metadata(
    url: &str,
) -> Result<RestServiceMetadata, Box<dyn Error>> {
    let client = reqwest::Client::new();
    let source_count = get_service_count(&client, url).await?;
    let metadata_json = get_service_metadata(&client, url).await?;
    let name = metadata_json["name"].as_str().unwrap_or_default().to_string();
    let max_record_count = metadata_json["maxRecordCount"]
        .as_u64();
    let (stats_enabled, pagination_enabled) = advanced_options(&metadata_json);
    let server_type = metadata_json["type"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let geo_type = metadata_json["geometryType"]
        .as_str()
        .unwrap_or_default();
    let fields = metadata_json["fields"]
        .as_array()
        .map(|fields| parse_fields(fields, geo_type))
        .unwrap_or_default();
    let oid_field = fields.iter()
        .find(|field| field.field_type == RestServiceFieldType::OID)
        .map(|field| field.clone());
    let spatial_reference = metadata_json["sourceSpatialReference"]
        .as_object()
        .and_then(|obj| obj["wkid"].as_u64());
    let max_min_oid = if !pagination_enabled && oid_field.is_some() {
        get_service_max_min(
            &client,
            url,
            oid_field.clone().unwrap().name,
            stats_enabled
        ).await?
    } else {
        None
    };
    let incremental_oid = if let Some(max_min) = max_min_oid {
        let difference = max_min.0 - max_min.1 + 1;
        source_count.map(|count| difference == count)
    } else {
        None
    };
    let rest_metadata = RestServiceMetadata {
        url: url.to_string(),
        name,
        source_count,
        max_record_count,
        pagination_enabled,
        stats_enabled,
        server_type,
        geo_type: geo_type.to_string(),
        fields,
        oid_field,
        max_min_oid,
        incremental_oid,
        spatial_reference,
    };
    Ok(rest_metadata)
}