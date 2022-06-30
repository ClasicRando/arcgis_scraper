use std::error::Error;
use std::fmt::{Display, Formatter};
use serde_json::{json, Value};
use reqwest::Url;

#[derive(Debug, PartialEq)]
pub(crate) enum RestServiceMetadataError {
    FieldParsing(String, String),
    FieldTypeParsing(String),
    GeometryTypeParsing(String),
    MissingKey(String),
    MissingOidField,
}

impl Display for RestServiceMetadataError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RestServiceMetadataError::FieldParsing(message, field_json) => {
                write!(f, "Message:\n{}\nRaw JSON:\n{}", message, field_json)
            }
            RestServiceMetadataError::FieldTypeParsing(field_type) => {
                write!(f, "Invalid Field Type: {}", field_type)
            }
            RestServiceMetadataError::GeometryTypeParsing(geo_type) => {
                write!(f, "Invalid Geometry Type: {}", geo_type)
            }
            RestServiceMetadataError::MissingKey(key) => {
                write!(f, "Missing required key: {}", key)
            }
            RestServiceMetadataError::MissingOidField => {
                write!(f, "Referenced missing OID field")
            }
        }
    }
}

impl Error for RestServiceMetadataError {}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum RestServiceGeometryType {
    Point,
    Multipoint,
    Polyline,
    Polygon,
    Envelope,
    None,
}

impl Display for RestServiceGeometryType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RestServiceGeometryType::Point => write!(f, "esriGeometryPoint"),
            RestServiceGeometryType::Multipoint => write!(f, "esriGeometryMultipoint"),
            RestServiceGeometryType::Polyline => write!(f, "esriGeometryPolyline"),
            RestServiceGeometryType::Polygon => write!(f, "esriGeometryPolygon"),
            RestServiceGeometryType::Envelope => write!(f, "esriGeometryEnvelope"),
            RestServiceGeometryType::None => write!(f, "esriGeometryNone"),
        }
    }
}

impl RestServiceGeometryType {
    pub(crate) fn from_str(
        geo_type: &str
    ) -> Result<RestServiceGeometryType, RestServiceMetadataError> {
        match geo_type {
            "esriGeometryPoint" => Ok(RestServiceGeometryType::Point),
            "esriGeometryMultipoint" => Ok(RestServiceGeometryType::Multipoint),
            "esriGeometryPolyline" => Ok(RestServiceGeometryType::Polyline),
            "esriGeometryPolygon" => Ok(RestServiceGeometryType::Polygon),
            "esriGeometryEnvelope" => Ok(RestServiceGeometryType::Envelope),
            _ => Err(
                RestServiceMetadataError::FieldTypeParsing(
                    format!("Could not decode the geometry type of \"{}\"", geo_type)
                )
            )
        }
    }
}

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
    pub(crate) fn from_str(
        field_type: &str,
    ) -> Result<RestServiceFieldType, RestServiceMetadataError> {
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
            _ => Err(
                RestServiceMetadataError::FieldTypeParsing(
                    format!("Could not decode the field type of \"{}\"", field_type)
                )
            )
        }
    }
}

#[cfg(test)]
mod rest_service_field_type_tests {
    use super::{RestServiceFieldType, RestServiceMetadataError};

    #[test]
    fn from_str_should_return_blob_when_passed_blob_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeBlob")?;
        assert_eq!(result, RestServiceFieldType::Blob);
        Ok(())
    }

    #[test]
    fn from_str_should_return_data_when_passed_data_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeDate")?;
        assert_eq!(result, RestServiceFieldType::Date);
        Ok(())
    }

    #[test]
    fn from_str_should_return_double_when_passed_double_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeDouble")?;
        assert_eq!(result, RestServiceFieldType::Double);
        Ok(())
    }

    #[test]
    fn from_str_should_return_float_when_passed_float_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeFloat")?;
        assert_eq!(result, RestServiceFieldType::Float);
        Ok(())
    }

    #[test]
    fn from_str_should_return_geometry_when_passed_geometry_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeGeometry")?;
        assert_eq!(result, RestServiceFieldType::Geometry);
        Ok(())
    }

    #[test]
    fn from_str_should_return_global_id_when_passed_global_id_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeGlobalID")?;
        assert_eq!(result, RestServiceFieldType::GlobalID);
        Ok(())
    }

    #[test]
    fn from_str_should_return_guid_when_passed_guid_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeGUID")?;
        assert_eq!(result, RestServiceFieldType::GUID);
        Ok(())
    }

    #[test]
    fn from_str_should_return_integer_when_passed_integer_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeInteger")?;
        assert_eq!(result, RestServiceFieldType::Integer);
        Ok(())
    }

    #[test]
    fn from_str_should_return_oid_when_passed_oid_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeOID")?;
        assert_eq!(result, RestServiceFieldType::OID);
        Ok(())
    }

    #[test]
    fn from_str_should_return_raster_when_passed_raster_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeRaster")?;
        assert_eq!(result, RestServiceFieldType::Raster);
        Ok(())
    }

    #[test]
    fn from_str_should_return_single_when_passed_single_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeSingle")?;
        assert_eq!(result, RestServiceFieldType::Single);
        Ok(())
    }

    #[test]
    fn from_str_should_return_small_integer_when_passed_small_integer_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeSmallInteger")?;
        assert_eq!(result, RestServiceFieldType::SmallInteger);
        Ok(())
    }

    #[test]
    fn from_str_should_return_string_when_passed_string_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeString")?;
        assert_eq!(result, RestServiceFieldType::String);
        Ok(())
    }

    #[test]
    fn from_str_should_return_xml_when_passed_xml_field_type() -> Result<(), RestServiceMetadataError> {
        let result = RestServiceFieldType::from_str("esriFieldTypeXML")?;
        assert_eq!(result, RestServiceFieldType::XML);
        Ok(())
    }

    #[test]
    fn from_str_should_fail_when_passed_invalid_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeUnknown");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            RestServiceMetadataError::FieldTypeParsing(
                "Could not decode the field type of \"esriFieldTypeUnknown\"".to_owned()
            ),
        );
    }
}

#[derive(Debug, Clone)]
pub struct RestServiceField {
    name: String,
    field_type: RestServiceFieldType,
    alias: String,
}

impl RestServiceField {
    fn new(field: &Value) -> Result<RestServiceField, RestServiceMetadataError> {
        let field_name = field["name"]
            .as_str()
            .ok_or(
                RestServiceMetadataError::FieldParsing(
                    "No name found".to_owned(),
                    field.to_owned().to_string(),
                )
            )?;
        let field_type = field["type"]
            .as_str()
            .ok_or(
                RestServiceMetadataError::FieldParsing(
                    "No type found".to_owned(),
                    field.to_owned().to_string(),
                )
            )?;
        let field_alias = field["alias"]
            .as_str()
            .ok_or(
                RestServiceMetadataError::FieldParsing(
                    "No alias found".to_owned(),
                    field.to_owned().to_string(),
                )
            )?;
        let field_type_enum = RestServiceFieldType::from_str(field_type)?;
        let result = RestServiceField {
            name: field_name.to_owned(),
            field_type: field_type_enum,
            alias: field_alias.to_owned(),
        };
        Ok(result)
    }
}

#[derive(Debug)]
pub(crate) struct RestServiceMetadata {
    url: String,
    name: String,
    source_count: Option<i64>,
    max_record_count: i64,
    stats_enabled: bool,
    pagination_enabled: bool,
    server_type: String,
    geo_type: RestServiceGeometryType,
    fields: Vec<RestServiceField>,
    oid_field: Option<RestServiceField>,
    max_min_oid: Option<(i64, i64)>,
    incremental_oid: Option<bool>,
    spatial_reference: Option<i64>,
}

impl RestServiceMetadata {
    fn scrape_count(&self) -> i64 {
        if self.max_record_count <= 10000 { self.max_record_count } else { 10000 }
    }

    fn is_table(&self) -> bool {
        self.server_type == "TABLE"
    }

    fn pagination_query(&self, query_index: i64) -> Result<String, Box<dyn Error>> {
        let result_offset = format!("{}", query_index * self.scrape_count());
        let result_record_count = format!("{}", self.scrape_count());
        let geometry_type = self.geo_type.to_string();
        let out_spatial_reference = self.spatial_reference.unwrap_or(4269).to_string();
        let static_options = vec![
            ("where", "1=1"),
            ("resultOffset", result_offset.as_str()),
            ("resultRecordCount", result_record_count.as_str()),
            ("geometryType", geometry_type.as_str()),
            ("outSR", out_spatial_reference.as_str()),
            ("outFields", "*"),
            ("f", "json"),
        ];
        let url = Url::parse_with_params(
            format!("{}/query", self.url).as_str(),
            static_options,
        )?;
        Ok(url.to_string())
    }

    fn oid_query(&self, query_index: i64) -> Result<String, Box<dyn Error>> {
        let oid_field_name = self.oid_field
            .to_owned()
            .ok_or(Box::new(RestServiceMetadataError::MissingOidField))?
            .name;
        let min_oid = self.max_min_oid
            .ok_or(Box::new(RestServiceMetadataError::MissingOidField))?
            .1;
        let lower_bound = min_oid + (query_index * self.scrape_count());
        let where_clause = format!(
            "{} >= {} and {} <= {}",
            oid_field_name,
            lower_bound,
            oid_field_name,
            lower_bound + self.scrape_count() - 1,
        );
        let geometry_type = self.geo_type.to_string();
        let out_spatial_reference = self.spatial_reference
            .unwrap_or(4269)
            .to_string();
        let static_options = vec![
            ("where", where_clause.as_str()),
            ("geometryType", geometry_type.as_str()),
            ("outSR", out_spatial_reference.as_str()),
            ("outFields", "*"),
            ("f", "json"),
        ];
        let url = Url::parse_with_params(
            format!("{}/query", self.url).as_str(),
            static_options,
        )?;
        Ok(url.to_string())
    }

    fn queries(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let mut result: Vec<String> = vec![];
        let mut remaining_records_count = self.source_count
            .ok_or(Box::new(RestServiceMetadataError::MissingKey("count".to_owned())))?;
        let mut query_index = 0_i64;
        let scrape_chunk_count = self.scrape_count();
        while remaining_records_count > 0 {
            if self.pagination_enabled {
                result.push(self.pagination_query(query_index)?);
            } else {
                result.push(self.oid_query(query_index)?);
            }
            query_index += 1;
            remaining_records_count = if remaining_records_count > scrape_chunk_count {
                remaining_records_count - scrape_chunk_count
            } else {
                0
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod misc_tests {

    #[test]
    fn parse_fields_should_succeed_when_passed_valid_json_array() {

    }
}

fn parse_fields(
    fields_json: &Vec<Value>,
    geo_type: &RestServiceGeometryType,
) -> Result<Vec<RestServiceField>, RestServiceMetadataError> {
    let mut fields: Vec<RestServiceField> = fields_json.iter()
        .map(|field| RestServiceField::new(field))
        .collect::<Result<Vec<RestServiceField>, RestServiceMetadataError>>()?
        .into_iter()
        .filter(|field| field.field_type != RestServiceFieldType::Geometry)
        .filter(|field| field.name.to_uppercase() != "SHAPE")
        .collect();
    match geo_type {
        RestServiceGeometryType::Point => {
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
        RestServiceGeometryType::Multipoint => fields.push(RestServiceField {
            name: String::from("POINTS"),
            field_type: RestServiceFieldType::String,
            alias: String::from("Points"),
        }),
        RestServiceGeometryType::Polygon => fields.push(RestServiceField {
            name: String::from("RINGS"),
            field_type: RestServiceFieldType::String,
            alias: String::from("Rings"),
        }),
        RestServiceGeometryType::Polyline => fields.push(RestServiceField {
            name: String::from("PATHS"),
            field_type: RestServiceFieldType::String,
            alias: String::from("Paths"),
        }),
        RestServiceGeometryType::Envelope => fields.push(RestServiceField {
            name: String::from("ENVELOPE"),
            field_type: RestServiceFieldType::String,
            alias: String::from("Envelope"),
        }),
        _ => {}
    };
    Ok(fields)
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
) -> Result<Option<i64>, Box<dyn Error>> {
    let count_url = Url::parse_with_params(
        format!("{}/query", url).as_str(),
        [("where", "1=1"), ("returnCountOnly", "true"), ("f", "json")],
    )?;
    let count_json: Value = client.get(count_url)
        .send()
        .await?
        .json()
        .await?;
    Ok(count_json["count"].as_i64())
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
) -> Result<Option<(i64, i64)>, Box<dyn Error>> {
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
) -> Result<Option<(i64, i64)>, Box<dyn Error>> {
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
            let max = object_ids_json[object_ids_json.len() - 1].as_i64().unwrap_or_default();
            let min = object_ids_json[0].as_i64().unwrap_or_default();
            (max, min)
        });
    Ok(max_min_oid)
}

async fn get_service_max_min_stats(
    client: &reqwest::Client,
    url: &str,
    oid_field_name: String,
) -> Result<Option<(i64, i64)>, Box<dyn Error>> {
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
            attributes["MAX_VALUE"].as_i64().unwrap_or_default(),
            attributes["MIN_VALUE"].as_i64().unwrap_or_default()
        ));
    Ok(max_min_oid)
}

pub(crate) async fn request_service_metadata(
    url: &str,
) -> Result<RestServiceMetadata, Box<dyn Error>> {
    let client = reqwest::Client::new();
    let source_count = get_service_count(&client, url).await?;
    let metadata_json = get_service_metadata(&client, url).await?;
    let name = metadata_json["name"]
        .as_str()
        .ok_or(RestServiceMetadataError::MissingKey("name".to_owned()))?
        .to_owned();
    let max_record_count = metadata_json["maxRecordCount"]
        .as_i64()
        .ok_or(RestServiceMetadataError::MissingKey("maxRecordCount".to_owned()))?;
    let (stats_enabled, pagination_enabled) = advanced_options(&metadata_json);
    let server_type = metadata_json["type"]
        .as_str()
        .ok_or(RestServiceMetadataError::MissingKey("type[server]".to_owned()))?
        .to_owned();
    let geo_type = if server_type == "table" {
        RestServiceGeometryType::None
    } else {
        let geo_type_str = metadata_json["geometryType"]
            .as_str()
            .ok_or(RestServiceMetadataError::MissingKey("geometryType".to_owned()))?;
        RestServiceGeometryType::from_str(geo_type_str)?
    };
    let fields_json = metadata_json["fields"]
        .as_array()
        .ok_or(RestServiceMetadataError::MissingKey("fields".to_owned()))?;
    let fields = parse_fields(
        fields_json,
        &geo_type,
    )?;
    let oid_field = fields.iter()
        .find(|field| field.field_type == RestServiceFieldType::OID)
        .map(|field| field.to_owned());
    let spatial_reference = metadata_json["sourceSpatialReference"]
        .as_object()
        .and_then(|obj| obj["wkid"].as_i64());
    let max_min_oid = if !pagination_enabled && oid_field.is_some() {
        get_service_max_min(
            &client,
            url,
            oid_field.to_owned().unwrap().name,
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
        url: url.to_owned(),
        name,
        source_count,
        max_record_count,
        pagination_enabled,
        stats_enabled,
        server_type,
        geo_type,
        fields,
        oid_field,
        max_min_oid,
        incremental_oid,
        spatial_reference,
    };
    Ok(rest_metadata)
}