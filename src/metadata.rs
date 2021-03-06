use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use serde_json::{json, Value};
use reqwest::Url;
use tablestream::{Stream, col, Column};

#[derive(Debug, PartialEq)]
pub(crate) enum RestServiceMetadataError {
    FieldParsing(String, String),
    FieldTypeParsing(String),
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

impl Display for RestServiceFieldType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RestServiceFieldType::Blob => write!(f, "esriFieldTypeBlob"),
            RestServiceFieldType::Date => write!(f, "esriFieldTypeDate"),
            RestServiceFieldType::Double => write!(f, "esriFieldTypeDouble"),
            RestServiceFieldType::Float => write!(f, "esriFieldTypeFloat"),
            RestServiceFieldType::Geometry => write!(f, "esriFieldTypeGeometry"),
            RestServiceFieldType::GlobalID => write!(f, "esriFieldTypeGlobalID"),
            RestServiceFieldType::GUID => write!(f, "esriFieldTypeGUID"),
            RestServiceFieldType::Integer => write!(f, "esriFieldTypeInteger"),
            RestServiceFieldType::OID => write!(f, "esriFieldTypeOID"),
            RestServiceFieldType::Raster => write!(f, "esriFieldTypeRaster"),
            RestServiceFieldType::Single => write!(f, "esriFieldTypeSingle"),
            RestServiceFieldType::SmallInteger => write!(f, "esriFieldTypeSmallInteger"),
            RestServiceFieldType::String => write!(f, "esriFieldTypeString"),
            RestServiceFieldType::XML => write!(f, "esriFieldTypeXML"),
        }
    }
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
pub(crate) struct RestServiceField {
    pub(crate) name: String,
    pub(crate) field_type: RestServiceFieldType,
    alias: String,
    pub(crate) codes: Option<HashMap<String, String>>,
}

fn parse_domain(
    domain_value: &Value,
) -> Result<Option<HashMap<String, String>>, RestServiceMetadataError> {
    match domain_value {
        Value::Object(domain) => {
            let is_code = domain["type"].as_str().unwrap_or_default() == "codedValue";
            if !is_code {
                return Ok(None)
            }
            let coded_values = domain["codedValues"].as_array()
                .ok_or(
                    RestServiceMetadataError::FieldParsing(
                        "codedValues value is not an object".to_owned(),
                        domain["codedValues"].to_string().to_owned(),
                    )
                )?;
            let mut codes = HashMap::new();
            for coded_value in coded_values {
                let coded_value_obj = coded_value.as_object()
                    .ok_or(
                        RestServiceMetadataError::FieldParsing(
                            "codedValues value is not an object".to_owned(),
                            domain["codedValues"].to_string().to_owned(),
                        )
                    )?;
                let code = match &coded_value_obj["code"] {
                    Value::Number(num) => num.to_string(),
                    Value::String(string) => string.to_owned(),
                    _ => return Err(
                        RestServiceMetadataError::FieldParsing(
                            "Expected Number or String for code Value".to_owned(),
                            coded_value_obj["code"].to_string(),
                        )
                    ),
                };
                let name = match &coded_value_obj["name"] {
                    Value::String(string) => string.to_owned(),
                    _ => return Err(
                        RestServiceMetadataError::FieldParsing(
                            "Expected String for name Value".to_owned(),
                            coded_value_obj["code"].to_string(),
                        )
                    ),
                };
                codes.insert(code, name);
            }
            Ok(Some(codes))
        },
        _ => Ok(None)
    }
}

impl RestServiceField {
    fn new(field: &Value) -> Result<Self, RestServiceMetadataError> {
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
        let domain_value = &field["domain"];
        let codes = parse_domain(domain_value)?;
        let result = Self {
            name: field_name.to_owned(),
            field_type: field_type_enum,
            alias: field_alias.to_owned(),
            codes,
        };
        Ok(result)
    }

    fn for_geometry(name: &str) -> RestServiceField {
        RestServiceField {
            name: name.to_owned(),
            field_type: RestServiceFieldType::Geometry,
            alias: name.to_owned(),
            codes: None
        }
    }
}

#[derive(Debug)]
pub(crate) struct RestServiceMetadata {
    url: String,
    pub(crate) name: String,
    source_count: Option<i64>,
    max_record_count: i64,
    pagination_enabled: bool,
    server_type: String,
    pub(crate) geo_type: RestServiceGeometryType,
    pub(crate) fields: Vec<RestServiceField>,
    oid_field: Option<RestServiceField>,
    max_min_oid: Option<(i64, i64)>,
    source_spatial_reference: Option<i64>,
    output_spatial_reference: Option<i64>,
}

impl RestServiceMetadata {
    fn scrape_count(&self) -> i64 {
        if self.max_record_count <= 10000 { self.max_record_count } else { 10000 }
    }

    fn is_table(&self) -> bool {
        self.server_type == "TABLE"
    }

    fn incremental_oid(&self) -> bool {
        if self.oid_field.is_none() {
            return false;
        }
        if let Some(max_min) = self.max_min_oid {
            let difference = max_min.0 - max_min.1 + 1;
            self.source_count.map(|count| difference == count).unwrap_or(false)
        } else {
            false
        }
    }

    fn pagination_query(&self, query_index: i64) -> Result<String, Box<dyn Error + Send + Sync>> {
        let result_offset = format!("{}", query_index * self.scrape_count());
        let result_record_count = format!("{}", self.scrape_count());
        let mut geometry_options = self.geometry_options()?;
        let mut url_params = vec![
            ("where", String::from("1=1")),
            ("resultOffset", result_offset),
            ("resultRecordCount", result_record_count),
            ("outFields", String::from("*")),
            ("f", String::from("json")),
        ];
        url_params.append(&mut geometry_options);
        let url = Url::parse_with_params(
            format!("{}/query", self.url).as_str(),
            url_params,
        )?;
        Ok(url.to_string())
    }

    fn geometry_options(&self) -> Result<Vec<(&str, String)>, &str> {
        if self.is_table() {
            Ok(vec![])
        } else {
            let geometry_type = self.geo_type.to_string();
            let out_spatial_reference = self.output_spatial_reference
                .unwrap_or(
                    self.source_spatial_reference.ok_or(
                        "No source spatial reference and no output spatial reference specified"
                    )?
                )
                .to_string();
            Ok(vec![
                ("geometryType", geometry_type),
                ("outSR", out_spatial_reference),
            ])
        }
    }

    fn oid_query(&self, query_index: i64) -> Result<String, Box<dyn Error + Send + Sync>> {
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
        let mut geometry_options = self.geometry_options()?;
        let mut url_params = vec![
            ("where", where_clause),
            ("outFields", String::from("*")),
            ("f", String::from("json")),
        ];
        url_params.append(&mut geometry_options);
        let url = Url::parse_with_params(
            format!("{}/query", self.url).as_str(),
            url_params,
        )?;
        Ok(url.to_string())
    }

    pub(crate) fn queries(&self) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        if !self.pagination_enabled && self.oid_field.is_none() {
            return Err(Box::new(RestServiceMetadataError::MissingOidField))
        }
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

    pub(crate) fn write_to_console(&self) -> io::Result<()> {
        println!("URL: {}", self.url);
        println!("Name: {}", self.name);
        println!("Feature Count: {}", self.source_count.unwrap_or(-1));
        println!("Max Scrape Chunk Count: {}", self.max_record_count);
        println!("Server Type: {}", self.server_type);
        if !self.is_table() {
            println!("Geometry Type: {}", self.geo_type);
        }
        let mut out = io::stdout();
        let mut stream = Stream::new(
            &mut out,
            vec![
                col!(RestServiceField: .name).header("Name"),
                col!(RestServiceField: .field_type).header("Type"),
                col!(RestServiceField: .alias).header("Alias"),
                Column::new(|f, c: &RestServiceField| {
                    write!(f, "{}", &c.codes.is_some())
                }).header("Is Coded?"),
            ],
        );
        for field in self.fields.iter() {
            stream.row(field.to_owned())?;
        }
        stream.finish()?;
        if let Some(oid_field) = &self.oid_field {
            println!("OID Field: {}", oid_field.name);
        }
        if let Some(reference) = &self.source_spatial_reference {
            println!("Service Spatial Reference: {}", reference);
        }
        Ok(())
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
            fields.push(RestServiceField::for_geometry("X"));
            fields.push(RestServiceField::for_geometry("Y"));
        }
        RestServiceGeometryType::Multipoint => {
            fields.push(RestServiceField::for_geometry("POINTS"))
        },
        RestServiceGeometryType::Polygon => {
            fields.push(RestServiceField::for_geometry("RINGS"))
        },
        RestServiceGeometryType::Polyline => {
            fields.push(RestServiceField::for_geometry("PATHS"))
        },
        RestServiceGeometryType::Envelope => {
            fields.push(RestServiceField::for_geometry("ENVELOPE"))
        },
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
) -> Result<Option<i64>, Box<dyn Error+ Sync + Send>> {
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
) -> Result<Value, Box<dyn Error+ Sync + Send>> {
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
) -> Result<Option<(i64, i64)>, Box<dyn Error + Sync + Send>> {
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
) -> Result<Option<(i64, i64)>, Box<dyn Error + Sync + Send>> {
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
) -> Result<Option<(i64, i64)>, Box<dyn Error + Sync + Send>> {
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
    output_spatial_reference: Option<i64>,
) -> Result<RestServiceMetadata, Box<dyn Error + Sync + Send>> {
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
    let rest_metadata = RestServiceMetadata {
        url: url.to_owned(),
        name,
        source_count,
        max_record_count,
        pagination_enabled,
        server_type,
        geo_type,
        fields,
        oid_field,
        max_min_oid,
        source_spatial_reference: spatial_reference,
        output_spatial_reference,
    };
    Ok(rest_metadata)
}
