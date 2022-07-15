use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::{fmt, io};
use serde::{Deserialize};
use serde_json::{json};
use reqwest::Url;
use tablestream::{Stream, col, Column};
use serde_aux::field_attributes::deserialize_string_from_number;

#[derive(Debug)]
pub(crate) enum RestServiceMetadataError {
    MissingOidField,
    InvalidResponse(String)
}

impl Display for RestServiceMetadataError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RestServiceMetadataError::MissingOidField => {
                write!(f, "Referenced missing OID field")
            }
            RestServiceMetadataError::InvalidResponse(message) => {
                write!(f, "Invalid Response: {}", message)
            }
        }
    }
}

impl Error for RestServiceMetadataError {}

#[derive(Debug, Clone, Deserialize)]
pub(crate) enum RestServiceGeometryType {
    #[serde(alias = "esriGeometryPoint")]
    Point,
    #[serde(alias = "esriGeometryMultipoint")]
    Multipoint,
    #[serde(alias = "esriGeometryPolyline")]
    Polyline,
    #[serde(alias = "esriGeometryPolygon")]
    Polygon,
    #[serde(alias = "esriGeometryEnvelope")]
    Envelope,
}

impl Display for RestServiceGeometryType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RestServiceGeometryType::Point => write!(f, "esriGeometryPoint"),
            RestServiceGeometryType::Multipoint => write!(f, "esriGeometryMultipoint"),
            RestServiceGeometryType::Polyline => write!(f, "esriGeometryPolyline"),
            RestServiceGeometryType::Polygon => write!(f, "esriGeometryPolygon"),
            RestServiceGeometryType::Envelope => write!(f, "esriGeometryEnvelope"),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub(crate) enum RestServiceFieldType {
    #[serde(alias = "esriFieldTypeBlob")]
    Blob,
    #[serde(alias = "esriFieldTypeDate")]
    Date,
    #[serde(alias = "esriFieldTypeDouble")]
    Double,
    #[serde(alias = "esriFieldTypeFloat")]
    Float,
    #[serde(alias = "esriFieldTypeGeometry")]
    Geometry,
    #[serde(alias = "esriFieldTypeGlobalID")]
    GlobalID,
    #[serde(alias = "esriFieldTypeGUID")]
    GUID,
    #[serde(alias = "esriFieldTypeInteger")]
    Integer,
    #[serde(alias = "esriFieldTypeOID")]
    OID,
    #[serde(alias = "esriFieldTypeRaster")]
    Raster,
    #[serde(alias = "esriFieldTypeSingle")]
    Single,
    #[serde(alias = "esriFieldTypeSmallInteger")]
    SmallInteger,
    #[serde(alias = "esriFieldTypeString")]
    String,
    #[serde(alias = "esriFieldTypeXML")]
    XML,
}

impl Display for RestServiceFieldType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            RestServiceFieldType::Blob => write!(f, "Blob"),
            RestServiceFieldType::Date => write!(f, "Date"),
            RestServiceFieldType::Double => write!(f, "Double"),
            RestServiceFieldType::Float => write!(f, "Float"),
            RestServiceFieldType::Geometry => write!(f, "Geometry"),
            RestServiceFieldType::GlobalID => write!(f, "GlobalID"),
            RestServiceFieldType::GUID => write!(f, "GUID"),
            RestServiceFieldType::Integer => write!(f, "Integer"),
            RestServiceFieldType::OID => write!(f, "OID"),
            RestServiceFieldType::Raster => write!(f, "Raster"),
            RestServiceFieldType::Single => write!(f, "Single"),
            RestServiceFieldType::SmallInteger => write!(f, "SmallInteger"),
            RestServiceFieldType::String => write!(f, "String"),
            RestServiceFieldType::XML => write!(f, "XML"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct CodedValue {
    pub(crate) name: String,
    #[serde(deserialize_with = "deserialize_string_from_number")]
    pub(crate) code: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum FieldDomain {
    Range {
        name: String,
        range: Vec<i32>
    },
    #[serde(alias = "codedValue")]
    Coded {
        #[serde(alias = "codedValues")]
        coded_values: Vec<CodedValue>
    },
    Inherited,
}

fn coded_to_map(coded_values: &Vec<CodedValue>) -> HashMap<String, String> {
    coded_values.iter()
        .map(|coded_value| (coded_value.code.to_owned(), coded_value.name.to_owned()))
        .collect()
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ServiceField {
    pub(crate) name: String,
    #[serde(alias = "type")]
    pub(crate) field_type: RestServiceFieldType,
    alias: String,
    pub(crate) domain: Option<FieldDomain>,
}

impl ServiceField {
    pub(crate) fn is_coded(&self) -> Option<HashMap<String, String>> {
        if let Some(domain) = &self.domain {
            if let FieldDomain::Coded { coded_values, .. } = domain {
                Some(coded_to_map(coded_values))
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct SpatialReference {
    #[serde(alias = "wkid")]
    wk_id: i32,
    #[serde(alias = "latestWkid")]
    latest_wk_id: i32
}

#[derive(Debug, Deserialize)]
pub(crate) struct RestServiceJsonMetadata {
    pub(crate) name: String,
    #[serde(alias = "maxRecordCount")]
    max_record_count: i32,
    #[serde(alias = "type")]
    server_type: String,
    #[serde(alias = "geometryType")]
    pub(crate) geo_type: RestServiceGeometryType,
    pub(crate) fields: Vec<ServiceField>,
    #[serde(alias = "objectIdField")]
    oid_field: Option<String>,
    #[serde(alias = "sourceSpatialReference")]
    source_spatial_reference: Option<SpatialReference>,
    #[serde(alias = "supportsPagination")]
    supports_pagination: Option<bool>,
    #[serde(alias = "supportsStatistics")]
    supports_statistics: Option<bool>,
    #[serde(alias = "advancedQueryCapabilities")]
    advanced_query_capabilities: Option<HashMap<String, bool>>
}

impl RestServiceJsonMetadata {
    fn supports_pagination(&self) -> bool {
        self.supports_pagination.unwrap_or(false) ||
            *self.advanced_query_capabilities.as_ref()
            .and_then( |aqc| aqc.get("supportsPagination"))
            .unwrap_or(&false)
    }

    fn supports_statistics(&self) -> bool {
        self.supports_statistics.unwrap_or(false) ||
            *self.advanced_query_capabilities.as_ref()
            .and_then(|aqc| aqc.get("supportsStatistics"))
            .unwrap_or(&false)
    }
}

#[derive(Debug)]
pub(crate) struct RestServiceMetadata {
    url: String,
    pub(crate) name: String,
    source_count: i32,
    max_record_count: i32,
    pagination_enabled: bool,
    server_type: String,
    pub(crate) geo_type: RestServiceGeometryType,
    pub(crate) fields: Vec<ServiceField>,
    oid_field: Option<String>,
    max_min_oid: Option<(i32, i32)>,
    source_spatial_reference: Option<i32>,
    output_spatial_reference: Option<i32>,
}

impl RestServiceMetadata {
    fn scrape_count(&self) -> i32 {
        if self.max_record_count <= 10000 { self.max_record_count } else { 10000 }
    }

    fn is_table(&self) -> bool {
        self.server_type == "TABLE"
    }

    fn incremental_oid(&self) -> bool {
        match self.oid_field {
            None => false,
            Some(_) => {
                if let Some(max_min) = self.max_min_oid {
                    let difference = max_min.0 - max_min.1 + 1;
                    self.source_count == difference
                } else {
                    false
                }
            }
        }
    }

    fn pagination_query(&self, query_index: i32) -> Result<String, Box<dyn Error + Send + Sync>> {
        let result_offset = format!("{}", query_index * self.scrape_count());
        let result_record_count = format!("{}", self.scrape_count());
        let mut geometry_options = self.geometry_options()?;
        let mut url_params = vec![
            ("where", String::from("1=1")),
            ("resultOffset", result_offset),
            ("resultRecordCount", result_record_count),
            ("outFields", String::from("*")),
            ("f", String::from("geojson")),
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

    fn oid_query(&self, query_index: i32) -> Result<String, Box<dyn Error + Send + Sync>> {
        let oid_field_name = self.oid_field
            .to_owned()
            .ok_or(Box::new(RestServiceMetadataError::MissingOidField))?;
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
            ("f", String::from("geojson")),
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
        let mut remaining_records_count = self.source_count;
        let mut query_index = 0;
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
        println!("Feature Count: {}", self.source_count);
        println!("Max Scrape Chunk Count: {}", self.max_record_count);
        println!("Server Type: {}", self.server_type);
        if !self.is_table() {
            println!("Geometry Type: {}", self.geo_type);
        }
        let mut out = io::stdout();
        let mut stream = Stream::new(
            &mut out,
            vec![
                col!(ServiceField: .name).header("Name"),
                col!(ServiceField: .field_type).header("Type"),
                col!(ServiceField: .alias).header("Alias"),
                Column::new(|f, c: &ServiceField| {
                    if let Some(domain) = &c.domain {
                        match domain {
                            FieldDomain::Coded { .. } => write!(f, "{}", true),
                            _ => write!(f, "{}", false),
                        }
                    } else {
                        write!(f, "{}", false)
                    }
                }).header("Is Coded?"),
            ],
        );
        for field in self.fields.iter() {
            stream.row(field.to_owned())?;
        }
        stream.finish()?;
        if let Some(oid_field) = &self.oid_field {
            println!("OID Field: {}", oid_field);
        }
        if let Some(max_min_oids) = &self.max_min_oid {
            println!("Max Min OID: {:?}", max_min_oids);
            println!("Incremental OID: {}", self.incremental_oid());
        }
        if let Some(reference) = &self.source_spatial_reference {
            println!("Service Spatial Reference: {}", reference);
        }
        if let Some(reference) = &self.output_spatial_reference {
            println!("Output Spatial Reference: {}", reference);
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct CountQueryResponse {
    count: i32
}

async fn get_service_count(
    client: &reqwest::Client,
    url: &str,
) -> Result<CountQueryResponse, Box<dyn Error+ Sync + Send>> {
    let count_url = Url::parse_with_params(
        format!("{}/query", url).as_str(),
        [("where", "1=1"), ("returnCountOnly", "true"), ("f", "json")],
    )?;
    let count_json: CountQueryResponse = client.get(count_url)
        .send()
        .await?
        .json()
        .await?;
    Ok(count_json)
}

async fn get_service_metadata(
    client: &reqwest::Client,
    url: &str,
) -> Result<RestServiceJsonMetadata, Box<dyn Error+ Sync + Send>> {
    let metadata_url = Url::parse_with_params(
        url,
        [("f", "json")],
    )?;
    let metadata_json: RestServiceJsonMetadata = client.get(metadata_url)
        .send()
        .await?
        .json()
        .await?;
    Ok(metadata_json)
}

#[derive(Debug, Deserialize)]
struct StatisticsResponseAttributes {
    #[serde(alias = "MAX_VALUE")]
    max: i32,
    #[serde(alias = "MIN_VALUE")]
    min: i32,
}

#[derive(Debug, Deserialize)]
struct StatisticsResponseFeature {
    attributes: StatisticsResponseAttributes,
}

#[derive(Debug, Deserialize)]
struct StatisticsResponse {
    features: Vec<StatisticsResponseFeature>,
}

fn out_statistics_parameter(oid_field_name: &str) -> String {
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
    oid_field_name: &str,
    stats_enabled: bool,
) -> Result<Option<(i32, i32)>, Box<dyn Error + Sync + Send>> {
    let result = if stats_enabled {
        get_service_max_min_stats(&client, url, oid_field_name).await?
    } else {
        get_service_max_min_oid(&client, url).await?
    };
    Ok(result)
}

#[derive(Debug, Deserialize)]
struct ObjectIdsResponse {
    #[serde(alias = "objectIds")]
    object_ids: Vec<i32>,
}

async fn get_object_ids_response(
    client: &reqwest::Client,
    url: &str,
) -> Result<ObjectIdsResponse, Box<dyn Error + Sync + Send>> {
    let max_min_url = Url::parse_with_params(
        format!("{}/query", url).as_str(),
        [("where","1=1"),("returnIdsOnly","true"),("f","json")],
    )?;
    let max_min_json = client.get(max_min_url)
        .send()
        .await?
        .json()
        .await?;
    return Ok(max_min_json);
}

async fn get_service_max_min_oid(
    client: &reqwest::Client,
    url: &str,
) -> Result<Option<(i32, i32)>, Box<dyn Error + Sync + Send>> {
    let max_min_json = get_object_ids_response(client, url).await?;
    Ok(Some((
        max_min_json.object_ids[max_min_json.object_ids.len() - 1],
        max_min_json.object_ids[0],
    )))
}

async fn get_service_max_min_stats(
    client: &reqwest::Client,
    url: &str,
    oid_field_name: &str,
) -> Result<Option<(i32, i32)>, Box<dyn Error + Sync + Send>> {
    let out_statistics = out_statistics_parameter(oid_field_name);
    let max_min_url = Url::parse_with_params(
        format!("{}/query", url).as_str(),
        [("outStatistics", out_statistics.as_str()), ("f", "json")],
    )?;
    let max_min_json: StatisticsResponse = client.get(max_min_url)
        .send()
        .await?
        .json()
        .await?;
    if max_min_json.features.is_empty() {
        return Err(
            Box::new(
                RestServiceMetadataError::InvalidResponse(
                    "No features in max min response".to_owned(),
                )
            )
        )
    }
    let feature = &max_min_json.features[0];
    Ok(Some((
        feature.attributes.max,
        feature.attributes.min,
    )))
}

pub(crate) async fn request_service_metadata(
    url: &str,
    output_spatial_reference: Option<i32>,
) -> Result<RestServiceMetadata, Box<dyn Error + Sync + Send>> {
    let client = reqwest::Client::new();
    let source_count = get_service_count(&client, url).await?;
    let metadata_json = get_service_metadata(&client, url).await?;
    let oid_field = if let Some(ref oid_field_name) = metadata_json.oid_field {
        metadata_json.fields.iter()
            .find(|field| field.name == *oid_field_name)
            .map(|field| field.name.to_owned())
    } else {
        metadata_json.fields.iter()
            .find(|field| field.field_type == RestServiceFieldType::OID)
            .map(|field| field.name.to_owned())
    };
    let pagination_enabled = metadata_json.supports_pagination();
    let max_min_oid = if !pagination_enabled {
        match oid_field {
            Some(ref oid) => {
                get_service_max_min(
                    &client,
                    url,
                    oid.as_str(),
                    metadata_json.supports_statistics(),
                ).await?
            },
            None => None,
        }
    } else {
        None
    };
    let rest_metadata = RestServiceMetadata {
        url: url.to_owned(),
        name: metadata_json.name,
        source_count: source_count.count,
        max_record_count: metadata_json.max_record_count,
        pagination_enabled,
        server_type: metadata_json.server_type,
        geo_type: metadata_json.geo_type,
        fields: metadata_json.fields,
        oid_field,
        max_min_oid,
        source_spatial_reference: metadata_json.source_spatial_reference
            .map(|sr| sr.wk_id),
        output_spatial_reference,
    };
    Ok(rest_metadata)
}
