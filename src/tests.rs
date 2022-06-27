#[cfg(test)]
mod tests {
    use crate::metadata::RestServiceFieldType;

    #[test]
    fn from_str_should_return_blob_when_passed_blob_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeBlob");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::Blob);
    }

    #[test]
    fn from_str_should_return_data_when_passed_data_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeDate");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::Date);
    }

    #[test]
    fn from_str_should_return_double_when_passed_double_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeDouble");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::Double);
    }

    #[test]
    fn from_str_should_return_float_when_passed_float_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeFloat");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::Float);
    }

    #[test]
    fn from_str_should_return_geometry_when_passed_geometry_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeGeometry");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::Geometry);
    }

    #[test]
    fn from_str_should_return_global_id_when_passed_global_id_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeGlobalID");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::GlobalID);
    }

    #[test]
    fn from_str_should_return_guid_when_passed_guid_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeGUID");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::GUID);
    }

    #[test]
    fn from_str_should_return_integer_when_passed_integer_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeInteger");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::Integer);
    }

    #[test]
    fn from_str_should_return_oid_when_passed_oid_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeOID");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::OID);
    }

    #[test]
    fn from_str_should_return_raster_when_passed_raster_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeRaster");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::Raster);
    }

    #[test]
    fn from_str_should_return_single_when_passed_single_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeSingle");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::Single);
    }

    #[test]
    fn from_str_should_return_small_integer_when_passed_small_integer_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeSmallInteger");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::SmallInteger);
    }

    #[test]
    fn from_str_should_return_string_when_passed_string_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeString");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::String);
    }

    #[test]
    fn from_str_should_return_xml_when_passed_xml_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeXML");
        assert!(result.is_ok());
        let result_unwrapped = result.unwrap();
        assert_eq!(result_unwrapped, RestServiceFieldType::XML);
    }

    #[test]
    fn from_str_should_fail_when_passed_invalid_field_type() {
        let result = RestServiceFieldType::from_str("esriFieldTypeUnknown");
        assert!(result.is_err());
        let result_unwrapped = result.unwrap_err();
        assert_eq!(result_unwrapped, "Could not decode the field type");
    }
}