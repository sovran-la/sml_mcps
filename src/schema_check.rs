//! A structural pre-flight check for tool `structuredContent`.
//!
//! When a tool declares an `outputSchema`, the spec makes a hard promise:
//!
//! > Servers **MUST** provide structured results that conform to this schema.
//!
//! This module enforces that promise cheaply, without pulling a full JSON
//! Schema implementation into a crate whose whole point is being small.
//!
//! **This is deliberately not a JSON Schema validator.** It checks the things
//! that are both cheap to check and overwhelmingly the actual bugs:
//!
//! - `structuredContent` is present at all
//! - the top-level `type` matches (`object`, `array`, `string`, ...)
//! - every entry in `required` is present
//! - each declared property's `type` matches, one level deep
//!
//! It does *not* check `pattern`, `minimum`, `format`, `additionalProperties`,
//! `$ref`, `oneOf`/`anyOf`/`allOf`, or anything nested more than one level. A
//! payload that passes here can still fail strict validation on the client.
//! Servers wanting full validation should validate before returning.
//!
//! Erring toward permissive is intentional: a false rejection would break a
//! working tool, while a false acceptance leaves the client exactly where it
//! would have been with no check at all.

use serde_json::Value;

/// Check `structured` against `schema`, returning a human-readable description
/// of the first problem found.
///
/// `Ok(())` means "nothing detectably wrong", not "fully valid" - see the
/// module docs.
pub fn validate_structured_output(
    schema: &Value,
    structured: Option<&Value>,
) -> Result<(), String> {
    let Some(structured) = structured else {
        return Err("no `structuredContent`".to_string());
    };

    check_value(schema, structured, "structuredContent")
}

/// Check one value against one (sub)schema.
fn check_value(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    // A schema we cannot interpret constrains nothing.
    let Some(schema) = schema.as_object() else {
        return Ok(());
    };

    // Composition keywords take over the meaning of the schema in ways this
    // checker does not model, so decline to judge rather than guess wrong.
    for keyword in ["oneOf", "anyOf", "allOf", "not", "if", "$ref"] {
        if schema.contains_key(keyword) {
            return Ok(());
        }
    }

    if let Some(expected) = schema.get("type") {
        if !type_matches(expected, value) {
            return Err(format!(
                "{} has type `{}`, expected `{}`",
                path,
                type_name(value),
                describe_expected(expected)
            ));
        }
    }

    if let Some(object) = value.as_object() {
        // Every entry in `required` must actually be there.
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for name in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(name) {
                    return Err(format!("{} is missing required property `{}`", path, name));
                }
            }
        }

        // Recurse one level into declared properties that are present.
        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (name, subschema) in properties {
                if let Some(subvalue) = object.get(name) {
                    check_value(subschema, subvalue, &format!("{}.{}", path, name))?;
                }
            }
        }
    }

    // Recurse into array items when a single item schema is given. Tuple-form
    // `items` (an array of schemas) is not modelled.
    if let (Some(array), Some(items)) = (value.as_array(), schema.get("items")) {
        if items.is_object() {
            for (index, element) in array.iter().enumerate() {
                check_value(items, element, &format!("{}[{}]", path, index))?;
            }
        }
    }

    Ok(())
}

/// Does `value` satisfy a schema `type`, which may be a string or an array of
/// strings?
fn type_matches(expected: &Value, value: &Value) -> bool {
    match expected {
        Value::String(name) => matches_type_name(name, value),
        Value::Array(names) => names
            .iter()
            .filter_map(Value::as_str)
            .any(|name| matches_type_name(name, value)),
        // Unrecognized `type` shape: do not judge.
        _ => true,
    }
}

fn matches_type_name(name: &str, value: &Value) -> bool {
    match name {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        // JSON Schema's `integer` accepts a float with zero fractional part.
        "integer" => value.as_i64().is_some() || value.as_u64().is_some() || is_integral_f64(value),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        // Unknown type keyword: do not judge.
        _ => true,
    }
}

fn is_integral_f64(value: &Value) -> bool {
    value.as_f64().is_some_and(|f| f.fract() == 0.0)
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn describe_expected(expected: &Value) -> String {
    match expected {
        Value::String(name) => name.clone(),
        Value::Array(names) => names
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" | "),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn weather_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "temperature": { "type": "number" },
                "conditions": { "type": "string" },
                "humidity": { "type": "number" }
            },
            "required": ["temperature", "conditions", "humidity"]
        })
    }

    #[test]
    fn accepts_a_conforming_payload() {
        let value = json!({ "temperature": 22.5, "conditions": "Partly cloudy", "humidity": 65 });
        assert!(validate_structured_output(&weather_schema(), Some(&value)).is_ok());
    }

    #[test]
    fn rejects_missing_structured_content() {
        let err = validate_structured_output(&weather_schema(), None).unwrap_err();
        assert!(err.contains("no `structuredContent`"), "{err}");
    }

    #[test]
    fn rejects_missing_required_property() {
        let value = json!({ "temperature": 22.5, "conditions": "Partly cloudy" });
        let err = validate_structured_output(&weather_schema(), Some(&value)).unwrap_err();
        assert!(err.contains("humidity"), "{err}");
        assert!(err.contains("required"), "{err}");
    }

    #[test]
    fn rejects_wrong_top_level_type() {
        let value = json!(["not", "an", "object"]);
        let err = validate_structured_output(&weather_schema(), Some(&value)).unwrap_err();
        assert!(err.contains("array"), "{err}");
        assert!(err.contains("object"), "{err}");
    }

    #[test]
    fn rejects_wrong_property_type() {
        let value = json!({ "temperature": "hot", "conditions": "Partly cloudy", "humidity": 65 });
        let err = validate_structured_output(&weather_schema(), Some(&value)).unwrap_err();
        assert!(err.contains("structuredContent.temperature"), "{err}");
        assert!(err.contains("string"), "{err}");
    }

    #[test]
    fn accepts_extra_properties() {
        // additionalProperties is not modelled; extras must not be rejected.
        let value = json!({
            "temperature": 22.5, "conditions": "Sunny", "humidity": 40, "extra": true
        });
        assert!(validate_structured_output(&weather_schema(), Some(&value)).is_ok());
    }

    #[test]
    fn integer_accepts_integral_floats_and_rejects_fractions() {
        let schema = json!({ "type": "object", "properties": { "n": { "type": "integer" } } });

        assert!(validate_structured_output(&schema, Some(&json!({ "n": 5 }))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!({ "n": 5.0 }))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!({ "n": 5.5 }))).is_err());
        assert!(validate_structured_output(&schema, Some(&json!({ "n": "5" }))).is_err());
    }

    #[test]
    fn number_accepts_both_integers_and_floats() {
        let schema = json!({ "type": "number" });
        assert!(validate_structured_output(&schema, Some(&json!(1))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!(1.5))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!("1"))).is_err());
    }

    #[test]
    fn union_types_accept_any_listed_member() {
        let schema = json!({ "type": ["string", "null"] });
        assert!(validate_structured_output(&schema, Some(&json!("hi"))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!(null))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!(3))).is_err());
    }

    #[test]
    fn checks_array_items_against_a_single_item_schema() {
        let schema = json!({ "type": "array", "items": { "type": "string" } });
        assert!(validate_structured_output(&schema, Some(&json!(["a", "b"]))).is_ok());

        let err = validate_structured_output(&schema, Some(&json!(["a", 2]))).unwrap_err();
        assert!(err.contains("structuredContent[1]"), "{err}");
    }

    #[test]
    fn recurses_into_nested_objects() {
        let schema = json!({
            "type": "object",
            "properties": {
                "inner": {
                    "type": "object",
                    "properties": { "flag": { "type": "boolean" } },
                    "required": ["flag"]
                }
            }
        });

        assert!(
            validate_structured_output(&schema, Some(&json!({ "inner": { "flag": true } })))
                .is_ok()
        );

        let err = validate_structured_output(&schema, Some(&json!({ "inner": {} }))).unwrap_err();
        assert!(err.contains("structuredContent.inner"), "{err}");
        assert!(err.contains("flag"), "{err}");
    }

    #[test]
    fn declines_to_judge_composition_keywords() {
        // We do not model these, so anything passes rather than being wrongly
        // rejected.
        for keyword in ["oneOf", "anyOf", "allOf", "not", "if", "$ref"] {
            let schema = json!({ "type": "object", keyword: json!({}) });
            assert!(
                validate_structured_output(&schema, Some(&json!("clearly not an object"))).is_ok(),
                "{keyword} should suppress judgement"
            );
        }
    }

    #[test]
    fn declines_to_judge_unknown_or_malformed_schemas() {
        assert!(validate_structured_output(&json!(true), Some(&json!(1))).is_ok());
        assert!(validate_structured_output(&json!("nonsense"), Some(&json!(1))).is_ok());
        assert!(validate_structured_output(&json!({ "type": "widget" }), Some(&json!(1))).is_ok());
        assert!(validate_structured_output(&json!({ "type": 42 }), Some(&json!(1))).is_ok());
    }

    #[test]
    fn empty_schema_accepts_anything() {
        let schema = json!({});
        for value in [json!(null), json!(1), json!("s"), json!([]), json!({})] {
            assert!(validate_structured_output(&schema, Some(&value)).is_ok());
        }
    }

    #[test]
    fn required_is_ignored_when_the_value_is_not_an_object() {
        // The type check already reported the real problem; don't double up.
        let schema = json!({ "required": ["x"] });
        assert!(validate_structured_output(&schema, Some(&json!([1, 2]))).is_ok());
    }

    #[test]
    fn tuple_form_items_are_not_judged() {
        let schema = json!({
            "type": "array",
            "items": [{ "type": "string" }, { "type": "number" }]
        });
        assert!(validate_structured_output(&schema, Some(&json!([1, "a"]))).is_ok());
    }
}
