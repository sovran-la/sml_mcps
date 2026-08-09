//! Validation of tool `structuredContent` against a declared `outputSchema`.
//!
//! When a tool declares an `outputSchema`, the spec makes a hard promise:
//!
//! > Servers **MUST** provide structured results that conform to this schema.
//!
//! This module enforces that promise with a real JSON Schema implementation
//! ([`boon`], draft 2020-12), so `pattern`, `minimum`, `format`,
//! `additionalProperties`, `$ref`, and `oneOf`/`anyOf`/`allOf` all mean what
//! they say. An earlier revision of this module checked only types and
//! `required`; anything that passed it could still be rejected by a strict
//! client, which made the check nearly worthless as a guarantee.
//!
//! Two deliberate exceptions to "full validation":
//!
//! - **A schema that will not compile constrains nothing.** A typo in a server's
//!   own `outputSchema` should not fail every call to a tool that is otherwise
//!   working, so an uncompilable schema is skipped. The one part of the promise
//!   that still holds is that `structuredContent` must be *present*.
//! - **`$ref` never leaves the process.** External references are refused
//!   rather than fetched, so a schema cannot turn into a file read or a network
//!   request. Self-references (`#/$defs/...`) work normally.
//!
//! `format` is an annotation in 2020-12 unless a schema opts in, which matches
//! boon's default, so `{"format": "email"}` describes intent without rejecting
//! a value the tool considers fine.

use boon::{Compiler, Schemas, UrlLoader, ValidationError};
use serde_json::Value;
use std::error::Error;

/// The URI a tool's `outputSchema` is compiled under.
///
/// Local `$ref`s resolve against it; it is never dereferenced, because
/// [`NoLoader`] refuses to load anything.
const SCHEMA_URI: &str = "sml://output-schema";

/// How many distinct problems to name in one error message.
///
/// A validation failure can produce a large tree (every branch of an `anyOf`
/// reports its own reason). Naming the first few is actionable; naming all of
/// them is noise.
const MAX_REPORTED: usize = 3;

/// A compiled `outputSchema`, ready to validate many results.
///
/// Compiling parses the schema and builds any regexes it uses, so a server
/// checking every call of a hot tool should keep one of these rather than
/// calling [`validate_structured_output`], which compiles each time.
pub struct CompiledSchema {
    schemas: Schemas,
    index: boon::SchemaIndex,
}

impl std::fmt::Debug for CompiledSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledSchema").finish_non_exhaustive()
    }
}

impl CompiledSchema {
    /// Compile a JSON Schema.
    ///
    /// Returns the compiler's complaint if `schema` is not a usable schema -
    /// which includes one whose `$ref` points outside the document, since
    /// nothing is loadable.
    pub fn new(schema: &Value) -> Result<Self, String> {
        let mut compiler = Compiler::new();
        // Refuse every external reference. Without this, boon resolves
        // `file://` URLs through its default loader, which would let an
        // `outputSchema` read the filesystem as a side effect of validation.
        compiler.use_loader(Box::new(NoLoader));
        compiler
            .add_resource(SCHEMA_URI, schema.clone())
            .map_err(|e| e.to_string())?;

        let mut schemas = Schemas::new();
        let index = compiler
            .compile(SCHEMA_URI, &mut schemas)
            .map_err(|e| e.to_string())?;

        Ok(Self { schemas, index })
    }

    /// Check a result's `structuredContent` against this schema.
    ///
    /// The absent case is a failure in its own right: declaring an
    /// `outputSchema` promises structured content, whatever the schema says.
    pub fn validate(&self, structured: Option<&Value>) -> Result<(), String> {
        let Some(structured) = structured else {
            return Err("no `structuredContent`".to_string());
        };

        self.schemas
            .validate(structured, self.index)
            .map_err(|e| describe(&e))
    }
}

/// Check `structured` against `schema`, returning a human-readable description
/// of what failed.
///
/// Compiles `schema` on every call; hold a [`CompiledSchema`] instead if that
/// shows up in a profile.
pub fn validate_structured_output(
    schema: &Value,
    structured: Option<&Value>,
) -> Result<(), String> {
    match CompiledSchema::new(schema) {
        Ok(compiled) => compiled.validate(structured),
        // An unusable schema cannot judge anything, and failing a working tool
        // over a mistake in its own schema helps nobody. The presence half of
        // the promise is still checkable, so it is still checked.
        Err(_) => match structured {
            Some(_) => Ok(()),
            None => Err("no `structuredContent`".to_string()),
        },
    }
}

/// A loader that loads nothing, so no `$ref` can escape the schema document.
struct NoLoader;

impl UrlLoader for NoLoader {
    fn load(&self, url: &str) -> Result<Value, Box<dyn Error>> {
        Err(format!("refusing to load external schema reference `{url}`").into())
    }
}

/// Render a validation failure as one line naming up to [`MAX_REPORTED`]
/// concrete problems.
///
/// boon reports a tree: a parent keyword failed *because* its children did.
/// The leaves are the specific complaints ("want string, but got number"),
/// which is what a server author needs to see.
fn describe(error: &ValidationError) -> String {
    let mut problems = Vec::new();
    collect_leaves(error, &mut problems);

    let truncated = problems.len() > MAX_REPORTED;
    problems.truncate(MAX_REPORTED);

    let mut message = problems.join("; ");
    if truncated {
        message.push_str("; ...");
    }
    message
}

/// Depth-first walk collecting the most specific complaints.
fn collect_leaves(error: &ValidationError, into: &mut Vec<String>) {
    if error.causes.is_empty() {
        into.push(format!(
            "{} {}",
            path_of(&error.instance_location.to_string()),
            error.kind
        ));
        return;
    }
    for cause in &error.causes {
        collect_leaves(cause, into);
    }
}

/// Turn a JSON Pointer into the dotted path used in error messages.
///
/// `""` -> `structuredContent`, `/a/0/b` -> `structuredContent.a[0].b`. This
/// reads far better in a log line than a pointer does.
fn path_of(pointer: &str) -> String {
    let mut path = String::from("structuredContent");
    for token in pointer.split('/').skip(1) {
        let token = token.replace("~1", "/").replace("~0", "~");
        if !token.is_empty() && token.bytes().all(|b| b.is_ascii_digit()) {
            path.push('[');
            path.push_str(&token);
            path.push(']');
        } else {
            path.push('.');
            path.push_str(&token);
        }
    }
    path
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

    /// A compiled schema is held across threads by servers that cache it.
    #[test]
    fn compiled_schemas_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CompiledSchema>();
    }

    //
    // The checks that existed before, which must keep working
    //

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
    fn accepts_extra_properties_by_default() {
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
    fn empty_schema_accepts_anything() {
        let schema = json!({});
        for value in [json!(null), json!(1), json!("s"), json!([]), json!({})] {
            assert!(validate_structured_output(&schema, Some(&value)).is_ok());
        }
    }

    #[test]
    fn required_only_applies_to_objects() {
        // JSON Schema scopes `required` to objects; an array satisfies it
        // vacuously. Nothing here declares a type, so nothing else complains.
        let schema = json!({ "required": ["x"] });
        assert!(validate_structured_output(&schema, Some(&json!([1, 2]))).is_ok());
    }

    //
    // Keywords the previous checker declined to judge
    //

    #[test]
    fn enforces_pattern() {
        let schema = json!({
            "type": "object",
            "properties": { "sku": { "type": "string", "pattern": "^[A-Z]{3}-[0-9]{4}$" } }
        });

        assert!(validate_structured_output(&schema, Some(&json!({ "sku": "ABC-1234" }))).is_ok());

        let err = validate_structured_output(&schema, Some(&json!({ "sku": "nope" }))).unwrap_err();
        assert!(err.contains("structuredContent.sku"), "{err}");
    }

    #[test]
    fn enforces_numeric_bounds() {
        let schema = json!({ "type": "integer", "minimum": 1, "maximum": 10 });

        assert!(validate_structured_output(&schema, Some(&json!(1))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!(10))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!(0))).is_err());
        assert!(validate_structured_output(&schema, Some(&json!(11))).is_err());
    }

    #[test]
    fn enforces_exclusive_bounds_and_multiple_of() {
        let schema = json!({ "type": "number", "exclusiveMinimum": 0, "multipleOf": 5 });

        assert!(validate_structured_output(&schema, Some(&json!(5))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!(0))).is_err());
        assert!(validate_structured_output(&schema, Some(&json!(7))).is_err());
    }

    #[test]
    fn enforces_string_lengths() {
        let schema = json!({ "type": "string", "minLength": 2, "maxLength": 4 });

        assert!(validate_structured_output(&schema, Some(&json!("abc"))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!("a"))).is_err());
        assert!(validate_structured_output(&schema, Some(&json!("abcde"))).is_err());
    }

    #[test]
    fn enforces_additional_properties_false() {
        let schema = json!({
            "type": "object",
            "properties": { "known": { "type": "string" } },
            "additionalProperties": false
        });

        assert!(validate_structured_output(&schema, Some(&json!({ "known": "yes" }))).is_ok());

        let value = json!({ "known": "yes", "surprise": 1 });
        let err = validate_structured_output(&schema, Some(&value)).unwrap_err();
        assert!(err.contains("surprise"), "{err}");
    }

    #[test]
    fn enforces_enum_and_const() {
        let schema = json!({
            "type": "object",
            "properties": {
                "status": { "enum": ["ok", "error"] },
                "kind": { "const": "weather" }
            }
        });

        let good = json!({ "status": "ok", "kind": "weather" });
        assert!(validate_structured_output(&schema, Some(&good)).is_ok());

        let bad_enum = json!({ "status": "maybe", "kind": "weather" });
        assert!(validate_structured_output(&schema, Some(&bad_enum)).is_err());

        let bad_const = json!({ "status": "ok", "kind": "traffic" });
        assert!(validate_structured_output(&schema, Some(&bad_const)).is_err());
    }

    #[test]
    fn enforces_one_of() {
        let schema = json!({
            "oneOf": [
                { "type": "object", "properties": { "ok": { "const": true } }, "required": ["ok"] },
                { "type": "object", "properties": { "err": { "type": "string" } },
                  "required": ["err"] }
            ]
        });

        assert!(validate_structured_output(&schema, Some(&json!({ "ok": true }))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!({ "err": "boom" }))).is_ok());
        // Matches neither branch.
        assert!(validate_structured_output(&schema, Some(&json!({ "other": 1 }))).is_err());
        // Matches both, which `oneOf` forbids.
        let both = json!({ "ok": true, "err": "boom" });
        assert!(validate_structured_output(&schema, Some(&both)).is_err());
    }

    #[test]
    fn enforces_any_of_and_all_of() {
        let any_of = json!({ "anyOf": [{ "type": "string" }, { "type": "integer" }] });
        assert!(validate_structured_output(&any_of, Some(&json!("s"))).is_ok());
        assert!(validate_structured_output(&any_of, Some(&json!(1))).is_ok());
        assert!(validate_structured_output(&any_of, Some(&json!(1.5))).is_err());

        let all_of = json!({
            "allOf": [{ "type": "string" }, { "minLength": 3 }]
        });
        assert!(validate_structured_output(&all_of, Some(&json!("abc"))).is_ok());
        assert!(validate_structured_output(&all_of, Some(&json!("ab"))).is_err());
    }

    #[test]
    fn enforces_not() {
        let schema = json!({ "not": { "type": "string" } });
        assert!(validate_structured_output(&schema, Some(&json!(1))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!("s"))).is_err());
    }

    #[test]
    fn resolves_internal_refs() {
        let schema = json!({
            "type": "object",
            "properties": {
                "start": { "$ref": "#/$defs/point" },
                "end": { "$ref": "#/$defs/point" }
            },
            "required": ["start", "end"],
            "$defs": {
                "point": {
                    "type": "object",
                    "properties": { "x": { "type": "number" }, "y": { "type": "number" } },
                    "required": ["x", "y"]
                }
            }
        });

        let good = json!({ "start": { "x": 0, "y": 0 }, "end": { "x": 1, "y": 1 } });
        assert!(validate_structured_output(&schema, Some(&good)).is_ok());

        let bad = json!({ "start": { "x": 0 }, "end": { "x": 1, "y": 1 } });
        let err = validate_structured_output(&schema, Some(&bad)).unwrap_err();
        assert!(err.contains("structuredContent.start"), "{err}");
        assert!(err.contains("y"), "{err}");
    }

    #[test]
    fn enforces_prefix_items_and_array_bounds() {
        let schema = json!({
            "type": "array",
            "prefixItems": [{ "type": "string" }, { "type": "number" }],
            "minItems": 2,
            "maxItems": 2
        });

        assert!(validate_structured_output(&schema, Some(&json!(["a", 1]))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!([1, "a"]))).is_err());
        assert!(validate_structured_output(&schema, Some(&json!(["a"]))).is_err());
    }

    #[test]
    fn enforces_unique_items() {
        let schema = json!({ "type": "array", "uniqueItems": true });
        assert!(validate_structured_output(&schema, Some(&json!([1, 2, 3]))).is_ok());
        assert!(validate_structured_output(&schema, Some(&json!([1, 1]))).is_err());
    }

    #[test]
    fn validates_deeply_nested_structures() {
        // The previous checker stopped recursing past one level, so this is the
        // shape it would have waved through.
        let schema = json!({
            "type": "object",
            "properties": {
                "rows": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "cells": { "type": "array", "items": { "type": "integer" } }
                        },
                        "required": ["cells"]
                    }
                }
            }
        });

        let good = json!({ "rows": [{ "cells": [1, 2] }, { "cells": [3] }] });
        assert!(validate_structured_output(&schema, Some(&good)).is_ok());

        let bad = json!({ "rows": [{ "cells": [1, 2] }, { "cells": [3, "four"] }] });
        let err = validate_structured_output(&schema, Some(&bad)).unwrap_err();
        assert!(err.contains("structuredContent.rows[1].cells[1]"), "{err}");
    }

    //
    // Format, and the deliberate exceptions
    //

    #[test]
    fn format_is_an_annotation_not_an_assertion() {
        // 2020-12 makes `format` descriptive by default, and boon agrees. A
        // tool that says "date-time" and returns something else is not our
        // problem to reject.
        let schema = json!({ "type": "string", "format": "date-time" });
        assert!(validate_structured_output(&schema, Some(&json!("not a date"))).is_ok());
    }

    #[test]
    fn an_uncompilable_schema_is_skipped_rather_than_failing_the_tool() {
        for schema in [
            json!("nonsense"),
            json!({ "type": 42 }),
            json!({ "type": "widget" }),
            json!({ "properties": "not an object" }),
        ] {
            assert!(
                validate_structured_output(&schema, Some(&json!({ "anything": true }))).is_ok(),
                "{schema} should be skipped, not fatal"
            );
        }
    }

    #[test]
    fn a_broken_schema_still_requires_structured_content_to_be_present() {
        // The presence half of the promise does not depend on the schema being
        // readable.
        let err = validate_structured_output(&json!("nonsense"), None).unwrap_err();
        assert!(err.contains("no `structuredContent`"), "{err}");
    }

    #[test]
    fn boolean_schemas_work() {
        assert!(validate_structured_output(&json!(true), Some(&json!(1))).is_ok());
        assert!(validate_structured_output(&json!(false), Some(&json!(1))).is_err());
    }

    #[test]
    fn external_refs_are_refused_not_fetched() {
        // A `$ref` out to the filesystem must not become a file read. It fails
        // to compile, which lands in the permissive path.
        let schema = json!({ "$ref": "file:///etc/passwd" });
        assert!(validate_structured_output(&schema, Some(&json!({}))).is_ok());

        // And compiling it directly reports why.
        let err = CompiledSchema::new(&schema).unwrap_err();
        assert!(err.contains("etc/passwd"), "{err}");
    }

    #[test]
    fn reports_several_problems_but_not_all_of_them() {
        let schema = json!({
            "type": "object",
            "properties": {
                "a": { "type": "string" },
                "b": { "type": "string" },
                "c": { "type": "string" },
                "d": { "type": "string" },
                "e": { "type": "string" }
            }
        });

        let value = json!({ "a": 1, "b": 2, "c": 3, "d": 4, "e": 5 });
        let err = validate_structured_output(&schema, Some(&value)).unwrap_err();
        assert!(err.ends_with("; ..."), "{err}");
        assert_eq!(
            err.matches("structuredContent.").count(),
            MAX_REPORTED,
            "{err}"
        );
    }

    //
    // Reuse
    //

    #[test]
    fn a_compiled_schema_validates_repeatedly() {
        let compiled = CompiledSchema::new(&weather_schema()).unwrap();

        for _ in 0..3 {
            let good = json!({ "temperature": 1.0, "conditions": "Clear", "humidity": 10 });
            assert!(compiled.validate(Some(&good)).is_ok());
            assert!(compiled.validate(Some(&json!({}))).is_err());
            assert!(compiled.validate(None).is_err());
        }
    }

    #[test]
    fn path_of_renders_pointers_readably() {
        assert_eq!(path_of(""), "structuredContent");
        assert_eq!(path_of("/a"), "structuredContent.a");
        assert_eq!(path_of("/a/0/b"), "structuredContent.a[0].b");
        // Escaped tokens: ~1 is `/`, ~0 is `~`.
        assert_eq!(path_of("/a~1b"), "structuredContent.a/b");
        assert_eq!(path_of("/a~0b"), "structuredContent.a~b");
    }
}
