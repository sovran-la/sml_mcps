//! Elicitation - servers asking the user for input, through the client.
//!
//! Two modes:
//!
//! - **Form** (2025-06-18): structured data collected in-band. The client sees
//!   the values. Servers **MUST NOT** ask for secrets this way.
//! - **URL** (2025-11-25): the client hands the user off to a URL. The data
//!   never passes through the client. This is the required mode for
//!   credentials, payments, and third-party OAuth.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// JSON-RPC error code meaning "complete a URL elicitation, then retry".
///
/// A server **MUST NOT** return this for any other reason. `data.elicitations`
/// carries the URL elicitations that must be completed first.
pub const URL_ELICITATION_REQUIRED: i32 = -32042;

/// Which kind of elicitation this is.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ElicitationMode {
    /// In-band structured data collection.
    #[default]
    Form,
    /// Out-of-band interaction at a URL.
    Url,
}

/// What the client declared it can handle.
///
/// Per the spec, an empty capability object means form mode only - that is the
/// backwards-compatible reading for clients written against 2025-06-18.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ElicitationCapability {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub form: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<Value>,
}

impl ElicitationCapability {
    /// Does the client support `mode`?
    pub fn supports(&self, mode: ElicitationMode) -> bool {
        match mode {
            // `{}` means form-only, so form is supported unless the client
            // explicitly declared url-only.
            ElicitationMode::Form => self.form.is_some() || self.url.is_none(),
            ElicitationMode::Url => self.url.is_some(),
        }
    }
}

/// Params of an `elicitation/create` request.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElicitParams {
    /// Omitted for form mode by servers targeting 2025-06-18 clients; clients
    /// **MUST** treat an absent mode as `form`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<ElicitationMode>,
    /// Why the input is needed, in human-readable terms.
    pub message: String,
    /// Form mode: JSON Schema for the expected response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_schema: Option<Value>,
    /// URL mode: where to send the user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// URL mode: correlates the completion notification with this request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elicitation_id: Option<String>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<crate::types::Meta>,
}

impl ElicitParams {
    /// A form-mode request for data matching `schema`.
    pub fn form(message: impl Into<String>, schema: Value) -> Self {
        Self {
            mode: Some(ElicitationMode::Form),
            message: message.into(),
            requested_schema: Some(schema),
            url: None,
            elicitation_id: None,
            meta: None,
        }
    }

    /// A URL-mode request sending the user to `url`.
    ///
    /// `elicitation_id` must be unique per request; it is what a later
    /// `notifications/elicitation/complete` refers back to.
    pub fn url(
        message: impl Into<String>,
        url: impl Into<String>,
        elicitation_id: impl Into<String>,
    ) -> Self {
        Self {
            mode: Some(ElicitationMode::Url),
            message: message.into(),
            requested_schema: None,
            url: Some(url.into()),
            elicitation_id: Some(elicitation_id.into()),
            meta: None,
        }
    }

    /// The mode, resolving an absent one to `form` as the spec requires.
    pub fn resolved_mode(&self) -> ElicitationMode {
        self.mode.unwrap_or(ElicitationMode::Form)
    }

    /// Check this request is internally consistent before it goes out.
    pub fn validate(&self) -> Result<(), String> {
        if self.message.trim().is_empty() {
            return Err("elicitation `message` must not be empty".into());
        }
        match self.resolved_mode() {
            ElicitationMode::Form => {
                if self.requested_schema.is_none() {
                    return Err("form mode elicitation requires `requestedSchema`".into());
                }
                if self.url.is_some() {
                    return Err("form mode elicitation must not carry a `url`".into());
                }
            }
            ElicitationMode::Url => {
                let Some(url) = &self.url else {
                    return Err("url mode elicitation requires `url`".into());
                };
                // "The `url` parameter MUST contain a valid URL." A scheme is
                // the minimum we can check without a URL parser dependency.
                if !url.contains("://") || url.starts_with("://") {
                    return Err(format!("url mode elicitation has an invalid `url`: {url}"));
                }
                if self.elicitation_id.is_none() {
                    return Err("url mode elicitation requires `elicitationId`".into());
                }
                if self.requested_schema.is_some() {
                    return Err("url mode elicitation must not carry a `requestedSchema`".into());
                }
            }
        }
        Ok(())
    }
}

/// What the user did.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ElicitAction {
    /// Explicitly approved and submitted. In form mode, `content` holds data;
    /// in URL mode it means consent to navigate, *not* that the out-of-band
    /// interaction finished.
    Accept,
    /// Explicitly refused.
    Decline,
    /// Dismissed without choosing - closed the dialog, pressed Escape, the
    /// browser failed to load.
    Cancel,
}

/// The client's answer to `elicitation/create`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElicitResult {
    pub action: ElicitAction,
    /// Submitted data. Present only for an accepted form-mode request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<Map<String, Value>>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<crate::types::Meta>,
}

impl ElicitResult {
    /// Did the user accept?
    pub fn accepted(&self) -> bool {
        self.action == ElicitAction::Accept
    }

    /// Submitted data, only when accepted.
    ///
    /// A `content` payload on a declined or cancelled result is not something
    /// the server should act on, so it is not returned here.
    pub fn accepted_content(&self) -> Option<&Map<String, Value>> {
        self.accepted().then_some(self.content.as_ref()?)
    }

    /// Deserialize the accepted content into a typed value.
    pub fn parse<T: serde::de::DeserializeOwned>(&self) -> Option<serde_json::Result<T>> {
        let content = self.accepted_content()?;
        Some(serde_json::from_value(Value::Object(content.clone())))
    }
}

/// Params of `notifications/elicitation/complete`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ElicitationCompleteParams {
    /// The id from the originating URL-mode `elicitation/create`.
    pub elicitation_id: String,
}

//
// Form schema builder
//

/// Builder for a form-mode `requestedSchema`.
///
/// The spec restricts these to a flat object of primitives so clients can
/// render a form without implementing JSON Schema. This builder can only
/// express that subset, so a schema it produces is valid by construction.
///
/// # Example
///
/// ```
/// use sml_mcps::ElicitSchema;
///
/// let schema = ElicitSchema::new()
///     .string("name", |f| f.title("Your name").min_length(1))
///     .required("name")
///     .number("age", |f| f.minimum(18.0))
///     .boolean("subscribe", |f| f.default(false))
///     .build();
///
/// assert_eq!(schema["type"], "object");
/// assert_eq!(schema["required"][0], "name");
/// ```
#[derive(Debug, Clone, Default)]
pub struct ElicitSchema {
    properties: Map<String, Value>,
    required: Vec<String>,
}

impl ElicitSchema {
    /// An empty schema.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a string property.
    pub fn string(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(StringField) -> StringField,
    ) -> Self {
        let field = f(<StringField as Default>::default());
        self.properties.insert(name.into(), field.build("string"));
        self
    }

    /// Add a number property.
    pub fn number(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(NumberField) -> NumberField,
    ) -> Self {
        let field = f(<NumberField as Default>::default());
        self.properties.insert(name.into(), field.build("number"));
        self
    }

    /// Add an integer property.
    pub fn integer(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(NumberField) -> NumberField,
    ) -> Self {
        let field = f(<NumberField as Default>::default());
        self.properties.insert(name.into(), field.build("integer"));
        self
    }

    /// Add a boolean property.
    pub fn boolean(
        mut self,
        name: impl Into<String>,
        f: impl FnOnce(BooleanField) -> BooleanField,
    ) -> Self {
        let field = f(<BooleanField as Default>::default());
        self.properties.insert(name.into(), field.build());
        self
    }

    /// Add a single-select enum whose values are their own labels.
    pub fn enumeration<I, S>(mut self, name: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let values: Vec<Value> = values
            .into_iter()
            .map(|v| Value::String(v.into()))
            .collect();
        self.properties.insert(
            name.into(),
            serde_json::json!({ "type": "string", "enum": values }),
        );
        self
    }

    /// Add a single-select enum with separate values and display titles.
    ///
    /// Rendered as `oneOf` of `{const, title}`, which is the 2025-11-25 shape
    /// (SEP-1330) for titled enums.
    pub fn titled_enumeration<I, V, T>(mut self, name: impl Into<String>, choices: I) -> Self
    where
        I: IntoIterator<Item = (V, T)>,
        V: Into<String>,
        T: Into<String>,
    {
        let one_of: Vec<Value> = choices
            .into_iter()
            .map(|(value, title)| {
                serde_json::json!({ "const": value.into(), "title": title.into() })
            })
            .collect();
        self.properties.insert(
            name.into(),
            serde_json::json!({ "type": "string", "oneOf": one_of }),
        );
        self
    }

    /// Add a multi-select enum whose values are their own labels.
    pub fn multi_enumeration<I, S>(mut self, name: impl Into<String>, values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let values: Vec<Value> = values
            .into_iter()
            .map(|v| Value::String(v.into()))
            .collect();
        self.properties.insert(
            name.into(),
            serde_json::json!({
                "type": "array",
                "items": { "type": "string", "enum": values }
            }),
        );
        self
    }

    /// Add a multi-select enum with separate values and display titles.
    pub fn titled_multi_enumeration<I, V, T>(mut self, name: impl Into<String>, choices: I) -> Self
    where
        I: IntoIterator<Item = (V, T)>,
        V: Into<String>,
        T: Into<String>,
    {
        let any_of: Vec<Value> = choices
            .into_iter()
            .map(|(value, title)| {
                serde_json::json!({ "const": value.into(), "title": title.into() })
            })
            .collect();
        self.properties.insert(
            name.into(),
            serde_json::json!({ "type": "array", "items": { "anyOf": any_of } }),
        );
        self
    }

    /// Attach metadata (`title`, `description`, `default`, `minItems`, ...) to
    /// an already-added property.
    ///
    /// Useful for the enum helpers, which take no field builder.
    pub fn annotate(mut self, name: &str, key: impl Into<String>, value: Value) -> Self {
        if let Some(Value::Object(property)) = self.properties.get_mut(name) {
            property.insert(key.into(), value);
        }
        self
    }

    /// Mark a property required. Names that were never added are ignored, so a
    /// typo cannot produce a schema demanding a field the client never sees.
    pub fn required(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        if self.properties.contains_key(&name) && !self.required.contains(&name) {
            self.required.push(name);
        }
        self
    }

    /// Produce the JSON Schema.
    pub fn build(self) -> Value {
        let mut schema = serde_json::json!({
            "type": "object",
            "properties": Value::Object(self.properties),
        });
        if !self.required.is_empty() {
            schema["required"] = Value::Array(
                self.required
                    .into_iter()
                    .map(Value::String)
                    .collect::<Vec<_>>(),
            );
        }
        schema
    }
}

/// Options for a string field.
#[derive(Debug, Clone, Default)]
pub struct StringField {
    title: Option<String>,
    description: Option<String>,
    min_length: Option<u64>,
    max_length: Option<u64>,
    pattern: Option<String>,
    format: Option<String>,
    default: Option<String>,
}

impl StringField {
    /// Display label.
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }
    /// Help text.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
    pub fn min_length(mut self, min: u64) -> Self {
        self.min_length = Some(min);
        self
    }
    pub fn max_length(mut self, max: u64) -> Self {
        self.max_length = Some(max);
        self
    }
    pub fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }
    /// One of the supported formats: `email`, `uri`, `date`, `date-time`.
    pub fn format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }
    /// Pre-populated value.
    pub fn default(mut self, value: impl Into<String>) -> Self {
        self.default = Some(value.into());
        self
    }

    fn build(self, type_name: &str) -> Value {
        let mut object = Map::new();
        object.insert("type".into(), Value::String(type_name.into()));
        insert_opt(&mut object, "title", self.title.map(Value::String));
        insert_opt(
            &mut object,
            "description",
            self.description.map(Value::String),
        );
        insert_opt(&mut object, "minLength", self.min_length.map(Into::into));
        insert_opt(&mut object, "maxLength", self.max_length.map(Into::into));
        insert_opt(&mut object, "pattern", self.pattern.map(Value::String));
        insert_opt(&mut object, "format", self.format.map(Value::String));
        insert_opt(&mut object, "default", self.default.map(Value::String));
        Value::Object(object)
    }
}

/// Options for a number or integer field.
#[derive(Debug, Clone, Default)]
pub struct NumberField {
    title: Option<String>,
    description: Option<String>,
    minimum: Option<f64>,
    maximum: Option<f64>,
    default: Option<f64>,
}

impl NumberField {
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
    pub fn minimum(mut self, minimum: f64) -> Self {
        self.minimum = Some(minimum);
        self
    }
    pub fn maximum(mut self, maximum: f64) -> Self {
        self.maximum = Some(maximum);
        self
    }
    pub fn default(mut self, value: f64) -> Self {
        self.default = Some(value);
        self
    }

    fn build(self, type_name: &str) -> Value {
        let mut object = Map::new();
        object.insert("type".into(), Value::String(type_name.into()));
        insert_opt(&mut object, "title", self.title.map(Value::String));
        insert_opt(
            &mut object,
            "description",
            self.description.map(Value::String),
        );
        insert_opt(&mut object, "minimum", self.minimum.and_then(number));
        insert_opt(&mut object, "maximum", self.maximum.and_then(number));
        insert_opt(&mut object, "default", self.default.and_then(number));
        Value::Object(object)
    }
}

/// Options for a boolean field.
#[derive(Debug, Clone, Default)]
pub struct BooleanField {
    title: Option<String>,
    description: Option<String>,
    default: Option<bool>,
}

impl BooleanField {
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
    pub fn default(mut self, value: bool) -> Self {
        self.default = Some(value);
        self
    }

    fn build(self) -> Value {
        let mut object = Map::new();
        object.insert("type".into(), Value::String("boolean".into()));
        insert_opt(&mut object, "title", self.title.map(Value::String));
        insert_opt(
            &mut object,
            "description",
            self.description.map(Value::String),
        );
        insert_opt(&mut object, "default", self.default.map(Value::Bool));
        Value::Object(object)
    }
}

fn insert_opt(object: &mut Map<String, Value>, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        object.insert(key.into(), value);
    }
}

/// NaN and infinity have no JSON representation, so such bounds are dropped
/// rather than silently serialized as `null`.
fn number(value: f64) -> Option<Value> {
    serde_json::Number::from_f64(value).map(Value::Number)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    //
    // Capability negotiation
    //

    #[test]
    fn empty_capability_means_form_only() {
        // "an empty capabilities object is equivalent to declaring support for
        // `form` mode only"
        let capability = ElicitationCapability::default();
        assert!(capability.supports(ElicitationMode::Form));
        assert!(!capability.supports(ElicitationMode::Url));
    }

    #[test]
    fn explicit_capabilities_are_honored() {
        let both: ElicitationCapability =
            serde_json::from_value(json!({ "form": {}, "url": {} })).unwrap();
        assert!(both.supports(ElicitationMode::Form));
        assert!(both.supports(ElicitationMode::Url));

        let url_only: ElicitationCapability = serde_json::from_value(json!({ "url": {} })).unwrap();
        assert!(!url_only.supports(ElicitationMode::Form));
        assert!(url_only.supports(ElicitationMode::Url));

        let form_only: ElicitationCapability =
            serde_json::from_value(json!({ "form": {} })).unwrap();
        assert!(form_only.supports(ElicitationMode::Form));
        assert!(!form_only.supports(ElicitationMode::Url));
    }

    //
    // Request shape
    //

    #[test]
    fn form_request_serializes_to_the_spec_shape() {
        let params = ElicitParams::form(
            "Please provide your GitHub username",
            json!({
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "required": ["name"]
            }),
        );
        let wire: Value = serde_json::to_value(&params).unwrap();

        assert_eq!(wire["mode"], "form");
        assert_eq!(wire["message"], "Please provide your GitHub username");
        assert_eq!(wire["requestedSchema"]["required"][0], "name");
        assert!(wire.get("url").is_none());
        assert!(wire.get("elicitationId").is_none());
    }

    #[test]
    fn url_request_serializes_to_the_spec_shape() {
        let params = ElicitParams::url(
            "Please provide your API key to continue.",
            "https://mcp.example.com/ui/set_api_key",
            "550e8400-e29b-41d4-a716-446655440000",
        );
        let wire: Value = serde_json::to_value(&params).unwrap();

        assert_eq!(wire["mode"], "url");
        assert_eq!(wire["url"], "https://mcp.example.com/ui/set_api_key");
        assert_eq!(
            wire["elicitationId"],
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert!(wire.get("requestedSchema").is_none());
    }

    #[test]
    fn absent_mode_resolves_to_form() {
        let params: ElicitParams =
            serde_json::from_value(json!({ "message": "hi", "requestedSchema": {} })).unwrap();
        assert_eq!(params.resolved_mode(), ElicitationMode::Form);
        assert!(params.mode.is_none());
    }

    #[test]
    fn validation_accepts_well_formed_requests() {
        assert!(
            ElicitParams::form("m", json!({ "type": "object" }))
                .validate()
                .is_ok()
        );
        assert!(
            ElicitParams::url("m", "https://example.com/x", "id-1")
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn validation_rejects_an_empty_message() {
        let mut params = ElicitParams::form("m", json!({}));
        params.message = "   ".into();
        assert!(params.validate().unwrap_err().contains("message"));
    }

    #[test]
    fn validation_rejects_form_without_schema() {
        let mut params = ElicitParams::form("m", json!({}));
        params.requested_schema = None;
        assert!(params.validate().unwrap_err().contains("requestedSchema"));
    }

    #[test]
    fn validation_rejects_mode_field_mixups() {
        let mut form = ElicitParams::form("m", json!({}));
        form.url = Some("https://example.com".into());
        assert!(form.validate().unwrap_err().contains("must not carry"));

        let mut url = ElicitParams::url("m", "https://example.com", "id");
        url.requested_schema = Some(json!({}));
        assert!(url.validate().unwrap_err().contains("must not carry"));
    }

    #[test]
    fn validation_rejects_url_mode_without_a_usable_url() {
        let mut params = ElicitParams::url("m", "https://example.com", "id");
        params.url = None;
        assert!(params.validate().unwrap_err().contains("requires `url`"));

        for bad in ["not-a-url", "://nohost", "example.com/path"] {
            let mut params = ElicitParams::url("m", "https://example.com", "id");
            params.url = Some(bad.into());
            assert!(
                params.validate().unwrap_err().contains("invalid `url`"),
                "{bad} should be rejected"
            );
        }
    }

    #[test]
    fn validation_rejects_url_mode_without_an_id() {
        let mut params = ElicitParams::url("m", "https://example.com", "id");
        params.elicitation_id = None;
        assert!(params.validate().unwrap_err().contains("elicitationId"));
    }

    //
    // Results
    //

    #[test]
    fn accept_with_content_parses() {
        let result: ElicitResult = serde_json::from_value(json!({
            "action": "accept",
            "content": { "name": "octocat", "age": 30 }
        }))
        .unwrap();

        assert!(result.accepted());
        assert_eq!(result.accepted_content().unwrap()["name"], "octocat");

        #[derive(serde::Deserialize)]
        struct Answer {
            name: String,
            age: u32,
        }
        let answer: Answer = result.parse().unwrap().unwrap();
        assert_eq!(answer.name, "octocat");
        assert_eq!(answer.age, 30);
    }

    #[test]
    fn decline_and_cancel_expose_no_content() {
        for action in ["decline", "cancel"] {
            // Even if a client wrongly sends content, a non-accept action must
            // not hand the server data to act on.
            let result: ElicitResult = serde_json::from_value(json!({
                "action": action,
                "content": { "name": "sneaky" }
            }))
            .unwrap();

            assert!(!result.accepted());
            assert!(result.accepted_content().is_none());
            assert!(result.parse::<Value>().is_none());
        }
    }

    #[test]
    fn url_mode_accept_has_no_content() {
        let result: ElicitResult = serde_json::from_value(json!({ "action": "accept" })).unwrap();
        assert!(result.accepted());
        assert!(result.accepted_content().is_none());
    }

    #[test]
    fn all_three_actions_round_trip() {
        for (action, name) in [
            (ElicitAction::Accept, "accept"),
            (ElicitAction::Decline, "decline"),
            (ElicitAction::Cancel, "cancel"),
        ] {
            assert_eq!(serde_json::to_value(action).unwrap(), json!(name));
            assert_eq!(
                serde_json::from_value::<ElicitAction>(json!(name)).unwrap(),
                action
            );
        }
    }

    #[test]
    fn completion_notification_params_round_trip() {
        let params = ElicitationCompleteParams {
            elicitation_id: "550e8400-e29b-41d4-a716-446655440000".into(),
        };
        let wire: Value = serde_json::to_value(&params).unwrap();
        assert_eq!(
            wire["elicitationId"],
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    //
    // Schema builder
    //

    #[test]
    fn builds_a_flat_primitive_schema() {
        let schema = ElicitSchema::new()
            .string("name", |f| f.description("Your full name"))
            .string("email", |f| f.format("email"))
            .number("age", |f| f.minimum(18.0))
            .required("name")
            .required("email")
            .build();

        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["name"]["type"], "string");
        assert_eq!(schema["properties"]["email"]["format"], "email");
        assert_eq!(schema["properties"]["age"]["minimum"], 18.0);
        assert_eq!(schema["required"][0], "name");
        assert_eq!(schema["required"][1], "email");
    }

    #[test]
    fn string_field_supports_every_documented_option() {
        let schema = ElicitSchema::new()
            .string("s", |f| {
                f.title("Display Name")
                    .description("Description text")
                    .min_length(3)
                    .max_length(50)
                    .pattern("^[A-Za-z]+$")
                    .format("email")
                    .default("user@example.com")
            })
            .build();

        let field = &schema["properties"]["s"];
        assert_eq!(field["type"], "string");
        assert_eq!(field["title"], "Display Name");
        assert_eq!(field["description"], "Description text");
        assert_eq!(field["minLength"], 3);
        assert_eq!(field["maxLength"], 50);
        assert_eq!(field["pattern"], "^[A-Za-z]+$");
        assert_eq!(field["format"], "email");
        assert_eq!(field["default"], "user@example.com");
    }

    #[test]
    fn number_and_integer_fields() {
        let schema = ElicitSchema::new()
            .number("n", |f| f.minimum(0.0).maximum(100.0).default(50.0))
            .integer("i", |f| f.title("Count"))
            .build();

        assert_eq!(schema["properties"]["n"]["type"], "number");
        assert_eq!(schema["properties"]["n"]["minimum"], 0.0);
        assert_eq!(schema["properties"]["n"]["maximum"], 100.0);
        assert_eq!(schema["properties"]["n"]["default"], 50.0);
        assert_eq!(schema["properties"]["i"]["type"], "integer");
        assert_eq!(schema["properties"]["i"]["title"], "Count");
    }

    #[test]
    fn non_finite_bounds_are_dropped_rather_than_serialized_as_null() {
        let schema = ElicitSchema::new()
            .number("n", |f| f.minimum(f64::NAN).maximum(f64::INFINITY))
            .build();
        assert!(schema["properties"]["n"].get("minimum").is_none());
        assert!(schema["properties"]["n"].get("maximum").is_none());
    }

    #[test]
    fn boolean_field_with_default() {
        let schema = ElicitSchema::new()
            .boolean("b", |f| f.title("Subscribe").default(false))
            .build();
        assert_eq!(schema["properties"]["b"]["type"], "boolean");
        assert_eq!(schema["properties"]["b"]["default"], false);
    }

    #[test]
    fn untitled_single_select_enum() {
        let schema = ElicitSchema::new()
            .enumeration("color", ["Red", "Green", "Blue"])
            .annotate("color", "default", json!("Red"))
            .build();

        assert_eq!(schema["properties"]["color"]["type"], "string");
        assert_eq!(schema["properties"]["color"]["enum"][2], "Blue");
        assert_eq!(schema["properties"]["color"]["default"], "Red");
    }

    #[test]
    fn titled_single_select_enum_uses_one_of() {
        let schema = ElicitSchema::new()
            .titled_enumeration("color", [("#FF0000", "Red"), ("#00FF00", "Green")])
            .build();

        let one_of = &schema["properties"]["color"]["oneOf"];
        assert_eq!(one_of[0]["const"], "#FF0000");
        assert_eq!(one_of[0]["title"], "Red");
        assert_eq!(one_of[1]["title"], "Green");
    }

    #[test]
    fn untitled_multi_select_enum() {
        let schema = ElicitSchema::new()
            .multi_enumeration("colors", ["Red", "Green", "Blue"])
            .annotate("colors", "minItems", json!(1))
            .annotate("colors", "maxItems", json!(2))
            .build();

        let field = &schema["properties"]["colors"];
        assert_eq!(field["type"], "array");
        assert_eq!(field["items"]["enum"][0], "Red");
        assert_eq!(field["minItems"], 1);
        assert_eq!(field["maxItems"], 2);
    }

    #[test]
    fn titled_multi_select_enum_uses_any_of() {
        let schema = ElicitSchema::new()
            .titled_multi_enumeration("colors", [("#FF0000", "Red"), ("#00FF00", "Green")])
            .build();

        let any_of = &schema["properties"]["colors"]["items"]["anyOf"];
        assert_eq!(any_of[0]["const"], "#FF0000");
        assert_eq!(any_of[1]["title"], "Green");
    }

    #[test]
    fn required_ignores_unknown_property_names() {
        // A typo must not produce a schema demanding a field that has no
        // corresponding form control.
        let schema = ElicitSchema::new()
            .string("name", |f| f)
            .required("nmae")
            .required("name")
            .build();
        assert_eq!(schema["required"].as_array().unwrap().len(), 1);
        assert_eq!(schema["required"][0], "name");
    }

    #[test]
    fn required_does_not_duplicate() {
        let schema = ElicitSchema::new()
            .string("name", |f| f)
            .required("name")
            .required("name")
            .build();
        assert_eq!(schema["required"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn empty_schema_omits_required() {
        let schema = ElicitSchema::new().build();
        assert_eq!(schema["type"], "object");
        assert!(schema.get("required").is_none());
    }

    #[test]
    fn annotate_ignores_unknown_properties() {
        let schema = ElicitSchema::new()
            .annotate("nope", "title", json!("x"))
            .build();
        assert!(schema["properties"].as_object().unwrap().is_empty());
    }
}
