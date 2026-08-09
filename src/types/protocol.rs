//! MCP Protocol Types
//!
//! Types for MCP initialization, capabilities, tools, resources, and prompts.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Current MCP protocol version
pub const PROTOCOL_VERSION: &str = "2025-03-26";

/// Free-form metadata attached to a protocol object.
///
/// MCP reserves any key whose prefix has `modelcontextprotocol` or `mcp` as its
/// second label (`io.modelcontextprotocol/`, `dev.mcp/`, ...). Use a prefix you
/// own - reverse-DNS is the recommended convention.
pub type Meta = serde_json::Map<String, Value>;

//
// Icons (2025-11-25)
//

/// A visual identifier for a tool, prompt, resource, or implementation.
///
/// Consumers treat icon URIs and bytes as untrusted input; `src` should be an
/// `https:` or `data:` URI, ideally same-origin with the server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Icon {
    /// `https:` URL or `data:` URI pointing at the image.
    pub src: String,
    /// MIME type, when the transport's own type is missing or too generic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// Size specifications, e.g. `["48x48"]`, or `["any"]` for SVG.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sizes: Vec<String>,
    /// Preferred background theme: `"light"` or `"dark"`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub theme: Option<String>,
}

impl Icon {
    /// An icon with just a source URI.
    pub fn new(src: impl Into<String>) -> Self {
        Self {
            src: src.into(),
            mime_type: None,
            sizes: Vec::new(),
            theme: None,
        }
    }

    /// Set the MIME type.
    pub fn with_mime_type(mut self, mime_type: impl Into<String>) -> Self {
        self.mime_type = Some(mime_type.into());
        self
    }

    /// Set the advertised sizes.
    pub fn with_sizes<I, S>(mut self, sizes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.sizes = sizes.into_iter().map(Into::into).collect();
        self
    }

    /// Set the preferred theme (`"light"` or `"dark"`).
    pub fn with_theme(mut self, theme: impl Into<String>) -> Self {
        self.theme = Some(theme.into());
        self
    }
}

//
// Annotations
//

/// Hints about how a client should use or display a resource or content block.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Annotations {
    /// Who this is for. `["user"]`, `["assistant"]`, or both.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audience: Vec<Role>,
    /// Importance from 0.0 (entirely optional) to 1.0 (effectively required).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<f64>,
    /// ISO 8601 timestamp of the last modification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
}

impl Annotations {
    /// Annotations targeted at a specific audience.
    pub fn for_audience(audience: impl IntoIterator<Item = Role>) -> Self {
        Self {
            audience: audience.into_iter().collect(),
            ..Default::default()
        }
    }

    /// Set the priority. Values outside 0.0..=1.0 are clamped, since the spec
    /// defines the scale and an out-of-range value has no meaning.
    pub fn with_priority(mut self, priority: f64) -> Self {
        self.priority = Some(priority.clamp(0.0, 1.0));
        self
    }

    /// Set the last-modified timestamp (ISO 8601).
    pub fn with_last_modified(mut self, timestamp: impl Into<String>) -> Self {
        self.last_modified = Some(timestamp.into());
        self
    }

    /// True when nothing is set, so it can be omitted from the wire.
    pub fn is_empty(&self) -> bool {
        self.audience.is_empty() && self.priority.is_none() && self.last_modified.is_none()
    }
}

//
// Initialization
//

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Implementation {
    /// Programmatic identifier.
    pub name: String,
    pub version: String,
    /// Human-readable display name (2025-06-18).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Human-readable context, aligned with the registry `server.json` format
    /// (2025-11-25).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Icons for display in user interfaces (2025-11-25).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Icon>,
    /// Project or documentation URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub website_url: Option<String>,
}

impl Implementation {
    /// An implementation with just a name and version.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InitializeParams {
    pub protocol_version: String,
    pub capabilities: ClientCapabilities,
    pub client_info: Implementation,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    pub protocol_version: String,
    pub capabilities: ServerCapabilities,
    pub server_info: Implementation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

//
// Capabilities
//

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ClientCapabilities {
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub experimental: HashMap<String, Value>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub sampling: HashMap<String, Value>,
    #[serde(default)]
    pub roots: RootCapabilities,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RootCapabilities {
    #[serde(default)]
    pub list_changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServerCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolsCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompts: Option<PromptsCapability>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logging: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experimental: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ToolsCapability {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub list_changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesCapability {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub subscribe: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub list_changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PromptsCapability {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub list_changed: bool,
}

//
// Tools
//

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Tool {
    /// Unique programmatic identifier.
    pub name: String,
    /// Human-readable display name (2025-06-18). Lets `name` stay a stable
    /// identifier while the UI shows something friendlier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema for the arguments. Defaults to the 2020-12 dialect when it
    /// carries no `$schema`.
    pub input_schema: Value,
    /// JSON Schema for `structuredContent` in the result (2025-06-18).
    ///
    /// When present, the server **MUST** return structured results conforming
    /// to it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<ToolAnnotations>,
    /// Icons for display in user interfaces (2025-11-25).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Icon>,
    /// Execution properties, currently just task support (2025-11-25).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution: Option<ToolExecution>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
}

/// Execution-related properties of a tool (2025-11-25).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolExecution {
    /// Whether this tool may (or must) be invoked as a task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_support: Option<TaskSupport>,
}

/// How a tool relates to task-augmented execution (2025-11-25).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskSupport {
    /// The tool cannot be invoked as a task. This is the default; servers
    /// **SHOULD** answer a task-augmented call with -32601.
    #[default]
    Forbidden,
    /// The client may invoke it either way.
    Optional,
    /// The client **MUST** invoke it as a task; a plain call gets -32601.
    Required,
}

/// Names a tool `name` must satisfy for broad client compatibility.
///
/// The spec states these as SHOULDs: 1-128 characters, and only ASCII letters,
/// digits, underscore, hyphen, and dot.
pub fn is_valid_tool_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.')
}

/// Tool behavior hints for clients
///
/// These hints help clients understand tool behavior:
/// - `read_only_hint`: Tool only reads data, never modifies
/// - `idempotent_hint`: Multiple calls with same args have same effect as one call
/// - `destructive_hint`: Tool may overwrite or heavily mutate data
///
/// All hints default to false/None when not specified.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ToolAnnotations {
    /// Human-readable display name for the tool (2025-06-18).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// If true, the tool only reads data and never modifies anything
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only_hint: bool,

    /// If true, calling multiple times with same args has same effect as once
    /// Only meaningful when read_only_hint is false
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub idempotent_hint: bool,

    /// If true, tool may overwrite or heavily mutate data
    /// Only meaningful when read_only_hint is false
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub destructive_hint: bool,

    /// Whether the tool touches an open world (the internet, a shared
    /// database) rather than a closed one (a local calculation).
    ///
    /// `None` means unspecified. Note the schema's own default is `true`, so
    /// omitting this is *not* the same as declaring `false`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_world_hint: Option<bool>,
}

impl ToolAnnotations {
    /// Create annotations for a read-only tool
    pub fn read_only() -> Self {
        Self {
            read_only_hint: true,
            ..Default::default()
        }
    }

    /// Create annotations for an idempotent write tool
    pub fn idempotent() -> Self {
        Self {
            idempotent_hint: true,
            ..Default::default()
        }
    }

    /// Create annotations for a destructive write tool
    pub fn destructive() -> Self {
        Self {
            destructive_hint: true,
            ..Default::default()
        }
    }

    /// Create annotations for an idempotent but destructive tool (e.g., write_file)
    pub fn idempotent_destructive() -> Self {
        Self {
            idempotent_hint: true,
            destructive_hint: true,
            ..Default::default()
        }
    }

    /// Set the human-readable display title.
    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Declare whether the tool interacts with an open world.
    pub fn with_open_world(mut self, open_world: bool) -> Self {
        self.open_world_hint = Some(open_world);
        self
    }

    /// Check if any hints are set (for skip_serializing)
    pub fn is_empty(&self) -> bool {
        self.title.is_none()
            && !self.read_only_hint
            && !self.idempotent_hint
            && !self.destructive_hint
            && self.open_world_hint.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListToolsParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListToolsResult {
    pub tools: Vec<Tool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallToolParams {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
}

/// The result of a `tools/call`.
///
/// Tool *execution* failures belong here with `is_error: true`, not as
/// JSON-RPC errors: the spec wants models to see actionable failure text so
/// they can self-correct. Reserve JSON-RPC errors for protocol-level problems
/// (unknown tool, malformed request).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CallToolResult {
    /// Unstructured content blocks, shown to the model and/or user.
    pub content: Vec<Content>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_error: bool,
    /// Structured result data (2025-06-18).
    ///
    /// When the tool declares an `outputSchema`, this **MUST** conform to it.
    /// For backwards compatibility a tool returning structured content
    /// **SHOULD** also serialize it into a text block.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<Value>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
}

impl CallToolResult {
    /// Create a successful text result
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            content: vec![Content::text(text)],
            ..Default::default()
        }
    }

    /// Create an error result
    ///
    /// This is a *tool execution* error: the call itself succeeded at the
    /// protocol level and the model gets the message to work with.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            content: vec![Content::text(message)],
            is_error: true,
            ..Default::default()
        }
    }

    /// Create a result from arbitrary content blocks.
    pub fn content(content: impl IntoIterator<Item = Content>) -> Self {
        Self {
            content: content.into_iter().collect(),
            ..Default::default()
        }
    }

    /// Create a structured result, mirroring the JSON into a text block.
    ///
    /// The text mirror is what the spec recommends for clients that predate
    /// `structuredContent` - they still see the data, just unstructured.
    pub fn structured(value: impl Serialize) -> Result<Self, serde_json::Error> {
        let value = serde_json::to_value(value)?;
        Ok(Self {
            content: vec![Content::text(serde_json::to_string(&value)?)],
            is_error: false,
            structured_content: Some(value),
            meta: None,
        })
    }

    /// Attach structured content to an existing result, without touching the
    /// content blocks.
    pub fn with_structured_content(mut self, value: Value) -> Self {
        self.structured_content = Some(value);
        self
    }

    /// Attach protocol metadata.
    pub fn with_meta(mut self, meta: Meta) -> Self {
        self.meta = Some(meta);
        self
    }

    /// Set a single `_meta` key.
    pub fn with_meta_key(mut self, key: impl Into<String>, value: Value) -> Self {
        self.meta
            .get_or_insert_with(Meta::new)
            .insert(key.into(), value);
        self
    }
}

//
// Content Types
//

/// A content block in a tool result or prompt message.
///
/// Marked `#[non_exhaustive]`: match with a wildcard arm, since MCP adds
/// content types across revisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
#[non_exhaustive]
pub enum Content {
    #[serde(rename = "text")]
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Annotations>,
    },
    #[serde(rename = "image")]
    Image {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Annotations>,
    },
    /// Base64 audio (2025-06-18).
    #[serde(rename = "audio")]
    Audio {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Annotations>,
    },
    /// A pointer to a resource the client can fetch or subscribe to
    /// (2025-06-18). Cheaper than embedding when the payload is large.
    #[serde(rename = "resource_link")]
    ResourceLink {
        uri: String,
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        size: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Annotations>,
    },
    /// An embedded resource.
    ///
    /// Note the nesting: the spec puts the resource under a `resource` key
    /// rather than flattening its fields into the content block.
    #[serde(rename = "resource")]
    Resource {
        resource: ResourceContent,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Annotations>,
    },
}

impl Content {
    pub fn text(text: impl Into<String>) -> Self {
        Content::Text {
            text: text.into(),
            annotations: None,
        }
    }

    pub fn image(data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        Content::Image {
            data: data.into(),
            mime_type: mime_type.into(),
            annotations: None,
        }
    }

    /// Base64-encoded audio with its MIME type (e.g. `audio/wav`).
    pub fn audio(data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        Content::Audio {
            data: data.into(),
            mime_type: mime_type.into(),
            annotations: None,
        }
    }

    /// A link to a resource, identified by URI and name.
    pub fn resource_link(uri: impl Into<String>, name: impl Into<String>) -> Self {
        Content::ResourceLink {
            uri: uri.into(),
            name: name.into(),
            title: None,
            description: None,
            mime_type: None,
            size: None,
            annotations: None,
        }
    }

    /// An embedded resource.
    pub fn embedded(resource: ResourceContent) -> Self {
        Content::Resource {
            resource,
            annotations: None,
        }
    }

    /// Serialize a value to JSON and wrap it in a text block.
    pub fn json(value: impl Serialize) -> Result<Self, serde_json::Error> {
        Ok(Content::text(serde_json::to_string(&value)?))
    }

    /// Attach annotations to this block.
    pub fn with_annotations(mut self, annotations: Annotations) -> Self {
        let slot = match &mut self {
            Content::Text { annotations, .. } => annotations,
            Content::Image { annotations, .. } => annotations,
            Content::Audio { annotations, .. } => annotations,
            Content::ResourceLink { annotations, .. } => annotations,
            Content::Resource { annotations, .. } => annotations,
        };
        *slot = Some(annotations);
        self
    }

    /// The annotations on this block, if any.
    pub fn annotations(&self) -> Option<&Annotations> {
        match self {
            Content::Text { annotations, .. }
            | Content::Image { annotations, .. }
            | Content::Audio { annotations, .. }
            | Content::ResourceLink { annotations, .. }
            | Content::Resource { annotations, .. } => annotations.as_ref(),
        }
    }

    /// The text of a text block, or `None` for any other kind.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Content::Text { text, .. } => Some(text),
            _ => None,
        }
    }
}

//
// Resources
//

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    pub uri: String,
    pub name: String,
    /// Human-readable display name (2025-06-18).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// Size in bytes, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Icons for display in user interfaces (2025-11-25).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Icon>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Annotations>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
}

/// A parameterized resource, addressed by an RFC 6570 URI template.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceTemplate {
    /// RFC 6570 URI template, e.g. `file:///{path}`.
    pub uri_template: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Icon>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Annotations>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListResourceTemplatesParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResourceTemplatesResult {
    pub resource_templates: Vec<ResourceTemplate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListResourcesParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResourcesResult {
    pub resources: Vec<Resource>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadResourceParams {
    pub uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadResourceResult {
    pub contents: Vec<ResourceContent>,
}

/// The contents of a resource: either UTF-8 text or base64 binary.
///
/// Marked `#[non_exhaustive]`: match with a wildcard arm.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum ResourceContent {
    Text {
        uri: String,
        text: String,
        #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
    },
    Blob {
        uri: String,
        blob: String,
        #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
    },
}

impl ResourceContent {
    /// Text contents for a URI.
    pub fn text(uri: impl Into<String>, text: impl Into<String>) -> Self {
        ResourceContent::Text {
            uri: uri.into(),
            text: text.into(),
            mime_type: None,
        }
    }

    /// Base64 binary contents for a URI.
    pub fn blob(uri: impl Into<String>, blob: impl Into<String>) -> Self {
        ResourceContent::Blob {
            uri: uri.into(),
            blob: blob.into(),
            mime_type: None,
        }
    }

    /// Set the MIME type.
    pub fn with_mime_type(mut self, mime: impl Into<String>) -> Self {
        match &mut self {
            ResourceContent::Text { mime_type, .. } | ResourceContent::Blob { mime_type, .. } => {
                *mime_type = Some(mime.into())
            }
        }
        self
    }

    /// The URI these contents belong to.
    pub fn uri(&self) -> &str {
        match self {
            ResourceContent::Text { uri, .. } | ResourceContent::Blob { uri, .. } => uri,
        }
    }
}

//
// Prompts
//

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Prompt {
    pub name: String,
    /// Human-readable display name (2025-06-18).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<PromptArgument>,
    /// Icons for display in user interfaces (2025-11-25).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Icon>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<Meta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PromptArgument {
    pub name: String,
    /// Human-readable display name (2025-06-18).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListPromptsParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPromptsResult {
    pub prompts: Vec<Prompt>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPromptParams {
    pub name: String,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub arguments: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetPromptResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub messages: Vec<PromptMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptMessage {
    pub role: Role,
    pub content: Content,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

//
// Logging
//

/// Params for `logging/setLevel`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetLevelParams {
    /// One of the eight RFC 5424 severities, lowercase.
    pub level: String,
}

/// Params for the `notifications/message` log notification.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogMessageParams {
    pub level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logger: Option<String>,
    /// Any JSON-serializable payload.
    pub data: Value,
}

//
// Ping
//

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PingParams {}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PingResult {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_tool_result_text() {
        let result = CallToolResult::text("hello world");
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"text\":\"hello world\""));
        assert!(!json.contains("\"isError\""));
    }

    #[test]
    fn test_tool_result_error() {
        let result = CallToolResult::error("something went wrong");
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"isError\":true"));
    }

    #[test]
    fn test_content_serialization() {
        let text = Content::text("hello");
        let json = serde_json::to_string(&text).unwrap();
        assert!(json.contains("\"type\":\"text\""));

        let image = Content::image("base64data", "image/png");
        let json = serde_json::to_string(&image).unwrap();
        assert!(json.contains("\"type\":\"image\""));
        assert!(json.contains("\"mimeType\":\"image/png\""));
    }

    //
    // Structured content / output schema (2025-06-18)
    //

    #[test]
    fn test_structured_result_mirrors_json_into_text() {
        #[derive(Serialize)]
        struct Weather {
            temperature: f64,
            conditions: String,
        }

        let result = CallToolResult::structured(Weather {
            temperature: 22.5,
            conditions: "Partly cloudy".into(),
        })
        .unwrap();

        // structuredContent is the real payload...
        assert_eq!(
            result.structured_content.as_ref().unwrap()["temperature"],
            22.5
        );
        // ...and the spec asks for a serialized mirror for older clients.
        let mirrored: Value = serde_json::from_str(result.content[0].as_text().unwrap()).unwrap();
        assert_eq!(mirrored["conditions"], "Partly cloudy");
        assert!(!result.is_error);
    }

    #[test]
    fn test_structured_content_serializes_under_the_spec_key() {
        let result = CallToolResult::text("hi").with_structured_content(json!({ "n": 1 }));
        let wire: Value = serde_json::to_value(&result).unwrap();
        assert_eq!(wire["structuredContent"]["n"], 1);
    }

    #[test]
    fn test_structured_content_omitted_when_absent() {
        let wire = serde_json::to_string(&CallToolResult::text("hi")).unwrap();
        assert!(!wire.contains("structuredContent"));
        assert!(!wire.contains("_meta"));
    }

    #[test]
    fn test_call_tool_result_meta_uses_underscore_key() {
        let result = CallToolResult::text("hi").with_meta_key("com.example/trace", json!("abc123"));
        let wire: Value = serde_json::to_value(&result).unwrap();
        assert_eq!(wire["_meta"]["com.example/trace"], "abc123");
    }

    #[test]
    fn test_call_tool_result_round_trips() {
        let original = CallToolResult::text("hi").with_structured_content(json!({ "a": [1, 2] }));
        let wire = serde_json::to_string(&original).unwrap();
        let parsed: CallToolResult = serde_json::from_str(&wire).unwrap();
        assert_eq!(parsed.structured_content.unwrap()["a"][1], 2);
    }

    //
    // Content types (2025-06-18)
    //

    #[test]
    fn test_audio_content_serialization() {
        let wire: Value = serde_json::to_value(Content::audio("YXVkaW8=", "audio/wav")).unwrap();
        assert_eq!(wire["type"], "audio");
        assert_eq!(wire["data"], "YXVkaW8=");
        assert_eq!(wire["mimeType"], "audio/wav");
    }

    #[test]
    fn test_resource_link_serialization() {
        let link = Content::resource_link("file:///project/src/main.rs", "main.rs");
        let wire: Value = serde_json::to_value(&link).unwrap();
        // The tag is snake_case here, unlike the other content types.
        assert_eq!(wire["type"], "resource_link");
        assert_eq!(wire["uri"], "file:///project/src/main.rs");
        assert_eq!(wire["name"], "main.rs");
        assert!(wire.get("title").is_none());
    }

    #[test]
    fn test_embedded_resource_nests_under_resource_key() {
        // The spec nests the resource; we used to flatten its fields into the
        // content block, which no conformant client would read.
        let content = Content::embedded(
            ResourceContent::text("file:///a.txt", "hello").with_mime_type("text/plain"),
        );
        let wire: Value = serde_json::to_value(&content).unwrap();

        assert_eq!(wire["type"], "resource");
        assert_eq!(wire["resource"]["uri"], "file:///a.txt");
        assert_eq!(wire["resource"]["text"], "hello");
        assert_eq!(wire["resource"]["mimeType"], "text/plain");
        assert!(wire.get("uri").is_none(), "must not be flattened");
    }

    #[test]
    fn test_content_annotations_round_trip() {
        let content = Content::text("hello").with_annotations(
            Annotations::for_audience([Role::User])
                .with_priority(0.8)
                .with_last_modified("2025-01-12T15:00:58Z"),
        );

        let wire: Value = serde_json::to_value(&content).unwrap();
        assert_eq!(wire["annotations"]["audience"][0], "user");
        assert_eq!(wire["annotations"]["priority"], 0.8);
        assert_eq!(wire["annotations"]["lastModified"], "2025-01-12T15:00:58Z");

        let parsed: Content = serde_json::from_value(wire).unwrap();
        assert_eq!(parsed.annotations().unwrap().audience, vec![Role::User]);
    }

    #[test]
    fn test_content_without_annotations_omits_the_key() {
        let wire = serde_json::to_string(&Content::text("hello")).unwrap();
        assert!(!wire.contains("annotations"));
        assert!(Content::text("x").annotations().is_none());
    }

    #[test]
    fn test_annotations_priority_is_clamped_to_the_defined_scale() {
        assert_eq!(
            Annotations::default().with_priority(5.0).priority,
            Some(1.0)
        );
        assert_eq!(
            Annotations::default().with_priority(-2.0).priority,
            Some(0.0)
        );
        assert_eq!(
            Annotations::default().with_priority(0.5).priority,
            Some(0.5)
        );
    }

    #[test]
    fn test_annotations_is_empty() {
        assert!(Annotations::default().is_empty());
        assert!(!Annotations::default().with_priority(0.1).is_empty());
        assert!(!Annotations::for_audience([Role::Assistant]).is_empty());
    }

    #[test]
    fn test_content_as_text() {
        assert_eq!(Content::text("hi").as_text(), Some("hi"));
        assert_eq!(Content::audio("d", "audio/wav").as_text(), None);
    }

    #[test]
    fn test_resource_content_helpers() {
        let text = ResourceContent::text("file:///a", "body").with_mime_type("text/plain");
        assert_eq!(text.uri(), "file:///a");
        let wire: Value = serde_json::to_value(&text).unwrap();
        assert_eq!(wire["mimeType"], "text/plain");

        let blob = ResourceContent::blob("file:///b", "AAAA").with_mime_type("image/png");
        assert_eq!(blob.uri(), "file:///b");
        let wire: Value = serde_json::to_value(&blob).unwrap();
        assert_eq!(wire["blob"], "AAAA");
    }

    //
    // Icons and titles (2025-06-18 / 2025-11-25)
    //

    #[test]
    fn test_icon_builder_and_serialization() {
        let icon = Icon::new("https://example.com/icon.svg")
            .with_mime_type("image/svg+xml")
            .with_sizes(["any"])
            .with_theme("dark");

        let wire: Value = serde_json::to_value(&icon).unwrap();
        assert_eq!(wire["src"], "https://example.com/icon.svg");
        assert_eq!(wire["mimeType"], "image/svg+xml");
        assert_eq!(wire["sizes"][0], "any");
        assert_eq!(wire["theme"], "dark");

        // Bare icon carries nothing extra.
        let bare = serde_json::to_string(&Icon::new("https://example.com/i.png")).unwrap();
        assert!(!bare.contains("sizes"));
        assert!(!bare.contains("theme"));
    }

    #[test]
    fn test_implementation_carries_display_metadata() {
        let mut implementation = Implementation::new("srv", "1.0.0");
        implementation.title = Some("My Server".into());
        implementation.description = Some("Does things".into());
        implementation.website_url = Some("https://example.com".into());
        implementation.icons = vec![Icon::new("https://example.com/i.png")];

        let wire: Value = serde_json::to_value(&implementation).unwrap();
        assert_eq!(wire["name"], "srv");
        assert_eq!(wire["title"], "My Server");
        assert_eq!(wire["description"], "Does things");
        assert_eq!(wire["websiteUrl"], "https://example.com");
        assert_eq!(wire["icons"][0]["src"], "https://example.com/i.png");

        // A minimal implementation stays minimal on the wire.
        let minimal = serde_json::to_string(&Implementation::new("s", "1")).unwrap();
        assert_eq!(minimal, r#"{"name":"s","version":"1"}"#);
    }

    #[test]
    fn test_tool_serializes_new_fields_only_when_set() {
        let bare = Tool {
            name: "t".into(),
            input_schema: json!({ "type": "object" }),
            ..Default::default()
        };
        let wire = serde_json::to_string(&bare).unwrap();
        for absent in [
            "title",
            "outputSchema",
            "icons",
            "execution",
            "_meta",
            "annotations",
        ] {
            assert!(!wire.contains(absent), "{absent} should be omitted: {wire}");
        }

        let full = Tool {
            name: "t".into(),
            title: Some("Tool".into()),
            input_schema: json!({ "type": "object" }),
            output_schema: Some(json!({ "type": "object" })),
            execution: Some(ToolExecution {
                task_support: Some(TaskSupport::Optional),
            }),
            icons: vec![Icon::new("https://example.com/i.png")],
            ..Default::default()
        };
        let wire: Value = serde_json::to_value(&full).unwrap();
        assert_eq!(wire["title"], "Tool");
        assert_eq!(wire["outputSchema"]["type"], "object");
        assert_eq!(wire["execution"]["taskSupport"], "optional");
    }

    #[test]
    fn test_task_support_wire_names() {
        for (value, name) in [
            (TaskSupport::Forbidden, "forbidden"),
            (TaskSupport::Optional, "optional"),
            (TaskSupport::Required, "required"),
        ] {
            assert_eq!(serde_json::to_value(value).unwrap(), json!(name));
            assert_eq!(
                serde_json::from_value::<TaskSupport>(json!(name)).unwrap(),
                value
            );
        }
        assert_eq!(TaskSupport::default(), TaskSupport::Forbidden);
    }

    #[test]
    fn test_tool_annotations_new_fields() {
        let annotations = ToolAnnotations::read_only()
            .with_title("Read Only Thing")
            .with_open_world(false);

        let wire: Value = serde_json::to_value(&annotations).unwrap();
        assert_eq!(wire["title"], "Read Only Thing");
        assert_eq!(wire["readOnlyHint"], true);
        assert_eq!(wire["openWorldHint"], false);

        // Absent open_world_hint must not be serialized as `false`: the
        // schema's default is `true`, so the two differ.
        let plain = serde_json::to_string(&ToolAnnotations::read_only()).unwrap();
        assert!(!plain.contains("openWorldHint"));

        assert!(ToolAnnotations::default().is_empty());
        assert!(!ToolAnnotations::default().with_title("x").is_empty());
        assert!(!ToolAnnotations::default().with_open_world(true).is_empty());
    }

    #[test]
    fn test_tool_annotation_constructors_still_set_expected_hints() {
        assert!(ToolAnnotations::read_only().read_only_hint);
        assert!(ToolAnnotations::idempotent().idempotent_hint);
        assert!(ToolAnnotations::destructive().destructive_hint);

        let both = ToolAnnotations::idempotent_destructive();
        assert!(both.idempotent_hint && both.destructive_hint && !both.read_only_hint);
    }

    #[test]
    fn test_resource_and_template_new_fields() {
        let resource = Resource {
            uri: "file:///a".into(),
            name: "a".into(),
            title: Some("File A".into()),
            size: Some(1024),
            icons: vec![Icon::new("https://example.com/i.png")],
            annotations: Some(Annotations::for_audience([Role::User])),
            ..Default::default()
        };
        let wire: Value = serde_json::to_value(&resource).unwrap();
        assert_eq!(wire["title"], "File A");
        assert_eq!(wire["size"], 1024);
        assert_eq!(wire["annotations"]["audience"][0], "user");

        let template = ResourceTemplate {
            uri_template: "file:///{path}".into(),
            name: "Project Files".into(),
            title: Some("📁 Project Files".into()),
            ..Default::default()
        };
        let wire: Value = serde_json::to_value(&template).unwrap();
        assert_eq!(wire["uriTemplate"], "file:///{path}");
        assert_eq!(wire["title"], "📁 Project Files");
    }

    #[test]
    fn test_prompt_new_fields() {
        let prompt = Prompt {
            name: "code_review".into(),
            title: Some("Request Code Review".into()),
            icons: vec![Icon::new("https://example.com/review.svg")],
            arguments: vec![PromptArgument {
                name: "code".into(),
                title: Some("Code".into()),
                required: true,
                ..Default::default()
            }],
            ..Default::default()
        };
        let wire: Value = serde_json::to_value(&prompt).unwrap();
        assert_eq!(wire["title"], "Request Code Review");
        assert_eq!(wire["icons"][0]["src"], "https://example.com/review.svg");
        assert_eq!(wire["arguments"][0]["title"], "Code");
    }

    //
    // Tool names
    //

    #[test]
    fn test_tool_name_validation() {
        for good in [
            "getUser",
            "DATA_EXPORT_v2",
            "admin.tools.list",
            "a",
            "x-y.z_1",
        ] {
            assert!(is_valid_tool_name(good), "{good} should be valid");
        }
        for bad in ["", "has space", "has,comma", "emoji🎉", "a/b", "a:b"] {
            assert!(!is_valid_tool_name(bad), "{bad} should be invalid");
        }

        assert!(is_valid_tool_name(&"a".repeat(128)));
        assert!(!is_valid_tool_name(&"a".repeat(129)));
    }

    //
    // Logging params
    //

    #[test]
    fn test_set_level_params_parse() {
        let params: SetLevelParams = serde_json::from_value(json!({ "level": "info" })).unwrap();
        assert_eq!(params.level, "info");
    }

    #[test]
    fn test_log_message_params_shape() {
        let params = LogMessageParams {
            level: "error".into(),
            logger: Some("database".into()),
            data: json!({ "error": "Connection failed" }),
        };
        let wire: Value = serde_json::to_value(&params).unwrap();
        assert_eq!(wire["level"], "error");
        assert_eq!(wire["logger"], "database");
        assert_eq!(wire["data"]["error"], "Connection failed");
    }
}
