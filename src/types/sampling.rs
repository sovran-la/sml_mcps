//! Sampling - servers asking the client's LLM for a completion.
//!
//! 2025-11-25 added tool calling (SEP-1577): a server can hand the model a
//! `tools` array, get back `tool_use` blocks, run them, and continue the
//! conversation with `tool_result` blocks. That loop has two structural rules
//! the spec states as MUSTs, both enforced by
//! [`CreateMessageParams::validate`]:
//!
//! - a message containing tool results contains *only* tool results
//! - every `tool_use` is answered by a matching `tool_result` in the very next
//!   user message, before anything else

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::types::{Annotations, Role};

/// What the client declared about sampling.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SamplingCapability {
    /// Present when the client can run tool-enabled sampling (2025-11-25).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Value>,
    /// Present when the client supports `includeContext` values other than
    /// `none`. Soft-deprecated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,
}

impl SamplingCapability {
    /// May we send a tool-enabled sampling request?
    ///
    /// Servers **MUST NOT** send one to a client that did not declare
    /// `sampling.tools`.
    pub fn supports_tools(&self) -> bool {
        self.tools.is_some()
    }

    /// May we ask for `includeContext` beyond `none`?
    pub fn supports_context(&self) -> bool {
        self.context.is_some()
    }
}

/// A block inside a sampling message.
///
/// Marked `#[non_exhaustive]`: match with a wildcard arm.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SamplingContent {
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Annotations>,
    },
    Image {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Annotations>,
    },
    Audio {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        annotations: Option<Annotations>,
    },
    /// The model wants a tool run. Assistant role only.
    ToolUse {
        /// Correlates with the `tool_result` that answers it.
        id: String,
        name: String,
        input: Value,
    },
    /// The outcome of a tool run. User role only.
    ToolResult {
        #[serde(rename = "toolUseId")]
        tool_use_id: String,
        content: Vec<SamplingContent>,
        // The enum-level `rename_all` renames variants, not fields, so every
        // multi-word field needs its camelCase name spelled out.
        #[serde(
            rename = "isError",
            default,
            skip_serializing_if = "std::ops::Not::not"
        )]
        is_error: bool,
    },
}

impl SamplingContent {
    pub fn text(text: impl Into<String>) -> Self {
        SamplingContent::Text {
            text: text.into(),
            annotations: None,
        }
    }

    pub fn image(data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        SamplingContent::Image {
            data: data.into(),
            mime_type: mime_type.into(),
            annotations: None,
        }
    }

    pub fn audio(data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        SamplingContent::Audio {
            data: data.into(),
            mime_type: mime_type.into(),
            annotations: None,
        }
    }

    /// A tool result answering the `tool_use` with id `tool_use_id`.
    pub fn tool_result(tool_use_id: impl Into<String>, text: impl Into<String>) -> Self {
        SamplingContent::ToolResult {
            tool_use_id: tool_use_id.into(),
            content: vec![SamplingContent::text(text)],
            is_error: false,
        }
    }

    /// A failed tool result.
    pub fn tool_error(tool_use_id: impl Into<String>, text: impl Into<String>) -> Self {
        SamplingContent::ToolResult {
            tool_use_id: tool_use_id.into(),
            content: vec![SamplingContent::text(text)],
            is_error: true,
        }
    }

    /// Is this a tool result block?
    pub fn is_tool_result(&self) -> bool {
        matches!(self, SamplingContent::ToolResult { .. })
    }

    /// The text of a text block, if that is what this is.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            SamplingContent::Text { text, .. } => Some(text),
            _ => None,
        }
    }
}

/// A message body: one block, or several.
///
/// The wire form is untagged, matching the spec's examples, which use a bare
/// object for single-block messages and an array otherwise.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SamplingBody {
    One(Box<SamplingContent>),
    Many(Vec<SamplingContent>),
}

impl SamplingBody {
    /// The blocks, however they were expressed.
    pub fn blocks(&self) -> &[SamplingContent] {
        match self {
            SamplingBody::One(content) => std::slice::from_ref(content),
            SamplingBody::Many(contents) => contents,
        }
    }
}

impl From<SamplingContent> for SamplingBody {
    fn from(content: SamplingContent) -> Self {
        SamplingBody::One(Box::new(content))
    }
}

impl From<Vec<SamplingContent>> for SamplingBody {
    fn from(contents: Vec<SamplingContent>) -> Self {
        SamplingBody::Many(contents)
    }
}

/// One turn of the conversation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingMessage {
    pub role: Role,
    pub content: SamplingBody,
}

impl SamplingMessage {
    /// A user turn.
    pub fn user(content: impl Into<SamplingBody>) -> Self {
        Self {
            role: Role::User,
            content: content.into(),
        }
    }

    /// An assistant turn.
    pub fn assistant(content: impl Into<SamplingBody>) -> Self {
        Self {
            role: Role::Assistant,
            content: content.into(),
        }
    }

    /// The blocks in this message.
    pub fn blocks(&self) -> &[SamplingContent] {
        self.content.blocks()
    }
}

/// A tool offered to the model during sampling (2025-11-25).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingTool {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: Value,
}

impl SamplingTool {
    pub fn new(name: impl Into<String>, input_schema: Value) -> Self {
        Self {
            name: name.into(),
            description: None,
            input_schema,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// How hard the model should try to use a tool.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ToolChoiceMode {
    /// The model decides. Default.
    #[default]
    Auto,
    /// The model **MUST** use at least one tool before finishing.
    Required,
    /// The model **MUST NOT** use any tool. Useful to force a final answer on
    /// the last iteration of a tool loop.
    None,
}

/// The `toolChoice` parameter.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ToolChoice {
    pub mode: ToolChoiceMode,
}

impl ToolChoice {
    pub fn auto() -> Self {
        Self {
            mode: ToolChoiceMode::Auto,
        }
    }
    pub fn required() -> Self {
        Self {
            mode: ToolChoiceMode::Required,
        }
    }
    pub fn none() -> Self {
        Self {
            mode: ToolChoiceMode::None,
        }
    }
}

/// A hint at a model or model family. Matched as a substring, advisory only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelHint {
    pub name: String,
}

impl ModelHint {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

/// What the server wants out of the model, without naming one.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ModelPreferences {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<ModelHint>,
    /// 0.0..=1.0. Higher prefers cheaper models.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_priority: Option<f64>,
    /// 0.0..=1.0. Higher prefers faster models.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed_priority: Option<f64>,
    /// 0.0..=1.0. Higher prefers more capable models.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intelligence_priority: Option<f64>,
}

impl ModelPreferences {
    /// Preferences that only hint at model names, in order.
    pub fn hinting<I, S>(hints: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            hints: hints.into_iter().map(ModelHint::new).collect(),
            ..Default::default()
        }
    }

    /// Priorities are a normalized 0..=1 scale, so out-of-range values clamp.
    pub fn with_cost_priority(mut self, priority: f64) -> Self {
        self.cost_priority = Some(priority.clamp(0.0, 1.0));
        self
    }
    pub fn with_speed_priority(mut self, priority: f64) -> Self {
        self.speed_priority = Some(priority.clamp(0.0, 1.0));
        self
    }
    pub fn with_intelligence_priority(mut self, priority: f64) -> Self {
        self.intelligence_priority = Some(priority.clamp(0.0, 1.0));
        self
    }
}

/// Params of `sampling/createMessage`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMessageParams {
    pub messages: Vec<SamplingMessage>,
    pub max_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_preferences: Option<ModelPreferences>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stop_sequences: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    /// `"none"` (default), or `"thisServer"`/`"allServers"` - both
    /// soft-deprecated and gated on the client's `sampling.context` capability.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_context: Option<String>,
    /// Tools the model may call (2025-11-25).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<SamplingTool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<crate::types::Meta>,
}

impl CreateMessageParams {
    /// A request with the two required parameters.
    pub fn new(messages: Vec<SamplingMessage>, max_tokens: u64) -> Self {
        Self {
            messages,
            max_tokens,
            system_prompt: None,
            model_preferences: None,
            temperature: None,
            stop_sequences: Vec::new(),
            metadata: None,
            include_context: None,
            tools: Vec::new(),
            tool_choice: None,
            meta: None,
        }
    }

    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into());
        self
    }

    pub fn with_model_preferences(mut self, preferences: ModelPreferences) -> Self {
        self.model_preferences = Some(preferences);
        self
    }

    pub fn with_temperature(mut self, temperature: f64) -> Self {
        self.temperature = Some(temperature);
        self
    }

    pub fn with_stop_sequences<I, S>(mut self, sequences: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.stop_sequences = sequences.into_iter().map(Into::into).collect();
        self
    }

    /// Offer tools to the model.
    pub fn with_tools(mut self, tools: impl IntoIterator<Item = SamplingTool>) -> Self {
        self.tools = tools.into_iter().collect();
        self
    }

    pub fn with_tool_choice(mut self, choice: ToolChoice) -> Self {
        self.tool_choice = Some(choice);
        self
    }

    /// Append a turn, e.g. the assistant's tool uses or the user's results.
    pub fn push_message(mut self, message: SamplingMessage) -> Self {
        self.messages.push(message);
        self
    }

    /// Check the message sequence against the spec's structural MUSTs.
    ///
    /// Catching this here beats letting the client reject it with -32602 after
    /// a round trip.
    pub fn validate(&self) -> Result<(), String> {
        if self.messages.is_empty() {
            return Err("sampling request must contain at least one message".into());
        }

        for (index, message) in self.messages.iter().enumerate() {
            let blocks = message.blocks();
            let results = blocks.iter().filter(|b| b.is_tool_result()).count();

            // "When a user message contains tool results, it MUST contain ONLY
            // tool results."
            if results > 0 && results != blocks.len() {
                return Err(format!(
                    "message {index} mixes tool results with other content; \
                     a message with tool results must contain only tool results"
                ));
            }
            if results > 0 && message.role != Role::User {
                return Err(format!(
                    "message {index} has tool results on a non-user role"
                ));
            }
            if message.role != Role::Assistant
                && blocks
                    .iter()
                    .any(|b| matches!(b, SamplingContent::ToolUse { .. }))
            {
                return Err(format!(
                    "message {index} has tool uses on a non-assistant role"
                ));
            }
        }

        // "every assistant message containing ToolUseContent blocks MUST be
        // followed by a user message that consists entirely of ToolResultContent
        // blocks, with each tool use matched by a corresponding tool result,
        // before any other message."
        for (index, message) in self.messages.iter().enumerate() {
            let uses: Vec<&str> = message
                .blocks()
                .iter()
                .filter_map(|b| match b {
                    SamplingContent::ToolUse { id, .. } => Some(id.as_str()),
                    _ => None,
                })
                .collect();
            if uses.is_empty() {
                continue;
            }

            let Some(next) = self.messages.get(index + 1) else {
                // A trailing tool-use turn is what the *client* returns; it is
                // only invalid once the server sends it back without results.
                return Err(format!(
                    "message {index} makes tool calls but no tool results follow"
                ));
            };

            let answered: Vec<&str> = next
                .blocks()
                .iter()
                .filter_map(|b| match b {
                    SamplingContent::ToolResult { tool_use_id, .. } => Some(tool_use_id.as_str()),
                    _ => None,
                })
                .collect();

            for id in &uses {
                if !answered.contains(id) {
                    return Err(format!(
                        "tool use `{id}` in message {index} has no matching tool result"
                    ));
                }
            }
            for id in &answered {
                if !uses.contains(id) {
                    return Err(format!(
                        "tool result `{id}` in message {} answers no tool use",
                        index + 1
                    ));
                }
            }
        }

        Ok(())
    }
}

/// Why the model stopped.
///
/// Not an enum: the spec allows provider-specific values beyond the ones it
/// names, and rejecting an unknown string would fail an otherwise fine
/// response.
pub mod stop_reason {
    /// The model finished its turn.
    pub const END_TURN: &str = "endTurn";
    /// The model wants tools run before continuing (2025-11-25).
    pub const TOOL_USE: &str = "toolUse";
    /// A stop sequence matched.
    pub const STOP_SEQUENCE: &str = "stopSequence";
    /// `maxTokens` was reached.
    pub const MAX_TOKENS: &str = "maxTokens";
}

/// The client's answer to `sampling/createMessage`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMessageResult {
    pub role: Role,
    pub content: SamplingBody,
    /// The model that actually ran, which may not be one that was hinted.
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<String>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<crate::types::Meta>,
}

impl CreateMessageResult {
    /// The blocks the model produced.
    pub fn blocks(&self) -> &[SamplingContent] {
        self.content.blocks()
    }

    /// Concatenated text of every text block.
    pub fn text(&self) -> String {
        self.blocks()
            .iter()
            .filter_map(SamplingContent::as_text)
            .collect::<Vec<_>>()
            .join("")
    }

    /// The tool calls the model made, in order.
    pub fn tool_uses(&self) -> Vec<(&str, &str, &Value)> {
        self.blocks()
            .iter()
            .filter_map(|b| match b {
                SamplingContent::ToolUse { id, name, input } => {
                    Some((id.as_str(), name.as_str(), input))
                }
                _ => None,
            })
            .collect()
    }

    /// Is the model waiting on tool results?
    ///
    /// Checks for actual `tool_use` blocks rather than trusting `stopReason`,
    /// since that field is advisory and provider-specific.
    pub fn wants_tools(&self) -> bool {
        !self.tool_uses().is_empty()
    }

    /// Turn this result into the assistant turn to append to the conversation.
    pub fn as_message(&self) -> SamplingMessage {
        SamplingMessage {
            role: self.role.clone(),
            content: self.content.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    //
    // Capability gating
    //

    #[test]
    fn plain_sampling_capability_forbids_tools() {
        let capability: SamplingCapability = serde_json::from_value(json!({})).unwrap();
        assert!(!capability.supports_tools());
        assert!(!capability.supports_context());
    }

    #[test]
    fn tool_capability_is_detected() {
        let capability: SamplingCapability =
            serde_json::from_value(json!({ "tools": {} })).unwrap();
        assert!(capability.supports_tools());
        assert!(!capability.supports_context());

        let context: SamplingCapability = serde_json::from_value(json!({ "context": {} })).unwrap();
        assert!(context.supports_context());
    }

    //
    // Wire shapes
    //

    #[test]
    fn basic_request_matches_the_spec_example() {
        let params = CreateMessageParams::new(
            vec![SamplingMessage::user(SamplingContent::text(
                "What is the capital of France?",
            ))],
            100,
        )
        .with_system_prompt("You are a helpful assistant.")
        .with_model_preferences(
            ModelPreferences::hinting(["claude-3-sonnet"])
                .with_intelligence_priority(0.8)
                .with_speed_priority(0.5),
        );

        let wire: Value = serde_json::to_value(&params).unwrap();
        assert_eq!(wire["messages"][0]["role"], "user");
        assert_eq!(wire["messages"][0]["content"]["type"], "text");
        assert_eq!(
            wire["messages"][0]["content"]["text"],
            "What is the capital of France?"
        );
        assert_eq!(wire["maxTokens"], 100);
        assert_eq!(wire["systemPrompt"], "You are a helpful assistant.");
        assert_eq!(
            wire["modelPreferences"]["hints"][0]["name"],
            "claude-3-sonnet"
        );
        assert_eq!(wire["modelPreferences"]["intelligencePriority"], 0.8);
        // Tool fields stay off the wire when unused.
        assert!(wire.get("tools").is_none());
        assert!(wire.get("toolChoice").is_none());
    }

    #[test]
    fn tool_enabled_request_matches_the_spec_example() {
        let params = CreateMessageParams::new(
            vec![SamplingMessage::user(SamplingContent::text(
                "What's the weather like in Paris and London?",
            ))],
            1000,
        )
        .with_tools([SamplingTool::new(
            "get_weather",
            json!({
                "type": "object",
                "properties": { "city": { "type": "string", "description": "City name" } },
                "required": ["city"]
            }),
        )
        .with_description("Get current weather for a city")])
        .with_tool_choice(ToolChoice::auto());

        let wire: Value = serde_json::to_value(&params).unwrap();
        assert_eq!(wire["tools"][0]["name"], "get_weather");
        assert_eq!(
            wire["tools"][0]["description"],
            "Get current weather for a city"
        );
        assert_eq!(wire["tools"][0]["inputSchema"]["required"][0], "city");
        assert_eq!(wire["toolChoice"]["mode"], "auto");
    }

    #[test]
    fn tool_use_and_result_blocks_use_snake_case_tags() {
        let use_block = SamplingContent::ToolUse {
            id: "call_abc123".into(),
            name: "get_weather".into(),
            input: json!({ "city": "Paris" }),
        };
        let wire: Value = serde_json::to_value(&use_block).unwrap();
        assert_eq!(wire["type"], "tool_use");
        assert_eq!(wire["id"], "call_abc123");
        assert_eq!(wire["input"]["city"], "Paris");

        let result = SamplingContent::tool_result("call_abc123", "18C, partly cloudy");
        let wire: Value = serde_json::to_value(&result).unwrap();
        assert_eq!(wire["type"], "tool_result");
        assert_eq!(wire["toolUseId"], "call_abc123");
        assert_eq!(wire["content"][0]["text"], "18C, partly cloudy");
        assert!(wire.get("isError").is_none());

        let failed = SamplingContent::tool_error("call_abc123", "boom");
        let wire: Value = serde_json::to_value(&failed).unwrap();
        assert_eq!(wire["isError"], true);
    }

    #[test]
    fn tool_choice_modes_round_trip() {
        for (choice, name) in [
            (ToolChoice::auto(), "auto"),
            (ToolChoice::required(), "required"),
            (ToolChoice::none(), "none"),
        ] {
            let wire: Value = serde_json::to_value(choice).unwrap();
            assert_eq!(wire["mode"], name);
        }
        assert_eq!(ToolChoiceMode::default(), ToolChoiceMode::Auto);
    }

    #[test]
    fn single_and_multi_block_bodies_both_parse() {
        let single: SamplingMessage = serde_json::from_value(
            json!({ "role": "user", "content": { "type": "text", "text": "hi" } }),
        )
        .unwrap();
        assert_eq!(single.blocks().len(), 1);
        assert_eq!(single.blocks()[0].as_text(), Some("hi"));

        let multi: SamplingMessage = serde_json::from_value(json!({
            "role": "assistant",
            "content": [
                { "type": "tool_use", "id": "a", "name": "t", "input": {} },
                { "type": "tool_use", "id": "b", "name": "t", "input": {} }
            ]
        }))
        .unwrap();
        assert_eq!(multi.blocks().len(), 2);
    }

    #[test]
    fn model_preference_priorities_clamp() {
        let preferences = ModelPreferences::default()
            .with_cost_priority(9.0)
            .with_speed_priority(-1.0)
            .with_intelligence_priority(0.5);
        assert_eq!(preferences.cost_priority, Some(1.0));
        assert_eq!(preferences.speed_priority, Some(0.0));
        assert_eq!(preferences.intelligence_priority, Some(0.5));
    }

    //
    // Results
    //

    #[test]
    fn text_result_parses() {
        let result: CreateMessageResult = serde_json::from_value(json!({
            "role": "assistant",
            "content": { "type": "text", "text": "The capital of France is Paris." },
            "model": "claude-3-sonnet-20240307",
            "stopReason": "endTurn"
        }))
        .unwrap();

        assert_eq!(result.text(), "The capital of France is Paris.");
        assert_eq!(result.model, "claude-3-sonnet-20240307");
        assert_eq!(result.stop_reason.as_deref(), Some(stop_reason::END_TURN));
        assert!(!result.wants_tools());
    }

    #[test]
    fn parallel_tool_use_result_parses() {
        let result: CreateMessageResult = serde_json::from_value(json!({
            "role": "assistant",
            "content": [
                { "type": "tool_use", "id": "call_abc123", "name": "get_weather", "input": { "city": "Paris" } },
                { "type": "tool_use", "id": "call_def456", "name": "get_weather", "input": { "city": "London" } }
            ],
            "model": "claude-3-sonnet-20240307",
            "stopReason": "toolUse"
        }))
        .unwrap();

        assert!(result.wants_tools());
        let uses = result.tool_uses();
        assert_eq!(uses.len(), 2);
        assert_eq!(uses[0].0, "call_abc123");
        assert_eq!(uses[0].1, "get_weather");
        assert_eq!(uses[1].2["city"], "London");
        assert_eq!(result.stop_reason.as_deref(), Some(stop_reason::TOOL_USE));
    }

    #[test]
    fn wants_tools_ignores_an_inaccurate_stop_reason() {
        // stopReason is advisory and provider-specific, so the block contents
        // are the source of truth.
        let result: CreateMessageResult = serde_json::from_value(json!({
            "role": "assistant",
            "content": [{ "type": "tool_use", "id": "a", "name": "t", "input": {} }],
            "model": "m",
            "stopReason": "someProviderSpecificReason"
        }))
        .unwrap();
        assert!(result.wants_tools());
    }

    #[test]
    fn missing_stop_reason_is_tolerated() {
        let result: CreateMessageResult = serde_json::from_value(json!({
            "role": "assistant",
            "content": { "type": "text", "text": "hi" },
            "model": "m"
        }))
        .unwrap();
        assert!(result.stop_reason.is_none());
    }

    #[test]
    fn result_converts_back_into_a_conversation_turn() {
        let result: CreateMessageResult = serde_json::from_value(json!({
            "role": "assistant",
            "content": [{ "type": "tool_use", "id": "a", "name": "t", "input": {} }],
            "model": "m",
            "stopReason": "toolUse"
        }))
        .unwrap();

        let message = result.as_message();
        assert_eq!(message.role, Role::Assistant);
        assert_eq!(message.blocks().len(), 1);
    }

    //
    // Structural validation of the tool loop
    //

    fn tool_use(id: &str) -> SamplingContent {
        SamplingContent::ToolUse {
            id: id.into(),
            name: "get_weather".into(),
            input: json!({}),
        }
    }

    #[test]
    fn a_complete_tool_loop_validates() {
        let params = CreateMessageParams::new(
            vec![
                SamplingMessage::user(SamplingContent::text("weather?")),
                SamplingMessage::assistant(vec![tool_use("a"), tool_use("b")]),
                SamplingMessage::user(vec![
                    SamplingContent::tool_result("a", "18C"),
                    SamplingContent::tool_result("b", "15C"),
                ]),
            ],
            1000,
        );
        assert!(params.validate().is_ok(), "{:?}", params.validate());
    }

    #[test]
    fn a_plain_conversation_validates() {
        let params = CreateMessageParams::new(
            vec![SamplingMessage::user(SamplingContent::text("hi"))],
            100,
        );
        assert!(params.validate().is_ok());
    }

    #[test]
    fn empty_message_list_is_rejected() {
        let params = CreateMessageParams::new(vec![], 100);
        assert!(params.validate().unwrap_err().contains("at least one"));
    }

    #[test]
    fn mixing_tool_results_with_other_content_is_rejected() {
        // The spec's explicit "Invalid - mixed content" example.
        let params = CreateMessageParams::new(
            vec![
                SamplingMessage::user(SamplingContent::text("weather?")),
                SamplingMessage::assistant(vec![tool_use("a")]),
                SamplingMessage::user(vec![
                    SamplingContent::text("Here are the results:"),
                    SamplingContent::tool_result("a", "18C"),
                ]),
            ],
            1000,
        );
        let err = params.validate().unwrap_err();
        assert!(err.contains("only tool results"), "{err}");
    }

    #[test]
    fn an_unanswered_tool_use_is_rejected() {
        // The spec's "Invalid sequence - missing tool result" example.
        let params = CreateMessageParams::new(
            vec![
                SamplingMessage::user(SamplingContent::text("weather?")),
                SamplingMessage::assistant(vec![tool_use("a"), tool_use("b")]),
                SamplingMessage::user(vec![SamplingContent::tool_result("a", "18C")]),
            ],
            1000,
        );
        let err = params.validate().unwrap_err();
        assert!(err.contains("`b`"), "{err}");
        assert!(err.contains("no matching tool result"), "{err}");
    }

    #[test]
    fn a_tool_result_answering_nothing_is_rejected() {
        let params = CreateMessageParams::new(
            vec![
                SamplingMessage::assistant(vec![tool_use("a")]),
                SamplingMessage::user(vec![
                    SamplingContent::tool_result("a", "ok"),
                    SamplingContent::tool_result("ghost", "?"),
                ]),
            ],
            1000,
        );
        let err = params.validate().unwrap_err();
        assert!(err.contains("`ghost`"), "{err}");
        assert!(err.contains("answers no tool use"), "{err}");
    }

    #[test]
    fn a_trailing_tool_use_with_no_results_is_rejected() {
        let params =
            CreateMessageParams::new(vec![SamplingMessage::assistant(vec![tool_use("a")])], 1000);
        assert!(
            params
                .validate()
                .unwrap_err()
                .contains("no tool results follow")
        );
    }

    #[test]
    fn tool_results_on_a_non_user_role_are_rejected() {
        let params = CreateMessageParams::new(
            vec![
                SamplingMessage::assistant(vec![tool_use("a")]),
                SamplingMessage::assistant(vec![SamplingContent::tool_result("a", "x")]),
            ],
            1000,
        );
        assert!(params.validate().unwrap_err().contains("non-user role"));
    }

    #[test]
    fn tool_uses_on_a_non_assistant_role_are_rejected() {
        let params =
            CreateMessageParams::new(vec![SamplingMessage::user(vec![tool_use("a")])], 1000);
        assert!(
            params
                .validate()
                .unwrap_err()
                .contains("non-assistant role")
        );
    }

    #[test]
    fn a_multi_turn_loop_validates() {
        let params = CreateMessageParams::new(
            vec![
                SamplingMessage::user(SamplingContent::text("weather?")),
                SamplingMessage::assistant(vec![tool_use("a")]),
                SamplingMessage::user(vec![SamplingContent::tool_result("a", "18C")]),
                SamplingMessage::assistant(SamplingContent::text("It is 18C.")),
                SamplingMessage::user(SamplingContent::text("and tomorrow?")),
                SamplingMessage::assistant(vec![tool_use("b")]),
                SamplingMessage::user(vec![SamplingContent::tool_result("b", "20C")]),
            ],
            1000,
        );
        assert!(params.validate().is_ok(), "{:?}", params.validate());
    }

    #[test]
    fn push_message_appends_in_order() {
        let params =
            CreateMessageParams::new(vec![SamplingMessage::user(SamplingContent::text("a"))], 10)
                .push_message(SamplingMessage::assistant(SamplingContent::text("b")));

        assert_eq!(params.messages.len(), 2);
        assert_eq!(params.messages[1].blocks()[0].as_text(), Some("b"));
    }
}
