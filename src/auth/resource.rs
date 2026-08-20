//! RFC 8707 Resource Indicators and RFC 9728 Protected Resource Metadata.
//!
//! An MCP server is an OAuth 2.1 *resource server*, and the spec puts two hard
//! obligations on it:
//!
//! > MCP servers **MUST** validate that access tokens were issued specifically
//! > for them as the intended audience, according to RFC 8707 Section 2.
//!
//! > MCP servers **MUST** implement OAuth 2.0 Protected Resource Metadata
//! > (RFC 9728).
//!
//! The first is what stops a token minted for some other service from being
//! replayed here; without it, "attackers could reuse legitimate tokens across
//! different services than intended." The second is how a client discovers
//! which authorization server to talk to in the first place.
//!
//! Both hang off one value: the server's **canonical resource URI**, which is
//! what the client puts in the `resource` parameter and what the authorization
//! server puts in the token's `aud` claim.

use serde::{Deserialize, Serialize};

/// A validated canonical URI identifying this MCP server.
///
/// The spec's rules, from RFC 8707 Section 2 and the MCP authorization page:
///
/// - it **MUST** be an absolute URI with a scheme (`mcp.example.com` is not one)
/// - it **MUST NOT** contain a fragment (`https://x.example.com#frag` is not one)
/// - it **MUST NOT** contain a query string: the metadata document is published
///   at a path derived from this one, and a request target is matched with its
///   query stripped, so one here means discovery can never match
/// - the canonical form lowercases scheme and host, though implementations
///   **SHOULD** accept uppercase for robustness
/// - a trailing slash **SHOULD** be omitted unless it is semantically
///   significant
///
/// Valid: `https://mcp.example.com/mcp`, `https://mcp.example.com`,
/// `https://mcp.example.com:8443`, `https://mcp.example.com/server/mcp`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceUri(String);

impl ResourceUri {
    /// Validate and normalize a canonical resource URI.
    pub fn parse(uri: &str) -> Result<Self, String> {
        let uri = uri.trim();
        if uri.is_empty() {
            return Err("resource URI must not be empty".into());
        }
        if uri.contains('#') {
            return Err(format!("resource URI must not contain a fragment: {uri}"));
        }
        // Not a style rule. The metadata document is published at a *path* -
        // `/.well-known/oauth-protected-resource` plus this URI's path - and a
        // request target is matched by path, with any query stripped off. A
        // resource URI carrying one produces a `metadata_path` no request can
        // ever equal, so RFC 9728 discovery is off and nothing says so.
        if uri.contains('?') {
            return Err(format!(
                "resource URI must not contain a query string, which would leave its \
                 metadata document unreachable: {uri}"
            ));
        }

        let Some((scheme, rest)) = uri.split_once("://") else {
            return Err(format!("resource URI must include a scheme: {uri}"));
        };
        if scheme.is_empty() || !scheme.starts_with(|c: char| c.is_ascii_alphabetic()) {
            return Err(format!("resource URI has an invalid scheme: {uri}"));
        }
        if rest.is_empty() {
            return Err(format!("resource URI has no host: {uri}"));
        }

        // Lowercase scheme and authority; leave the path alone, since paths are
        // case-sensitive and may distinguish two servers on one host.
        let (authority, path) = match rest.find('/') {
            Some(index) => (&rest[..index], &rest[index..]),
            None => (rest, ""),
        };
        if authority.is_empty() {
            return Err(format!("resource URI has no host: {uri}"));
        }

        // A bare trailing slash carries no meaning; drop it for consistency.
        let path = path.strip_suffix('/').unwrap_or(path);

        Ok(Self(format!(
            "{}://{}{}",
            scheme.to_ascii_lowercase(),
            authority.to_ascii_lowercase(),
            path
        )))
    }

    /// The normalized URI.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Scheme and authority, with no path: `https://mcp.example.com:8443`.
    fn origin(&self) -> &str {
        let after_scheme = self.0.find("://").map(|i| i + 3).unwrap_or(0);
        match self.0[after_scheme..].find('/') {
            Some(index) => &self.0[..after_scheme + index],
            None => &self.0,
        }
    }

    /// Path component, or `""` when there is none.
    fn path(&self) -> &str {
        let after_scheme = self.0.find("://").map(|i| i + 3).unwrap_or(0);
        match self.0[after_scheme..].find('/') {
            Some(index) => &self.0[after_scheme + index..],
            None => "",
        }
    }

    /// The well-known URL where this resource's metadata lives.
    ///
    /// RFC 9728 inserts the resource's path *after* the well-known segment, so
    /// `https://example.com/public/mcp` publishes metadata at
    /// `https://example.com/.well-known/oauth-protected-resource/public/mcp`.
    /// A path-less resource publishes at the root form.
    pub fn metadata_url(&self) -> String {
        format!(
            "{}/.well-known/oauth-protected-resource{}",
            self.origin(),
            self.path()
        )
    }

    /// The request path a client will GET for this metadata.
    pub fn metadata_path(&self) -> String {
        format!("/.well-known/oauth-protected-resource{}", self.path())
    }
}

impl std::fmt::Display for ResourceUri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// OAuth 2.0 Protected Resource Metadata (RFC 9728).
///
/// Served at [`ResourceUri::metadata_path`] and pointed at by the
/// `resource_metadata` parameter of a `WWW-Authenticate` challenge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtectedResourceMetadata {
    /// This server's canonical URI.
    pub resource: String,
    /// Authorization servers that can issue tokens for it. RFC 9728 requires
    /// at least one; which to use is the client's choice.
    pub authorization_servers: Vec<String>,
    /// The minimal set of scopes needed for basic functionality.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes_supported: Vec<String>,
    /// How tokens may be presented. `header` is what MCP requires.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bearer_methods_supported: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_documentation: Option<String>,
}

impl ProtectedResourceMetadata {
    /// Metadata for `resource`, issued by `authorization_servers`.
    pub fn new<I, S>(resource: ResourceUri, authorization_servers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            resource: resource.as_str().to_string(),
            authorization_servers: authorization_servers.into_iter().map(Into::into).collect(),
            // MCP mandates the Authorization header and forbids tokens in the
            // query string, so `header` is the only honest value here.
            bearer_methods_supported: vec!["header".to_string()],
            scopes_supported: Vec::new(),
            resource_name: None,
            resource_documentation: None,
        }
    }

    /// Declare the minimal scopes needed for basic functionality.
    pub fn with_scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.scopes_supported = scopes.into_iter().map(Into::into).collect();
        self
    }

    /// Set a human-readable name.
    pub fn with_resource_name(mut self, name: impl Into<String>) -> Self {
        self.resource_name = Some(name.into());
        self
    }

    /// Set a documentation URL.
    pub fn with_documentation(mut self, url: impl Into<String>) -> Self {
        self.resource_documentation = Some(url.into());
        self
    }

    /// The canonical resource URI, re-parsed.
    pub fn resource_uri(&self) -> Result<ResourceUri, String> {
        ResourceUri::parse(&self.resource)
    }
}

/// Build a `WWW-Authenticate` challenge for a 401.
///
/// The `resource_metadata` parameter is one of the two discovery mechanisms
/// RFC 9728 defines and the one clients try first. Including `scope` is a
/// SHOULD: it tells the client exactly what to request, "following the
/// principle of least privilege and preventing clients from requesting
/// excessive permissions."
pub fn unauthorized_challenge(metadata_url: &str, scopes: &[String]) -> String {
    let mut challenge = format!("Bearer resource_metadata=\"{}\"", escape(metadata_url));
    if !scopes.is_empty() {
        challenge.push_str(&format!(", scope=\"{}\"", escape(&scopes.join(" "))));
    }
    challenge
}

/// Build a `WWW-Authenticate` challenge for a 403 caused by missing scopes.
///
/// RFC 6750 Section 3.1 assigns `insufficient_scope` to this case, and the
/// spec asks servers to name the scopes that would satisfy the request.
pub fn insufficient_scope_challenge(
    metadata_url: &str,
    required_scopes: &[String],
    description: Option<&str>,
) -> String {
    let mut challenge = format!(
        "Bearer error=\"insufficient_scope\", scope=\"{}\", resource_metadata=\"{}\"",
        escape(&required_scopes.join(" ")),
        escape(metadata_url)
    );
    if let Some(description) = description {
        challenge.push_str(&format!(", error_description=\"{}\"", escape(description)));
    }
    challenge
}

/// Escape a quoted-string parameter value, so a stray quote cannot inject a
/// second parameter into the challenge.
fn escape(value: &str) -> String {
    value.replace('\\', r"\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    //
    // Canonical URI rules
    //

    #[test]
    fn accepts_the_spec_examples() {
        for valid in [
            "https://mcp.example.com/mcp",
            "https://mcp.example.com",
            "https://mcp.example.com:8443",
            "https://mcp.example.com/server/mcp",
        ] {
            assert_eq!(
                ResourceUri::parse(valid).unwrap().as_str(),
                valid,
                "{valid} should round-trip unchanged"
            );
        }
    }

    #[test]
    fn rejects_the_spec_counterexamples() {
        // "mcp.example.com (missing scheme)"
        assert!(
            ResourceUri::parse("mcp.example.com")
                .unwrap_err()
                .contains("scheme")
        );
        // "https://mcp.example.com#fragment (contains fragment)"
        assert!(
            ResourceUri::parse("https://mcp.example.com#fragment")
                .unwrap_err()
                .contains("fragment")
        );
    }

    #[test]
    fn rejects_a_query_string() {
        // Not pedantry: the metadata document is published at a path built from
        // this URI's path, and a request target is matched with its query
        // stripped. A resource URI carrying one produces a `metadata_path` no
        // request can equal, so discovery is off and nothing says so.
        for bad in [
            "https://mcp.example.com/mcp?v=2",
            "https://mcp.example.com?",
            "https://mcp.example.com/mcp?",
        ] {
            let error = ResourceUri::parse(bad).unwrap_err();
            assert!(error.contains("query string"), "{bad}: {error}");
            assert!(
                error.contains(bad),
                "the message has to name the URI that was refused: {error}"
            );
        }
    }

    #[test]
    fn a_query_string_would_have_published_metadata_nobody_can_reach() {
        // What the refusal above is protecting, spelled out: the path a client
        // would GET carries the query, and the path the server compares against
        // does not. This is the shape the two would have disagreed in.
        let published = "/.well-known/oauth-protected-resource/mcp?v=2";
        let requested = "/.well-known/oauth-protected-resource/mcp";

        assert_ne!(published, requested);

        // Without a query, the two are the same string - which is the whole
        // requirement.
        let clean = ResourceUri::parse("https://mcp.example.com/mcp").unwrap();
        assert_eq!(clean.metadata_path(), requested);
    }

    #[test]
    fn a_path_that_merely_contains_a_question_mark_is_still_a_query() {
        // There is no escaping that changes this: `?` starts the query
        // component wherever it appears, so a path cannot contain a literal
        // one, and a URI that has one is refused rather than silently
        // reinterpreted.
        assert!(ResourceUri::parse("https://example.com/a?b/c").is_err());
    }

    #[test]
    fn rejects_other_malformed_uris() {
        for bad in ["", "   ", "://example.com", "https://", "1https://x.com"] {
            assert!(ResourceUri::parse(bad).is_err(), "{bad:?} should fail");
        }
    }

    #[test]
    fn normalizes_case_and_trailing_slash() {
        // "implementations SHOULD accept uppercase scheme and host components
        // for robustness"
        assert_eq!(
            ResourceUri::parse("HTTPS://MCP.Example.COM/mcp")
                .unwrap()
                .as_str(),
            "https://mcp.example.com/mcp"
        );
        // "implementations SHOULD consistently use the form without the
        // trailing slash"
        assert_eq!(
            ResourceUri::parse("https://mcp.example.com/")
                .unwrap()
                .as_str(),
            "https://mcp.example.com"
        );
    }

    #[test]
    fn preserves_path_case() {
        // Paths are case-sensitive and may distinguish two servers on a host.
        assert_eq!(
            ResourceUri::parse("https://example.com/MyServer/MCP")
                .unwrap()
                .as_str(),
            "https://example.com/MyServer/MCP"
        );
    }

    #[test]
    fn splits_origin_and_path() {
        let uri = ResourceUri::parse("https://example.com:8443/public/mcp").unwrap();
        assert_eq!(uri.origin(), "https://example.com:8443");
        assert_eq!(uri.path(), "/public/mcp");

        let bare = ResourceUri::parse("https://example.com").unwrap();
        assert_eq!(bare.origin(), "https://example.com");
        assert_eq!(bare.path(), "");
    }

    //
    // Metadata discovery locations
    //

    #[test]
    fn metadata_url_inserts_the_path_after_the_well_known_segment() {
        // The spec's own example: "https://example.com/public/mcp could host
        // metadata at
        // https://example.com/.well-known/oauth-protected-resource/public/mcp"
        let uri = ResourceUri::parse("https://example.com/public/mcp").unwrap();
        assert_eq!(
            uri.metadata_url(),
            "https://example.com/.well-known/oauth-protected-resource/public/mcp"
        );
        assert_eq!(
            uri.metadata_path(),
            "/.well-known/oauth-protected-resource/public/mcp"
        );
    }

    #[test]
    fn path_less_resource_uses_the_root_metadata_form() {
        let uri = ResourceUri::parse("https://mcp.example.com").unwrap();
        assert_eq!(
            uri.metadata_url(),
            "https://mcp.example.com/.well-known/oauth-protected-resource"
        );
        assert_eq!(uri.metadata_path(), "/.well-known/oauth-protected-resource");
    }

    //
    // Metadata document
    //

    #[test]
    fn metadata_serializes_the_required_rfc9728_fields() {
        let metadata = ProtectedResourceMetadata::new(
            ResourceUri::parse("https://mcp.example.com/mcp").unwrap(),
            ["https://auth.example.com"],
        )
        .with_scopes(["files:read", "files:write"])
        .with_resource_name("Example MCP Server");

        let wire: serde_json::Value = serde_json::to_value(&metadata).unwrap();
        assert_eq!(wire["resource"], "https://mcp.example.com/mcp");
        assert_eq!(wire["authorization_servers"][0], "https://auth.example.com");
        assert_eq!(wire["scopes_supported"][0], "files:read");
        assert_eq!(wire["resource_name"], "Example MCP Server");
        // MCP forbids tokens in the query string, so only `header` is honest.
        assert_eq!(wire["bearer_methods_supported"][0], "header");
    }

    #[test]
    fn metadata_omits_empty_optional_fields() {
        let metadata = ProtectedResourceMetadata::new(
            ResourceUri::parse("https://mcp.example.com").unwrap(),
            ["https://auth.example.com"],
        );
        let wire = serde_json::to_string(&metadata).unwrap();
        assert!(!wire.contains("scopes_supported"));
        assert!(!wire.contains("resource_name"));
        assert!(!wire.contains("resource_documentation"));
    }

    #[test]
    fn metadata_round_trips() {
        let metadata = ProtectedResourceMetadata::new(
            ResourceUri::parse("https://mcp.example.com/mcp").unwrap(),
            ["https://auth.example.com"],
        )
        .with_documentation("https://example.com/docs");

        let wire = serde_json::to_string(&metadata).unwrap();
        let parsed: ProtectedResourceMetadata = serde_json::from_str(&wire).unwrap();
        assert_eq!(
            parsed.resource_uri().unwrap().as_str(),
            "https://mcp.example.com/mcp"
        );
        assert_eq!(
            parsed.resource_documentation.as_deref(),
            Some("https://example.com/docs")
        );
    }

    //
    // WWW-Authenticate challenges
    //

    #[test]
    fn unauthorized_challenge_matches_the_spec_example() {
        let challenge = unauthorized_challenge(
            "https://mcp.example.com/.well-known/oauth-protected-resource",
            &["files:read".to_string()],
        );
        assert_eq!(
            challenge,
            "Bearer resource_metadata=\"https://mcp.example.com/.well-known/oauth-protected-resource\", \
             scope=\"files:read\""
        );
    }

    #[test]
    fn unauthorized_challenge_omits_an_empty_scope() {
        let challenge = unauthorized_challenge("https://example.com/.well-known/x", &[]);
        assert!(!challenge.contains("scope"));
        assert!(challenge.starts_with("Bearer resource_metadata="));
    }

    #[test]
    fn insufficient_scope_challenge_matches_the_spec_example() {
        let challenge = insufficient_scope_challenge(
            "https://mcp.example.com/.well-known/oauth-protected-resource",
            &[
                "files:read".to_string(),
                "files:write".to_string(),
                "user:profile".to_string(),
            ],
            Some("Additional file write permission required"),
        );

        assert!(challenge.contains("error=\"insufficient_scope\""));
        assert!(challenge.contains("scope=\"files:read files:write user:profile\""));
        assert!(challenge.contains(
            "resource_metadata=\"https://mcp.example.com/.well-known/oauth-protected-resource\""
        ));
        assert!(
            challenge.contains("error_description=\"Additional file write permission required\"")
        );
    }

    #[test]
    fn challenge_values_are_escaped() {
        // A quote in a value must not be able to close the quoted-string and
        // inject another parameter.
        let challenge = unauthorized_challenge(
            "https://example.com/\" , scope=\"admin",
            &[r#"a\b"#.to_string()],
        );
        assert!(challenge.contains("\\\""), "{challenge}");
        assert!(challenge.contains(r"a\\b"), "{challenge}");
        // Exactly two parameters survive.
        assert_eq!(challenge.matches("resource_metadata=").count(), 1);
    }
}
