//! JWT Token Validation
//!
//! Validates tokens and extracts claims. Does NOT issue tokens -
//! that's the job of your OAuth provider (Auth0, Cognito, etc).

use crate::auth::ResourceUri;
use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation, decode};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum JwtError {
    #[error("Missing Authorization header")]
    MissingHeader,

    #[error("Invalid Authorization header format (expected 'Bearer <token>')")]
    InvalidFormat,

    #[error("Token validation failed: {0}")]
    ValidationFailed(#[from] jsonwebtoken::errors::Error),

    #[error("Token expired")]
    Expired,

    #[error("Invalid issuer")]
    InvalidIssuer,

    #[error("Invalid audience")]
    InvalidAudience,
}

/// Standard JWT claims plus custom fields for multi-tenancy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// Subject (user ID)
    pub sub: String,

    /// Expiration time (Unix timestamp)
    pub exp: u64,

    /// Issued at (Unix timestamp)
    #[serde(default)]
    pub iat: u64,

    /// Issuer
    #[serde(default)]
    pub iss: Option<String>,

    /// Audience: the resource(s) this token was minted for.
    ///
    /// RFC 7519 allows either a single string or an array, and a token issued
    /// for several resources uses the array form - so this must accept both or
    /// multi-audience tokens fail to deserialize before validation even runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aud: Option<Audience>,

    /// Tenant ID for multi-tenancy (custom claim)
    #[serde(default)]
    pub tenant_id: Option<String>,

    /// Scopes/permissions (custom claim)
    #[serde(default)]
    pub scope: Option<String>,
}

/// A JWT `aud` claim: one audience or several.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Audience {
    One(String),
    Many(Vec<String>),
}

impl Audience {
    /// The audiences, however they were expressed.
    pub fn values(&self) -> &[String] {
        match self {
            Audience::One(value) => std::slice::from_ref(value),
            Audience::Many(values) => values,
        }
    }

    /// Does this token name `resource` as an intended audience?
    pub fn contains(&self, resource: &str) -> bool {
        self.values().iter().any(|value| value == resource)
    }
}

impl Claims {
    /// The audiences this token was issued for.
    pub fn audiences(&self) -> &[String] {
        self.aud.as_ref().map(Audience::values).unwrap_or(&[])
    }

    /// Was this token issued for `resource`?
    ///
    /// The check that stops a token minted for another service being replayed
    /// here: "MCP servers MUST only accept tokens specifically intended for
    /// themselves and MUST reject tokens that do not include them in the
    /// audience claim."
    pub fn is_for_resource(&self, resource: &ResourceUri) -> bool {
        self.aud
            .as_ref()
            .is_some_and(|aud| aud.contains(resource.as_str()))
    }

    /// Every scope on this token.
    pub fn scopes(&self) -> Vec<&str> {
        self.scope
            .as_deref()
            .map(|s| s.split_whitespace().collect())
            .unwrap_or_default()
    }

    /// Which of `required` this token is missing.
    ///
    /// An empty result means the request is authorized; anything else is what
    /// belongs in the `scope` parameter of an `insufficient_scope` challenge.
    pub fn missing_scopes<'a>(&self, required: &'a [String]) -> Vec<&'a str> {
        required
            .iter()
            .map(String::as_str)
            .filter(|scope| !self.has_scope(scope))
            .collect()
    }

    /// Get the user ID (subject)
    pub fn user_id(&self) -> &str {
        &self.sub
    }

    /// Get tenant ID, falling back to user ID if not set
    pub fn tenant_id(&self) -> &str {
        self.tenant_id.as_deref().unwrap_or(&self.sub)
    }

    /// Check if a scope is present
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scope
            .as_ref()
            .map(|s| s.split_whitespace().any(|s| s == scope))
            .unwrap_or(false)
    }
}

/// JWT Validator configuration
pub struct JwtValidator {
    decoding_key: DecodingKey,
    validation: Validation,
    /// Set by [`JwtValidator::for_resource`]; enforced after decoding.
    resource: Option<ResourceUri>,
}

impl JwtValidator {
    /// Create a validator for HS256 (symmetric) tokens
    ///
    /// Use this for development/testing. In production, prefer RS256.
    pub fn hs256(secret: &[u8]) -> Self {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;

        Self {
            decoding_key: DecodingKey::from_secret(secret),
            validation,
            resource: None,
        }
    }

    /// Create a validator for RS256 (asymmetric) tokens
    ///
    /// Use this in production with your OAuth provider's public key.
    pub fn rs256_pem(public_key_pem: &[u8]) -> Result<Self, JwtError> {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = true;

        Ok(Self {
            decoding_key: DecodingKey::from_rsa_pem(public_key_pem)?,
            validation,
            resource: None,
        })
    }

    /// Create a validator for RS256 using JWKS components (n, e)
    pub fn rs256_components(n: &str, e: &str) -> Result<Self, JwtError> {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = true;

        Ok(Self {
            decoding_key: DecodingKey::from_rsa_components(n, e)?,
            validation,
            resource: None,
        })
    }

    /// Require a specific issuer
    pub fn with_issuer(mut self, issuer: &str) -> Self {
        self.validation.set_issuer(&[issuer]);
        self
    }

    /// Require a specific audience
    pub fn with_audience(mut self, audience: &str) -> Self {
        self.validation.set_audience(&[audience]);
        self
    }

    /// Bind this validator to the server's canonical resource URI.
    ///
    /// This is the RFC 8707 audience check the spec makes a MUST: a token is
    /// accepted only if its `aud` names this exact resource. Without it the
    /// server would accept tokens minted for other services, which "breaks a
    /// fundamental OAuth security boundary."
    ///
    /// Prefer this over [`JwtValidator::with_audience`], which takes an
    /// unvalidated string and does not enforce canonical-URI rules.
    pub fn for_resource(mut self, resource: &ResourceUri) -> Self {
        self.validation.set_audience(&[resource.as_str()]);
        self.validation.validate_aud = true;
        self.resource = Some(resource.clone());
        self
    }

    /// The resource this validator is bound to, if any.
    pub fn resource(&self) -> Option<&ResourceUri> {
        self.resource.as_ref()
    }

    /// Extract token from Authorization header
    pub fn extract_token(auth_header: &str) -> Result<&str, JwtError> {
        auth_header
            .strip_prefix("Bearer ")
            .ok_or(JwtError::InvalidFormat)
    }

    /// Validate a token and return claims
    pub fn validate(&self, token: &str) -> Result<Claims, JwtError> {
        let token_data: TokenData<Claims> = decode(token, &self.decoding_key, &self.validation)?;
        let claims = token_data.claims;

        // `jsonwebtoken` only compares the audience when the claim is present,
        // so a token with no `aud` at all passes its check. That is exactly the
        // token the spec says to refuse: servers "MUST reject tokens that do
        // not include them in the audience claim". Enforce it ourselves rather
        // than depend on the library's edge-case semantics.
        if let Some(resource) = &self.resource {
            if !claims.is_for_resource(resource) {
                return Err(JwtError::InvalidAudience);
            }
        }

        Ok(claims)
    }

    /// Validate from Authorization header value
    pub fn validate_header(&self, auth_header: &str) -> Result<Claims, JwtError> {
        let token = Self::extract_token(auth_header)?;
        self.validate(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};

    fn create_test_token(claims: &Claims, secret: &[u8]) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            claims,
            &EncodingKey::from_secret(secret),
        )
        .unwrap()
    }

    #[test]
    fn test_validate_valid_token() {
        let secret = b"super-secret-key-for-testing";
        let validator = JwtValidator::hs256(secret);

        let claims = Claims {
            sub: "user-123".to_string(),
            exp: (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs())
                + 3600, // 1 hour from now
            iat: 0,
            iss: None,
            aud: None,
            tenant_id: Some("tenant-456".to_string()),
            scope: Some("read write".to_string()),
        };

        let token = create_test_token(&claims, secret);
        let validated = validator.validate(&token).unwrap();

        assert_eq!(validated.user_id(), "user-123");
        assert_eq!(validated.tenant_id(), "tenant-456");
        assert!(validated.has_scope("read"));
        assert!(validated.has_scope("write"));
        assert!(!validated.has_scope("admin"));
    }

    #[test]
    fn test_validate_expired_token() {
        let secret = b"super-secret-key-for-testing";
        let validator = JwtValidator::hs256(secret);

        let claims = Claims {
            sub: "user-123".to_string(),
            exp: 1000, // Way in the past
            iat: 0,
            iss: None,
            aud: None,
            tenant_id: None,
            scope: None,
        };

        let token = create_test_token(&claims, secret);
        let result = validator.validate(&token);

        assert!(result.is_err());
    }

    #[test]
    fn test_extract_token() {
        let header = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test";
        let token = JwtValidator::extract_token(header).unwrap();
        assert_eq!(token, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.test");
    }

    #[test]
    fn test_extract_token_invalid_format() {
        let header = "Basic abc123";
        let result = JwtValidator::extract_token(header);
        assert!(result.is_err());
    }

    //
    // RFC 8707 audience binding
    //

    fn resource() -> ResourceUri {
        ResourceUri::parse("https://mcp.example.com/mcp").unwrap()
    }

    fn claims_with(aud: Option<Audience>) -> Claims {
        Claims {
            sub: "user-123".into(),
            exp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600,
            iat: 0,
            iss: None,
            aud,
            tenant_id: None,
            scope: None,
        }
    }

    #[test]
    fn test_audience_accepts_string_and_array_forms() {
        // RFC 7519 allows either; a multi-audience token must still parse.
        let single: Claims =
            serde_json::from_value(serde_json::json!({ "sub": "u", "exp": 0, "aud": "https://a" }))
                .unwrap();
        assert_eq!(single.audiences(), ["https://a"]);

        let many: Claims = serde_json::from_value(
            serde_json::json!({ "sub": "u", "exp": 0, "aud": ["https://a", "https://b"] }),
        )
        .unwrap();
        assert_eq!(many.audiences(), ["https://a", "https://b"]);

        let none: Claims =
            serde_json::from_value(serde_json::json!({ "sub": "u", "exp": 0 })).unwrap();
        assert!(none.audiences().is_empty());
    }

    #[test]
    fn test_is_for_resource_matches_exactly() {
        let target = resource();

        assert!(claims_with(Some(Audience::One(target.as_str().into()))).is_for_resource(&target));
        assert!(
            claims_with(Some(Audience::Many(vec![
                "https://other.example.com".into(),
                target.as_str().into(),
            ])))
            .is_for_resource(&target)
        );

        assert!(!claims_with(None).is_for_resource(&target));
        assert!(
            !claims_with(Some(Audience::One("https://other.example.com/mcp".into())))
                .is_for_resource(&target)
        );
        // A prefix is not a match; a token for the host is not a token for us.
        assert!(
            !claims_with(Some(Audience::One("https://mcp.example.com".into())))
                .is_for_resource(&target)
        );
    }

    #[test]
    fn test_validator_rejects_a_token_for_another_resource() {
        let secret = b"secret";
        let validator = JwtValidator::hs256(secret).for_resource(&resource());

        let foreign = create_test_token(
            &claims_with(Some(Audience::One("https://other.example.com/mcp".into()))),
            secret,
        );
        assert!(matches!(
            validator.validate(&foreign),
            Err(JwtError::ValidationFailed(_)) | Err(JwtError::InvalidAudience)
        ));
    }

    #[test]
    fn test_validator_rejects_a_token_with_no_audience() {
        // jsonwebtoken skips its audience check when the claim is absent, so
        // this is the case the explicit check exists for.
        let secret = b"secret";
        let validator = JwtValidator::hs256(secret).for_resource(&resource());

        let token = create_test_token(&claims_with(None), secret);
        assert!(matches!(
            validator.validate(&token),
            Err(JwtError::InvalidAudience)
        ));
    }

    #[test]
    fn test_validator_accepts_a_token_for_this_resource() {
        let secret = b"secret";
        let target = resource();
        let validator = JwtValidator::hs256(secret).for_resource(&target);

        let token = create_test_token(
            &claims_with(Some(Audience::One(target.as_str().into()))),
            secret,
        );
        let validated = validator.validate(&token).unwrap();
        assert_eq!(validated.user_id(), "user-123");
        assert_eq!(validator.resource().unwrap().as_str(), target.as_str());
    }

    #[test]
    fn test_unbound_validator_ignores_audience() {
        // Without for_resource, audience is not this validator's business.
        let secret = b"secret";
        let validator = JwtValidator::hs256(secret);

        assert!(
            validator
                .validate(&create_test_token(&claims_with(None), secret))
                .is_ok()
        );
        assert!(validator.resource().is_none());
    }

    #[test]
    fn test_scope_helpers() {
        let mut claims = claims_with(None);
        claims.scope = Some("files:read files:write".into());

        assert_eq!(claims.scopes(), ["files:read", "files:write"]);
        assert!(
            claims
                .missing_scopes(&["files:read".to_string()])
                .is_empty()
        );
        assert_eq!(
            claims.missing_scopes(&["files:read".to_string(), "admin".to_string()]),
            ["admin"]
        );

        // No scope claim at all means everything is missing.
        let bare = claims_with(None);
        assert!(bare.scopes().is_empty());
        assert_eq!(bare.missing_scopes(&["any".to_string()]), ["any"]);
    }

    #[test]
    fn test_tenant_id_fallback() {
        let claims = Claims {
            sub: "user-123".to_string(),
            exp: 0,
            iat: 0,
            iss: None,
            aud: None,
            tenant_id: None, // No tenant_id
            scope: None,
        };

        // Should fall back to user_id
        assert_eq!(claims.tenant_id(), "user-123");
    }
}
