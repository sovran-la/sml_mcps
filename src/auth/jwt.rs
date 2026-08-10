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
///
/// # Audience is not optional
///
/// The authorization spec is blunt: MCP servers "**MUST** validate that access
/// tokens were issued specifically for them as the intended audience" and
/// "**MUST** reject tokens that do not include them in the audience claim".
/// A validator built without [`for_resource`](JwtValidator::for_resource) or
/// [`with_audience`](JwtValidator::with_audience) therefore cannot be used to
/// protect a server - [`HttpServer::serve_with_auth`](crate::HttpServer) refuses
/// to start with one.
///
/// The audience check is done here rather than delegated to `jsonwebtoken`,
/// whose semantics are the wrong way round for this purpose: with `aud`
/// configured it *ignores* a token that carries no `aud` at all, and with none
/// configured it *rejects* every token that has one - which is every RFC 8707
/// conformant token.
pub struct JwtValidator {
    decoding_key: DecodingKey,
    validation: Validation,
    /// Set by [`JwtValidator::for_resource`]; reported by
    /// [`resource`](JwtValidator::resource).
    resource: Option<ResourceUri>,
    /// Audiences a token must name at least one of. Empty means unbound.
    audiences: Vec<String>,
}

/// Validation settings shared by every constructor.
///
/// `validate_nbf` is off by default in `jsonwebtoken`, which accepts a
/// not-yet-valid token. `validate_aud` is deliberately off because the audience
/// check lives in [`JwtValidator::validate`]; see the type docs.
fn base_validation(algorithm: Algorithm) -> Validation {
    let mut validation = Validation::new(algorithm);
    validation.validate_exp = true;
    validation.validate_nbf = true;
    validation.validate_aud = false;
    validation
}

impl JwtValidator {
    /// Create a validator for HS256 (symmetric) tokens
    ///
    /// Use this for development/testing. In production, prefer RS256.
    ///
    /// Bind it to your resource before serving:
    /// `JwtValidator::hs256(secret).for_resource(&resource)`.
    pub fn hs256(secret: &[u8]) -> Self {
        Self {
            decoding_key: DecodingKey::from_secret(secret),
            validation: base_validation(Algorithm::HS256),
            resource: None,
            audiences: Vec::new(),
        }
    }

    /// Create a validator for RS256 (asymmetric) tokens
    ///
    /// Use this in production with your OAuth provider's public key.
    pub fn rs256_pem(public_key_pem: &[u8]) -> Result<Self, JwtError> {
        Ok(Self {
            decoding_key: DecodingKey::from_rsa_pem(public_key_pem)?,
            validation: base_validation(Algorithm::RS256),
            resource: None,
            audiences: Vec::new(),
        })
    }

    /// Create a validator for RS256 using JWKS components (n, e)
    pub fn rs256_components(n: &str, e: &str) -> Result<Self, JwtError> {
        Ok(Self {
            decoding_key: DecodingKey::from_rsa_components(n, e)?,
            validation: base_validation(Algorithm::RS256),
            resource: None,
            audiences: Vec::new(),
        })
    }

    /// Require a specific issuer
    pub fn with_issuer(mut self, issuer: &str) -> Self {
        self.validation.set_issuer(&[issuer]);
        self
    }

    /// Require a specific audience.
    ///
    /// A token is accepted only if its `aud` names this value. Prefer
    /// [`for_resource`](JwtValidator::for_resource), which takes a validated
    /// canonical URI; this exists for servers whose audience is not expressed
    /// as one.
    pub fn with_audience(mut self, audience: &str) -> Self {
        self.audiences.push(audience.to_string());
        self
    }

    /// Bind this validator to the server's canonical resource URI.
    ///
    /// This is the RFC 8707 audience check the spec makes a MUST: a token is
    /// accepted only if its `aud` names this exact resource. Without it the
    /// server would accept tokens minted for other services, which "breaks a
    /// fundamental OAuth security boundary."
    pub fn for_resource(mut self, resource: &ResourceUri) -> Self {
        self.audiences.push(resource.as_str().to_string());
        self.resource = Some(resource.clone());
        self
    }

    /// The resource this validator is bound to, if any.
    pub fn resource(&self) -> Option<&ResourceUri> {
        self.resource.as_ref()
    }

    /// Does this validator enforce an audience at all?
    ///
    /// `false` means it accepts tokens minted for anyone, which is the MUST
    /// violation `serve_with_auth` refuses to start with.
    pub fn binds_audience(&self) -> bool {
        !self.audiences.is_empty()
    }

    /// Extract token from Authorization header
    ///
    /// The scheme is matched case-insensitively: RFC 7235 §2.1, "The scheme
    /// name is case-insensitive", so `bearer <token>` is as valid as
    /// `Bearer <token>`.
    pub fn extract_token(auth_header: &str) -> Result<&str, JwtError> {
        let (scheme, token) = auth_header.split_once(' ').ok_or(JwtError::InvalidFormat)?;
        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err(JwtError::InvalidFormat);
        }
        let token = token.trim_start();
        if token.is_empty() {
            return Err(JwtError::InvalidFormat);
        }
        Ok(token)
    }

    /// Validate a token and return claims
    pub fn validate(&self, token: &str) -> Result<Claims, JwtError> {
        let token_data: TokenData<Claims> = decode(token, &self.decoding_key, &self.validation)?;
        let claims = token_data.claims;

        // An absent `aud` is the token the spec says to refuse, and it is the
        // one `jsonwebtoken` waves through. Checked here, where "no audience
        // configured" is the only way to opt out - and that is refused at the
        // point of use.
        if !self.audiences.is_empty() {
            let named = claims
                .aud
                .as_ref()
                .is_some_and(|aud| self.audiences.iter().any(|want| aud.contains(want)));
            if !named {
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
        // An unbound validator checks no audience, which is why
        // `serve_with_auth` refuses to start with one.
        let secret = b"secret";
        let validator = JwtValidator::hs256(secret);

        assert!(
            validator
                .validate(&create_test_token(&claims_with(None), secret))
                .is_ok()
        );
        assert!(validator.resource().is_none());
        assert!(!validator.binds_audience());
    }

    #[test]
    fn test_an_unbound_validator_no_longer_rejects_conformant_tokens() {
        // `Validation::new` leaves `validate_aud: true, aud: None`, which
        // `jsonwebtoken` turns into a hard `InvalidAudience` for any token that
        // *has* an `aud` - i.e. every RFC 8707 conformant one. Backwards: it
        // refused the good tokens and accepted the bad.
        let secret = b"secret";
        let validator = JwtValidator::hs256(secret);

        let conformant = create_test_token(
            &claims_with(Some(Audience::One("https://mcp.example.com/mcp".into()))),
            secret,
        );
        assert!(validator.validate(&conformant).is_ok());
    }

    #[test]
    fn test_with_audience_enforces_the_audience_it_names() {
        // `set_audience` alone does not do this: jsonwebtoken's
        // `(NotPresent, Some(_))` case falls through, so an `aud`-less token
        // used to sail past `with_audience` too.
        let secret = b"secret";
        let validator = JwtValidator::hs256(secret).with_audience("https://mcp.example.com/mcp");
        assert!(validator.binds_audience());

        assert!(matches!(
            validator.validate(&create_test_token(&claims_with(None), secret)),
            Err(JwtError::InvalidAudience)
        ));
        assert!(matches!(
            validator.validate(&create_test_token(
                &claims_with(Some(Audience::One("https://elsewhere.example.com".into()))),
                secret
            )),
            Err(JwtError::InvalidAudience)
        ));
        assert!(
            validator
                .validate(&create_test_token(
                    &claims_with(Some(Audience::One("https://mcp.example.com/mcp".into()))),
                    secret
                ))
                .is_ok()
        );
    }

    #[test]
    fn test_a_not_yet_valid_token_is_rejected() {
        // `validate_nbf` is off by default in jsonwebtoken, so a token that
        // does not become valid until next week was accepted today.
        let secret = b"secret";
        let validator = JwtValidator::hs256(secret).for_resource(&resource());

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let token = encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({
                "sub": "user-123",
                "aud": resource().as_str(),
                "exp": now + 7200,
                "nbf": now + 3600,
            }),
            &EncodingKey::from_secret(secret),
        )
        .unwrap();

        assert!(matches!(
            validator.validate(&token),
            Err(JwtError::ValidationFailed(_))
        ));
    }

    #[test]
    fn test_bearer_scheme_is_case_insensitive() {
        // RFC 7235 §2.1: "The scheme name is case-insensitive."
        for header in ["Bearer abc.def", "bearer abc.def", "BEARER abc.def"] {
            assert_eq!(JwtValidator::extract_token(header).unwrap(), "abc.def");
        }

        for header in ["Basic abc.def", "Bearer", "Bearer ", ""] {
            assert!(
                matches!(
                    JwtValidator::extract_token(header),
                    Err(JwtError::InvalidFormat)
                ),
                "{header:?} should not yield a token"
            );
        }
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
