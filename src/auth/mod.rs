//! JWT Authentication
//!
//! Validates JWT tokens and extracts claims for multi-tenancy.

mod jwt;
mod resource;

pub use jwt::{Audience, Claims, JwtError, JwtValidator};
pub use resource::{
    ProtectedResourceMetadata, ResourceUri, insufficient_scope_challenge, unauthorized_challenge,
};
