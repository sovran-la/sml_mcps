//! `Origin` header validation for the Streamable HTTP transport.
//!
//! The MCP specification requires servers to validate the `Origin` header to
//! defend against DNS rebinding, where a page on an attacker-controlled site
//! re-points a hostname at `127.0.0.1` and then talks to a local MCP server
//! using the victim's browser as a proxy:
//!
//! > Servers **MUST** validate the `Origin` header on all incoming connections
//! > to prevent DNS rebinding attacks. If the `Origin` header is present and
//! > invalid, servers **MUST** respond with HTTP 403 Forbidden.
//!
//! Note the exact wording: only a *present* `Origin` is checked. Non-browser
//! clients (curl, MCP CLI clients, the SDKs) do not send one, and rejecting
//! those would break every real deployment while adding no security - the
//! attack requires a browser, and browsers always send `Origin` on
//! cross-origin requests.

/// Which `Origin` header values a [`HttpServer`](crate::HttpServer) accepts.
///
/// The default is [`OriginPolicy::Loopback`], which is the safe choice for the
/// local-server case the spec calls out. Deployments that legitimately serve
/// browsers from a known web origin should use [`OriginPolicy::allowlist`].
///
/// # Example
///
/// ```
/// use sml_mcps::OriginPolicy;
///
/// // Default: only browser pages served from loopback may talk to us.
/// let policy = OriginPolicy::default();
/// assert!(policy.is_allowed("http://localhost:5173"));
/// assert!(!policy.is_allowed("https://evil.example.com"));
///
/// // Explicit allowlist for a known front-end origin.
/// let policy = OriginPolicy::allowlist(["https://app.example.com"]);
/// assert!(policy.is_allowed("https://app.example.com"));
/// assert!(!policy.is_allowed("http://localhost:3000"));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum OriginPolicy {
    /// Accept only origins whose host is a loopback address: `localhost`,
    /// anything under `.localhost`, `127.0.0.0/8`, or `::1`. Any port and
    /// either `http` or `https` is fine.
    ///
    /// This is the default, and is what protects a locally-bound server from
    /// DNS rebinding: the attacker's page is served from a public origin, so
    /// its `Origin` header never looks like loopback.
    #[default]
    Loopback,

    /// Accept only the listed origins.
    ///
    /// Entries are normalized (lowercased scheme and host, trailing slash
    /// stripped) and compared exactly, including port. `http://example.com`
    /// therefore does *not* match `http://example.com:8080`.
    Allowlist(Vec<String>),

    /// Accept every origin, including `null`.
    ///
    /// Only safe when the server is unreachable from a browser, or is behind a
    /// gateway that performs its own origin checks. Choosing this on a
    /// localhost-bound server reintroduces the DNS rebinding exposure the
    /// check exists to prevent.
    Any,
}

impl OriginPolicy {
    /// Build an allowlist policy from anything string-like.
    pub fn allowlist<I, S>(origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::Allowlist(
            origins
                .into_iter()
                .map(|o| normalize_origin(o.as_ref()).unwrap_or_else(|| o.as_ref().to_string()))
                .collect(),
        )
    }

    /// Is this `Origin` header value acceptable?
    ///
    /// Callers should only reach this with a header that was actually present;
    /// an absent `Origin` is not a rejection condition (see module docs).
    pub fn is_allowed(&self, origin: &str) -> bool {
        if matches!(self, OriginPolicy::Any) {
            return true;
        }

        // `null` is the opaque origin browsers send from sandboxed iframes,
        // `data:` URLs, and some `file://` contexts. It names no host, so it
        // can never be validated - treat it as hostile.
        let Some(normalized) = normalize_origin(origin) else {
            return false;
        };

        match self {
            OriginPolicy::Any => true,
            OriginPolicy::Allowlist(allowed) => allowed.contains(&normalized),
            OriginPolicy::Loopback => host_of(&normalized).is_some_and(is_loopback_host),
        }
    }
}

/// Normalize an `Origin` header into `scheme://host[:port]`, lowercasing the
/// scheme and host and dropping a single trailing slash.
///
/// Returns `None` for anything that isn't a well-formed serialized origin,
/// which includes the literal `null`.
fn normalize_origin(origin: &str) -> Option<String> {
    let origin = origin.trim();
    if origin.is_empty() || origin.eq_ignore_ascii_case("null") {
        return None;
    }

    let (scheme, rest) = origin.split_once("://")?;
    if scheme.is_empty() || !scheme.starts_with(|c: char| c.is_ascii_alphabetic()) {
        return None;
    }
    if !scheme
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.')
    {
        return None;
    }

    // A serialized origin has no path, query, or fragment. Tolerate exactly one
    // trailing slash, which some clients append.
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    if authority.is_empty()
        || authority.contains('/')
        || authority.contains('?')
        || authority.contains('#')
        || authority.contains('@')
    {
        return None;
    }

    // Split host from port, honoring bracketed IPv6 literals.
    let (host, port) = if let Some(close) = authority.rfind(']') {
        if !authority.starts_with('[') {
            return None;
        }
        let (host, tail) = authority.split_at(close + 1);
        match tail {
            "" => (host, None),
            _ => (host, Some(tail.strip_prefix(':')?)),
        }
    } else {
        match authority.split_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (authority, None),
        }
    };

    if host.is_empty() {
        return None;
    }
    if let Some(port) = port {
        if port.is_empty() || !port.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        if port.parse::<u16>().is_err() {
            return None;
        }
    }

    let mut normalized = String::with_capacity(origin.len());
    normalized.push_str(&scheme.to_ascii_lowercase());
    normalized.push_str("://");
    normalized.push_str(&host.to_ascii_lowercase());
    if let Some(port) = port {
        normalized.push(':');
        normalized.push_str(port);
    }
    Some(normalized)
}

/// Extract the host from an already-normalized origin, without brackets.
fn host_of(normalized: &str) -> Option<&str> {
    let (_, rest) = normalized.split_once("://")?;
    let host = if rest.starts_with('[') {
        let close = rest.find(']')?;
        &rest[1..close]
    } else {
        rest.split_once(':').map(|(h, _)| h).unwrap_or(rest)
    };
    Some(host)
}

/// Does this host name or literal refer to the loopback interface?
fn is_loopback_host(host: &str) -> bool {
    // RFC 6761 reserves `localhost` and anything under it for loopback.
    if host == "localhost" || host.ends_with(".localhost") {
        return true;
    }
    match host.parse::<std::net::IpAddr>() {
        Ok(addr) => addr.is_loopback(),
        // Bare IPv4 shorthand like `127.1` is not accepted by `IpAddr`, but
        // browsers normalize such hosts before building the Origin header, so
        // anything that reaches here and fails to parse is a real DNS name.
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_scheme_and_host_case() {
        assert_eq!(
            normalize_origin("HTTP://LocalHost:3000").as_deref(),
            Some("http://localhost:3000")
        );
    }

    #[test]
    fn normalizes_trailing_slash() {
        assert_eq!(
            normalize_origin("https://example.com/").as_deref(),
            Some("https://example.com")
        );
    }

    #[test]
    fn keeps_port_distinct() {
        assert_ne!(
            normalize_origin("http://example.com"),
            normalize_origin("http://example.com:80")
        );
    }

    #[test]
    fn normalizes_ipv6_literal() {
        assert_eq!(
            normalize_origin("http://[::1]:8080").as_deref(),
            Some("http://[::1]:8080")
        );
        assert_eq!(
            normalize_origin("http://[::1]").as_deref(),
            Some("http://[::1]")
        );
    }

    #[test]
    fn rejects_malformed_origins() {
        for bad in [
            "",
            "   ",
            "null",
            "NULL",
            "example.com",
            "://example.com",
            "1http://example.com",
            "http://",
            "http:///path",
            "http://example.com/path",
            "http://example.com?q=1",
            "http://example.com#frag",
            "http://user@example.com",
            "http://example.com:",
            "http://example.com:abc",
            "http://example.com:99999",
            "http://[::1",
            "http://[::1]x80",
        ] {
            assert!(
                normalize_origin(bad).is_none(),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn loopback_policy_accepts_loopback_hosts() {
        let policy = OriginPolicy::Loopback;
        for good in [
            "http://localhost",
            "http://localhost:3000",
            "https://localhost:8443",
            "http://LOCALHOST:3000",
            "http://app.localhost:5173",
            "http://127.0.0.1",
            "http://127.0.0.1:8080",
            "http://127.1.2.3:9999",
            "http://[::1]:8080",
        ] {
            assert!(policy.is_allowed(good), "expected {good:?} to be allowed");
        }
    }

    #[test]
    fn loopback_policy_rejects_remote_hosts() {
        let policy = OriginPolicy::Loopback;
        for bad in [
            "https://evil.example.com",
            "http://example.com:3000",
            // The classic DNS rebinding shape: a public name that currently
            // resolves to loopback. The *name* is what we check, so it fails.
            "http://localhost.evil.example.com",
            "http://notlocalhost",
            "http://192.168.1.5:3000",
            "http://[2001:db8::1]",
            "null",
        ] {
            assert!(!policy.is_allowed(bad), "expected {bad:?} to be rejected");
        }
    }

    #[test]
    fn allowlist_policy_matches_exactly() {
        let policy = OriginPolicy::allowlist(["https://app.example.com", "http://localhost:5173"]);

        assert!(policy.is_allowed("https://app.example.com"));
        assert!(policy.is_allowed("https://app.example.com/"));
        assert!(policy.is_allowed("HTTPS://APP.EXAMPLE.COM"));
        assert!(policy.is_allowed("http://localhost:5173"));

        assert!(!policy.is_allowed("http://app.example.com")); // scheme differs
        assert!(!policy.is_allowed("https://app.example.com:443")); // port differs
        assert!(!policy.is_allowed("https://evil.example.com"));
        assert!(!policy.is_allowed("http://localhost:3000"));
        assert!(!policy.is_allowed("null"));
    }

    #[test]
    fn allowlist_normalizes_configured_entries() {
        let policy = OriginPolicy::allowlist(["HTTPS://App.Example.COM/"]);
        assert_eq!(
            policy,
            OriginPolicy::Allowlist(vec!["https://app.example.com".to_string()])
        );
        assert!(policy.is_allowed("https://app.example.com"));
    }

    #[test]
    fn allowlist_keeps_unparseable_entries_without_panicking() {
        // Garbage in the allowlist must not match anything, and must not panic.
        let policy = OriginPolicy::allowlist(["not-an-origin"]);
        assert!(!policy.is_allowed("not-an-origin"));
        assert!(!policy.is_allowed("http://not-an-origin"));
    }

    #[test]
    fn any_policy_accepts_everything() {
        let policy = OriginPolicy::Any;
        assert!(policy.is_allowed("https://evil.example.com"));
        assert!(policy.is_allowed("null"));
        assert!(policy.is_allowed(""));
    }

    #[test]
    fn default_policy_is_loopback() {
        assert_eq!(OriginPolicy::default(), OriginPolicy::Loopback);
    }

    #[test]
    fn host_of_extracts_host() {
        assert_eq!(host_of("http://localhost:3000"), Some("localhost"));
        assert_eq!(host_of("http://[::1]:8080"), Some("::1"));
        assert_eq!(host_of("https://example.com"), Some("example.com"));
        assert_eq!(host_of("garbage"), None);
    }

    #[test]
    fn is_loopback_host_covers_ipv4_and_ipv6() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("127.255.255.254"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("foo.localhost"));
        assert!(!is_loopback_host("128.0.0.1"));
        assert!(!is_loopback_host("localhost.evil.com"));
        assert!(!is_loopback_host("::2"));
    }
}
