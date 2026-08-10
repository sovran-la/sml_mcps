//! Pagination utilities for MCP list operations
//!
//! Provides cursor-based pagination for tools, resources, and prompts.
//! Cursors are base64-encoded offsets for simplicity and opacity.

use crate::types::{McpError, Result};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

/// Default page size for list operations
pub const DEFAULT_PAGE_SIZE: usize = 50;

/// Pagination state derived from a cursor
#[derive(Debug, Clone)]
pub struct PageState {
    /// Offset to start from (0-indexed)
    pub offset: usize,
    /// Number of items to return
    pub limit: usize,
}

impl PageState {
    /// Create a new page state from an optional cursor and page size.
    ///
    /// A cursor that does not decode is `-32602 Invalid params`, not page one.
    /// Pagination §Error Handling makes that a SHOULD for the list operations
    /// and the tasks page makes it a MUST for `tasks/list`; either way,
    /// silently restarting is worse than it sounds - a client that mangles a
    /// cursor gets an infinite loop over page one instead of a diagnosable
    /// failure.
    pub fn from_cursor(cursor: Option<&str>, page_size: usize) -> Result<Self> {
        let offset = match cursor {
            Some(cursor) => decode_cursor(cursor)
                .ok_or_else(|| McpError::InvalidParams(format!("Invalid cursor: {cursor}")))?,
            None => 0,
        };

        Ok(Self::at(offset, page_size))
    }

    /// A page state at an explicit offset.
    ///
    /// `page_size` is clamped to at least one: a zero page returns nothing and
    /// hands back a cursor to the same offset, which is an infinite loop for
    /// any client that follows it.
    pub fn at(offset: usize, page_size: usize) -> Self {
        Self {
            offset,
            limit: page_size.max(1),
        }
    }

    /// Calculate the next cursor if there are more items
    ///
    /// Returns Some(cursor) if `total_items > offset + returned_count`
    pub fn next_cursor(&self, total_items: usize, returned_count: usize) -> Option<String> {
        let next_offset = self.offset + returned_count;
        if next_offset < total_items {
            Some(encode_cursor(next_offset))
        } else {
            None
        }
    }
}

/// Encode an offset into an opaque cursor string
fn encode_cursor(offset: usize) -> String {
    URL_SAFE_NO_PAD.encode(offset.to_string())
}

/// A well-formed cursor for an arbitrary offset, so a test can ask for a page
/// that does not exist without hand-rolling the encoding.
#[cfg(test)]
pub(crate) fn encode_cursor_for_test(offset: usize) -> String {
    encode_cursor(offset)
}

/// Decode a cursor string back to an offset
fn decode_cursor(cursor: &str) -> Option<usize> {
    URL_SAFE_NO_PAD
        .decode(cursor)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .and_then(|s| s.parse().ok())
}

/// Apply pagination to a slice, returning the page and whether there are more items
pub fn paginate<T: Clone>(items: &[T], state: &PageState) -> (Vec<T>, Option<String>) {
    let total = items.len();

    // Handle offset beyond bounds
    if state.offset >= total {
        return (Vec::new(), None);
    }

    // Get the page slice
    let end = (state.offset + state.limit).min(total);
    let page: Vec<T> = items[state.offset..end].to_vec();
    let returned_count = page.len();

    // Calculate next cursor
    let next_cursor = state.next_cursor(total, returned_count);

    (page, next_cursor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_cursor() {
        let offset = 42usize;
        let cursor = encode_cursor(offset);
        let decoded = decode_cursor(&cursor);
        assert_eq!(decoded, Some(42));
    }

    #[test]
    fn test_decode_invalid_cursor() {
        assert_eq!(decode_cursor("not-valid-base64!!!"), None);
        assert_eq!(decode_cursor("aGVsbG8"), None); // "hello" - not a number
    }

    #[test]
    fn test_page_state_no_cursor() {
        let state = PageState::from_cursor(None, 10).unwrap();
        assert_eq!(state.offset, 0);
        assert_eq!(state.limit, 10);
    }

    #[test]
    fn test_page_state_with_cursor() {
        let cursor = encode_cursor(25);
        let state = PageState::from_cursor(Some(&cursor), 10).unwrap();
        assert_eq!(state.offset, 25);
        assert_eq!(state.limit, 10);
    }

    #[test]
    fn test_an_undecodable_cursor_is_invalid_params() {
        // Silently restarting at page one turns a mangled cursor into an
        // infinite loop over the first page instead of a diagnosable failure.
        let error = PageState::from_cursor(Some("!!!not-a-cursor!!!"), 10).unwrap_err();
        assert_eq!(error.to_jsonrpc_error().code, -32602);
        assert!(error.to_string().contains("cursor"), "{error}");

        // Decodable base64 that is not an offset is just as invalid.
        assert!(PageState::from_cursor(Some("aGVsbG8"), 10).is_err());
    }

    #[test]
    fn test_a_zero_page_size_cannot_loop_forever() {
        // limit 0 returns an empty page and a cursor to the same offset, so a
        // client following cursors never terminates.
        let state = PageState::from_cursor(None, 0).unwrap();
        assert_eq!(state.limit, 1);

        let items: Vec<i32> = (0..3).collect();
        let (page, next) = paginate(&items, &state);
        assert_eq!(page, [0]);
        assert_eq!(decode_cursor(&next.unwrap()), Some(1));
    }

    #[test]
    fn test_next_cursor_has_more() {
        let state = PageState {
            offset: 0,
            limit: 10,
        };
        let next = state.next_cursor(100, 10);
        assert!(next.is_some());

        // Verify the cursor decodes to 10
        let decoded = decode_cursor(&next.unwrap());
        assert_eq!(decoded, Some(10));
    }

    #[test]
    fn test_next_cursor_no_more() {
        let state = PageState {
            offset: 90,
            limit: 10,
        };
        let next = state.next_cursor(100, 10);
        assert!(next.is_none());
    }

    #[test]
    fn test_next_cursor_partial_page() {
        let state = PageState {
            offset: 95,
            limit: 10,
        };
        let next = state.next_cursor(100, 5); // Only 5 items returned
        assert!(next.is_none());
    }

    #[test]
    fn test_paginate_first_page() {
        let items: Vec<i32> = (0..100).collect();
        let state = PageState::from_cursor(None, 10).unwrap();
        let (page, next) = paginate(&items, &state);

        assert_eq!(page.len(), 10);
        assert_eq!(page[0], 0);
        assert_eq!(page[9], 9);
        assert!(next.is_some());
    }

    #[test]
    fn test_paginate_middle_page() {
        let items: Vec<i32> = (0..100).collect();
        let cursor = encode_cursor(50);
        let state = PageState::from_cursor(Some(&cursor), 10).unwrap();
        let (page, next) = paginate(&items, &state);

        assert_eq!(page.len(), 10);
        assert_eq!(page[0], 50);
        assert_eq!(page[9], 59);
        assert!(next.is_some());
    }

    #[test]
    fn test_paginate_last_page() {
        let items: Vec<i32> = (0..100).collect();
        let cursor = encode_cursor(95);
        let state = PageState::from_cursor(Some(&cursor), 10).unwrap();
        let (page, next) = paginate(&items, &state);

        assert_eq!(page.len(), 5); // Only 5 items left
        assert_eq!(page[0], 95);
        assert_eq!(page[4], 99);
        assert!(next.is_none());
    }

    #[test]
    fn test_paginate_beyond_bounds() {
        let items: Vec<i32> = (0..10).collect();
        let cursor = encode_cursor(100);
        let state = PageState::from_cursor(Some(&cursor), 10).unwrap();
        let (page, next) = paginate(&items, &state);

        assert!(page.is_empty());
        assert!(next.is_none());
    }

    #[test]
    fn test_paginate_empty_slice() {
        let items: Vec<i32> = vec![];
        let state = PageState::from_cursor(None, 10).unwrap();
        let (page, next) = paginate(&items, &state);

        assert!(page.is_empty());
        assert!(next.is_none());
    }

    #[test]
    fn test_paginate_smaller_than_page_size() {
        let items: Vec<i32> = (0..5).collect();
        let state = PageState::from_cursor(None, 10).unwrap();
        let (page, next) = paginate(&items, &state);

        assert_eq!(page.len(), 5);
        assert!(next.is_none());
    }
}
