//! Small security-sensitive helpers.

/// Constant-time byte-slice equality, so secret comparison (auth headers) does
/// not leak the position of the first mismatch via timing. Length is allowed to
/// differ observably — that is standard for HMAC/secret checks.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
