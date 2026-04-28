//! Tron address normalization and conversion helpers.
//!
//! Internal canonical format in solver-types remains 20-byte EVM-style address bytes.
//! This module handles conversions between:
//! - Base58Check (`T...`)
//! - Tron hex41 (`41...`, with optional `0x`)
//! - Canonical 20-byte bytes / `0x` hex

use crate::Address;

const TRON_PREFIX_BYTE: u8 = 0x41;

/// Returns true if the input looks like a Tron Base58Check address.
pub fn looks_like_tron_base58(value: &str) -> bool {
	value.starts_with('T') && value.len() >= 30 && value.len() <= 40
}

/// Returns true if the input looks like Tron hex41 format.
pub fn looks_like_tron_hex41(value: &str) -> bool {
	let clean = value.trim_start_matches("0x").trim_start_matches("0X");
	clean.len() == 42 && clean.starts_with("41")
}

/// Convert a Tron Base58Check address to canonical 20-byte address bytes.
pub fn tron_base58_to_evm20_bytes(value: &str) -> Result<[u8; 20], String> {
	let payload = bs58::decode(value)
		.with_check(None)
		.into_vec()
		.map_err(|e| format!("Invalid Tron Base58Check address: {e}"))?;
	parse_tron_payload_to_20(&payload)
}

/// Convert a Tron hex41 address to canonical 20-byte address bytes.
pub fn tron_hex41_to_evm20_bytes(value: &str) -> Result<[u8; 20], String> {
	let clean = value.trim_start_matches("0x").trim_start_matches("0X");
	if clean.len() != 42 {
		return Err(format!(
			"Invalid Tron hex41 length: expected 42 hex chars, got {}",
			clean.len()
		));
	}
	let payload = hex::decode(clean).map_err(|e| format!("Invalid Tron hex41: {e}"))?;
	parse_tron_payload_to_20(&payload)
}

/// Convert canonical 20-byte address bytes into Tron hex41 (lowercase, no 0x).
pub fn evm20_bytes_to_tron_hex41(value: &[u8]) -> Result<String, String> {
	let bytes20: [u8; 20] = value
		.try_into()
		.map_err(|_| format!("Expected 20-byte address, got {}", value.len()))?;
	let mut payload = [0u8; 21];
	payload[0] = TRON_PREFIX_BYTE;
	payload[1..].copy_from_slice(&bytes20);
	Ok(hex::encode(payload))
}

/// Convert canonical 20-byte address bytes into Tron Base58Check.
pub fn evm20_bytes_to_tron_base58(value: &[u8]) -> Result<String, String> {
	let bytes20: [u8; 20] = value
		.try_into()
		.map_err(|_| format!("Expected 20-byte address, got {}", value.len()))?;
	let mut payload = [0u8; 21];
	payload[0] = TRON_PREFIX_BYTE;
	payload[1..].copy_from_slice(&bytes20);
	Ok(bs58::encode(payload).with_check().into_string())
}

/// Parse any supported Tron address representation into canonical 20-byte `Address`.
pub fn parse_tron_address(value: &str) -> Result<Address, String> {
	if looks_like_tron_base58(value) {
		return tron_base58_to_evm20_bytes(value).map(|v| Address(v.to_vec()));
	}
	if looks_like_tron_hex41(value) {
		return tron_hex41_to_evm20_bytes(value).map(|v| Address(v.to_vec()));
	}
	Err("Unsupported Tron address format".to_string())
}

/// Best-effort readable formatting for logs.
///
/// Returns: `0x... (tron_base58: T...)` when conversion succeeds.
pub fn format_address_for_log(address: &Address) -> String {
	let evm_hex = format!("0x{}", hex::encode(&address.0));
	match evm20_bytes_to_tron_base58(&address.0) {
		Ok(base58) => format!("{evm_hex} (tron_base58: {base58})"),
		Err(_) => evm_hex,
	}
}

fn parse_tron_payload_to_20(payload: &[u8]) -> Result<[u8; 20], String> {
	if payload.len() != 21 {
		return Err(format!(
			"Invalid Tron payload length: expected 21 bytes, got {}",
			payload.len()
		));
	}
	if payload[0] != TRON_PREFIX_BYTE {
		return Err(format!(
			"Invalid Tron payload prefix: expected 0x41, got 0x{:02x}",
			payload[0]
		));
	}
	let mut out = [0u8; 20];
	out.copy_from_slice(&payload[1..]);
	Ok(out)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_base58_to_evm20() {
		let out = tron_base58_to_evm20_bytes("TG3XXyExBkPp9nzdajDZsozEu4BkaSJozs").unwrap();
		assert_eq!(
			hex::encode(out),
			"42a1e39aefa49290f2b3f9ed688d7cecf86cd6e0"
		);
	}

	#[test]
	fn test_hex41_to_evm20() {
		let out = tron_hex41_to_evm20_bytes("4142A1E39AEFA49290F2B3F9ED688D7CECF86CD6E0").unwrap();
		assert_eq!(
			hex::encode(out),
			"42a1e39aefa49290f2b3f9ed688d7cecf86cd6e0"
		);
	}

	#[test]
	fn test_evm20_to_base58() {
		let base58 = evm20_bytes_to_tron_base58(
			&hex::decode("42a1e39aefa49290f2b3f9ed688d7cecf86cd6e0").unwrap(),
		)
		.unwrap();
		assert_eq!(base58, "TG3XXyExBkPp9nzdajDZsozEu4BkaSJozs");
	}

	#[test]
	fn test_parse_tron_address() {
		let a = parse_tron_address("TG3XXyExBkPp9nzdajDZsozEu4BkaSJozs").unwrap();
		let b = parse_tron_address("0x4142A1E39AEFA49290F2B3F9ED688D7CECF86CD6E0").unwrap();
		assert_eq!(a, b);
	}
}
