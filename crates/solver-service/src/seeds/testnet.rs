//! Testnet seed configuration.
//!
//! Contains hardcoded configuration for testnet networks:
//! - Optimism Sepolia (chain ID: 11155420)
//! - Base Sepolia (chain ID: 84532)
//! - Tron Shasta (chain ID: 2494104990)
//! - HyperEVM Testnet (chain ID: 998)

use super::types::{NetworkSeed, SeedConfig, COMMON_DEFAULTS};
use alloy_primitives::address;

/// Testnet seed configuration.
pub static TESTNET_SEED: SeedConfig = SeedConfig {
	networks: &[OPTIMISM_SEPOLIA, BASE_SEPOLIA, TRON_SHASTA, HYPEREVM_TESTNET],
	defaults: COMMON_DEFAULTS,
};

/// Optimism Sepolia network seed (chain ID: 11155420).
pub static OPTIMISM_SEPOLIA: NetworkSeed = NetworkSeed {
	chain_id: 11155420,
	name: "optimism-sepolia",
	default_rpc_urls: &["https://sepolia.optimism.io"],
	// Permit2/EIP-3009 escrow input settler
	input_settler: address!("1F0b9d6984f5f9187Db70469085f7935453b815F"),
	// Output settler
	output_settler: address!("923aa6CC898540092616f97cA770FFb8080354Fc"),
	// Compact/resource-lock input settler
	input_settler_compact: address!("086e28545bB8494C6041225922A705877AE5362A"),
	// The Compact contract
	the_compact: address!("00000000000000171ede64904551eedf3c6c9788"),
	// Allocator for compact flow
	allocator: address!("565466528d126141ddb5c7d558803f79d9b66a6d"),
	// Hyperlane contracts
	hyperlane_mailbox: address!("6966b0E55883d49BFB24539356a2f8A673E02039"),
	hyperlane_igp: address!("28B02B97a850872C4D33C3E024fab6499ad96564"),
	hyperlane_oracle: address!("c8604e4aBC757C5C1990BAe679A7b219808EDc9c"),
};

/// Base Sepolia network seed (chain ID: 84532).
pub static BASE_SEPOLIA: NetworkSeed = NetworkSeed {
	chain_id: 84532,
	name: "base-sepolia",
	default_rpc_urls: &["https://sepolia.base.org"],
	// Permit2/EIP-3009 escrow input settler
	input_settler: address!("6C0428cc521CC418A8842d46d413F5F96775c67B"),
	// Output settler
	output_settler: address!("C450A11afb68731833BE13225A88ecdad7D7Ed52"),
	// Compact/resource-lock input settler
	input_settler_compact: address!("a7B995442F909849F96B5ED07ff7f58E57a41fc9"),
	// The Compact contract
	the_compact: address!("00000000000000171ede64904551eedf3c6c9788"),
	// Allocator for compact flow
	allocator: address!("04bb6e565f0067e0411528e2d3a55a712d9a8b32"),
	// Hyperlane contracts
	hyperlane_mailbox: address!("6966b0E55883d49BFB24539356a2f8A673E02039"),
	hyperlane_igp: address!("28B02B97a850872C4D33C3E024fab6499ad96564"),
	hyperlane_oracle: address!("3f1ED0CEf17842C8cD47CcbaDf534eaB6BEf5d46"),
};

/// Tron Shasta testnet seed (chain ID: 2494104990).
pub static TRON_SHASTA: NetworkSeed = NetworkSeed {
	chain_id: 2494104990,
	name: "tron-shasta",
	default_rpc_urls: &["https://api.shasta.trongrid.io/jsonrpc"],
	// Permit2/EIP-3009 escrow input settler (N/A on Tron for now, reuse input settler)
	input_settler: address!("16f1c40c13634f4a97d8004453ed86b7189583bc"),
	// Output settler
	output_settler: address!("3274dd713c02aaf9cbbba913733177d9a40c0a03"),
	// Compact/resource-lock is not deployed on Shasta in this setup
	input_settler_compact: address!("16f1c40c13634f4a97d8004453ed86b7189583bc"),
	// The Compact contract (placeholder until Tron-native equivalent exists)
	the_compact: address!("0000000000000000000000000000000000000000"),
	// Allocator placeholder
	allocator: address!("0000000000000000000000000000000000000000"),
	// Hyperlane contracts
	hyperlane_mailbox: address!("B50E9F9CB52543D754768dD67f1e6148ABd78B0B"),
	hyperlane_igp: address!("0000000000000000000000000000000000000000"),
	hyperlane_oracle: address!("2136957a89d3552e260b8dd4134c2c66d37db044"),
};

/// HyperEVM testnet seed (chain ID: 998).
pub static HYPEREVM_TESTNET: NetworkSeed = NetworkSeed {
	chain_id: 998,
	name: "hyperevm-testnet",
	default_rpc_urls: &["https://rpc.hyperliquid-testnet.xyz/evm"],
	// Input settler is currently not used on this route
	input_settler: address!("0000000000000000000000000000000000000000"),
	// Output settler
	output_settler: address!("e241df14e36c639610e6f564a74b0bc9350dbc60"),
	// Compact/resource-lock placeholders
	input_settler_compact: address!("0000000000000000000000000000000000000000"),
	the_compact: address!("0000000000000000000000000000000000000000"),
	allocator: address!("0000000000000000000000000000000000000000"),
	// Hyperlane contracts
	hyperlane_mailbox: address!("589C201a07c26b4725A4A829d772f24423da480B"),
	hyperlane_igp: address!("0000000000000000000000000000000000000000"),
	hyperlane_oracle: address!("bAe9c270727bD11Bf1b7F358526B1bf6c463ff28"),
};

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_testnet_seed_networks() {
		assert_eq!(TESTNET_SEED.networks.len(), 4);
	}

	#[test]
	fn test_optimism_sepolia_chain_id() {
		assert_eq!(OPTIMISM_SEPOLIA.chain_id, 11155420);
		assert_eq!(OPTIMISM_SEPOLIA.name, "optimism-sepolia");
	}

	#[test]
	fn test_base_sepolia_chain_id() {
		assert_eq!(BASE_SEPOLIA.chain_id, 84532);
		assert_eq!(BASE_SEPOLIA.name, "base-sepolia");
	}

	#[test]
	fn test_get_network() {
		let optimism = TESTNET_SEED.get_network(11155420);
		assert!(optimism.is_some());
		assert_eq!(optimism.unwrap().name, "optimism-sepolia");

		let base = TESTNET_SEED.get_network(84532);
		assert!(base.is_some());
		assert_eq!(base.unwrap().name, "base-sepolia");

		let unknown = TESTNET_SEED.get_network(1);
		assert!(unknown.is_none());
	}

	#[test]
	fn test_supported_chain_ids() {
		let chain_ids = TESTNET_SEED.supported_chain_ids();
		assert_eq!(chain_ids.len(), 4);
		assert!(chain_ids.contains(&11155420));
		assert!(chain_ids.contains(&84532));
		assert!(chain_ids.contains(&2494104990));
		assert!(chain_ids.contains(&998));
	}

	#[test]
	fn test_contract_addresses_not_zero() {
		// Verify all addresses are not zero address
		let zero = address!("0000000000000000000000000000000000000000");

		// Optimism Sepolia
		assert_ne!(OPTIMISM_SEPOLIA.input_settler, zero);
		assert_ne!(OPTIMISM_SEPOLIA.output_settler, zero);
		assert_ne!(OPTIMISM_SEPOLIA.input_settler_compact, zero);
		assert_ne!(OPTIMISM_SEPOLIA.the_compact, zero);
		assert_ne!(OPTIMISM_SEPOLIA.allocator, zero);
		assert_ne!(OPTIMISM_SEPOLIA.hyperlane_mailbox, zero);
		assert_ne!(OPTIMISM_SEPOLIA.hyperlane_igp, zero);
		assert_ne!(OPTIMISM_SEPOLIA.hyperlane_oracle, zero);

		// Base Sepolia
		assert_ne!(BASE_SEPOLIA.input_settler, zero);
		assert_ne!(BASE_SEPOLIA.output_settler, zero);
		assert_ne!(BASE_SEPOLIA.input_settler_compact, zero);
		assert_ne!(BASE_SEPOLIA.the_compact, zero);
		assert_ne!(BASE_SEPOLIA.allocator, zero);
		assert_ne!(BASE_SEPOLIA.hyperlane_mailbox, zero);
		assert_ne!(BASE_SEPOLIA.hyperlane_igp, zero);
		assert_ne!(BASE_SEPOLIA.hyperlane_oracle, zero);
	}

	#[test]
	fn test_the_compact_same_across_networks() {
		// The Compact should be the same address across all networks
		assert_eq!(OPTIMISM_SEPOLIA.the_compact, BASE_SEPOLIA.the_compact);
	}

	#[test]
	fn test_hyperlane_mailbox_same_on_testnet() {
		// On testnets, mailbox addresses may be the same
		assert_eq!(
			OPTIMISM_SEPOLIA.hyperlane_mailbox,
			BASE_SEPOLIA.hyperlane_mailbox
		);
	}
}
