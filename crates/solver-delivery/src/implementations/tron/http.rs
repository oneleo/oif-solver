//! Tron HTTP delivery scaffold implementation.
//!
//! This module registers a Tron delivery backend (`tron_http`) without changing
//! existing EVM behavior. Write/read transaction paths are intentionally left as
//! explicit NotImplemented responses to avoid false-positive execution.

use crate::{DeliveryError, DeliveryInterface, TransactionTrackingWithConfig};
use alloy_primitives::Bytes;
use async_trait::async_trait;
use solver_account::AccountSigner;
use solver_types::{
	ConfigSchema, Field, FieldType, Log, LogFilter, NetworksConfig, Schema, Transaction,
	TransactionHash, TransactionReceipt,
};
use std::collections::HashMap;

/// Scaffold implementation for Tron delivery.
///
/// This type only validates config and exposes explicit NotImplemented
/// responses for runtime methods during Step 1 integration.
pub struct TronHttpDelivery {
	network_ids: Vec<u64>,
}

impl TronHttpDelivery {
	pub fn new(network_ids: Vec<u64>, networks: &NetworksConfig) -> Result<Self, DeliveryError> {
		if network_ids.is_empty() {
			return Err(DeliveryError::Network(
				"tron_http requires at least one network_id".to_string(),
			));
		}

		for network_id in &network_ids {
			let network = networks.get(network_id).ok_or_else(|| {
				DeliveryError::Network(format!("Network {network_id} not found in configuration"))
			})?;
			if network.get_http_url().is_none() {
				return Err(DeliveryError::Network(format!(
					"No HTTP RPC URL configured for network {network_id}"
				)));
			}
		}

		Ok(Self { network_ids })
	}

	fn not_implemented(operation: &str) -> DeliveryError {
		DeliveryError::Network(format!("NotImplemented: tron_http {operation} is not implemented"))
	}
}

/// Configuration schema for Tron HTTP delivery.
pub struct TronHttpDeliverySchema;

impl TronHttpDeliverySchema {
	pub fn validate_config(
		config: &serde_json::Value,
	) -> Result<(), solver_types::ValidationError> {
		let instance = Self;
		instance.validate(config)
	}
}

impl ConfigSchema for TronHttpDeliverySchema {
	fn validate(&self, config: &serde_json::Value) -> Result<(), solver_types::ValidationError> {
		let schema = Schema::new(
			vec![Field::new(
				"network_ids",
				FieldType::Array(Box::new(FieldType::Integer {
					min: Some(1),
					max: None,
				})),
			)
			.with_validator(|value| {
				if let Some(arr) = value.as_array() {
					if arr.is_empty() {
						return Err("network_ids cannot be empty".to_string());
					}
					Ok(())
				} else {
					Err("network_ids must be an array".to_string())
				}
			})],
			vec![],
		);

		schema.validate(config)
	}
}

#[async_trait]
impl DeliveryInterface for TronHttpDelivery {
	fn config_schema(&self) -> Box<dyn ConfigSchema> {
		Box::new(TronHttpDeliverySchema)
	}

	async fn submit(
		&self,
		_tx: Transaction,
		_tracking: Option<TransactionTrackingWithConfig>,
	) -> Result<TransactionHash, DeliveryError> {
		Err(Self::not_implemented("submit"))
	}

	async fn get_receipt(
		&self,
		_hash: &TransactionHash,
		_chain_id: u64,
	) -> Result<TransactionReceipt, DeliveryError> {
		Err(Self::not_implemented("get_receipt"))
	}

	async fn get_gas_price(&self, _chain_id: u64) -> Result<String, DeliveryError> {
		Err(Self::not_implemented("get_gas_price"))
	}

	async fn get_balance(
		&self,
		_address: &str,
		_token: Option<&str>,
		_chain_id: u64,
	) -> Result<String, DeliveryError> {
		Err(Self::not_implemented("get_balance"))
	}

	async fn get_allowance(
		&self,
		_owner: &str,
		_spender: &str,
		_token_address: &str,
		_chain_id: u64,
	) -> Result<String, DeliveryError> {
		Err(Self::not_implemented("get_allowance"))
	}

	async fn get_nonce(&self, _address: &str, _chain_id: u64) -> Result<u64, DeliveryError> {
		Err(Self::not_implemented("get_nonce"))
	}

	async fn get_block_number(&self, _chain_id: u64) -> Result<u64, DeliveryError> {
		Err(Self::not_implemented("get_block_number"))
	}

	async fn estimate_gas(&self, _tx: Transaction) -> Result<u64, DeliveryError> {
		Err(Self::not_implemented("estimate_gas"))
	}

	async fn eth_call(&self, _tx: Transaction) -> Result<Bytes, DeliveryError> {
		Err(Self::not_implemented("eth_call"))
	}

	async fn tx_exists(
		&self,
		_hash: &TransactionHash,
		_chain_id: u64,
	) -> Result<bool, DeliveryError> {
		Err(Self::not_implemented("tx_exists"))
	}

	async fn get_logs(&self, _chain_id: u64, _filter: LogFilter) -> Result<Vec<Log>, DeliveryError> {
		Err(Self::not_implemented("get_logs"))
	}
}

/// Factory function for creating tron_http delivery implementation.
pub fn create_tron_http_delivery(
	config: &serde_json::Value,
	networks: &NetworksConfig,
	_default_signer: &AccountSigner,
	_network_signers: &HashMap<u64, AccountSigner>,
) -> Result<Box<dyn DeliveryInterface>, DeliveryError> {
	TronHttpDeliverySchema::validate_config(config)
		.map_err(|e| DeliveryError::Network(format!("Invalid configuration: {e}")))?;

	let network_ids = config
		.get("network_ids")
		.and_then(|v| v.as_array())
		.map(|arr| {
			arr.iter()
				.filter_map(|v| v.as_i64().map(|i| i as u64))
				.collect::<Vec<_>>()
		})
		.ok_or_else(|| DeliveryError::Network("network_ids is required".to_string()))?;

	let delivery = TronHttpDelivery::new(network_ids, networks)?;
	Ok(Box::new(delivery))
}

/// Registry for tron_http delivery implementation.
pub struct Registry;

impl solver_types::ImplementationRegistry for Registry {
	const NAME: &'static str = "tron_http";
	type Factory = crate::DeliveryFactory;

	fn factory() -> Self::Factory {
		create_tron_http_delivery
	}
}

impl crate::DeliveryRegistry for Registry {}

#[cfg(test)]
mod tests {
	use super::*;
	use solver_types::utils::tests::builders::{NetworkConfigBuilder, NetworksConfigBuilder};

	fn create_test_networks() -> NetworksConfig {
		NetworksConfigBuilder::new()
			.add_network(2494104990, NetworkConfigBuilder::new().build())
			.build()
	}

	#[test]
	fn test_registry_name() {
		assert_eq!(
			<Registry as solver_types::ImplementationRegistry>::NAME,
			"tron_http"
		);
	}

	#[tokio::test]
	async fn test_submit_returns_explicit_not_implemented() {
		let networks = create_test_networks();
		let delivery = TronHttpDelivery::new(vec![2494104990], &networks).unwrap();
		let tx = Transaction {
			to: None,
			data: vec![],
			value: alloy_primitives::U256::ZERO,
			chain_id: 2494104990,
			nonce: None,
			gas_limit: None,
			gas_price: None,
			max_fee_per_gas: None,
			max_priority_fee_per_gas: None,
		};
		let err = delivery.submit(tx, None).await.unwrap_err();
		assert!(err.to_string().contains("NotImplemented: tron_http submit"));
	}

	#[tokio::test]
	async fn test_get_receipt_returns_explicit_not_implemented() {
		let networks = create_test_networks();
		let delivery = TronHttpDelivery::new(vec![2494104990], &networks).unwrap();
		let err = delivery
			.get_receipt(&TransactionHash(vec![0u8; 32]), 2494104990)
			.await
			.unwrap_err();
		assert!(err
			.to_string()
			.contains("NotImplemented: tron_http get_receipt"));
	}

	#[tokio::test]
	async fn test_get_logs_returns_explicit_not_implemented() {
		let networks = create_test_networks();
		let delivery = TronHttpDelivery::new(vec![2494104990], &networks).unwrap();
		let filter = LogFilter::new(
			solver_types::Address(vec![0u8; 20]),
			0,
			None,
			vec![None, None, None, None],
		);
		let err = delivery.get_logs(2494104990, filter).await.unwrap_err();
		assert!(err.to_string().contains("NotImplemented: tron_http get_logs"));
	}
}
