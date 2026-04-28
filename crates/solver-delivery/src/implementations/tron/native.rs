//! Tron native delivery implementation.
//!
//! Step 2 focuses on read-only JSON-RPC adapters with:
//! - multi-RPC failover per chain
//! - schema-driven retry policy
//! - explicit Tron RPC error labeling
//! - normalized conversion to solver types

use crate::{DeliveryError, DeliveryInterface, TransactionTrackingWithConfig};
use alloy_primitives::{Bytes, U256};
use async_trait::async_trait;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use solver_account::AccountSigner;
use solver_types::{
	Address, ConfigSchema, Field, FieldType, H256, Log, LogFilter, NetworksConfig, Schema,
	Transaction, TransactionHash, TransactionReceipt,
};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone)]
struct RetryPolicy {
	max_retries: u32,
	initial_backoff_ms: u64,
	max_backoff_ms: u64,
	request_timeout_ms: u64,
}

impl Default for RetryPolicy {
	fn default() -> Self {
		Self {
			max_retries: 2,
			initial_backoff_ms: 300,
			max_backoff_ms: 3_000,
			request_timeout_ms: 8_000,
		}
	}
}

impl RetryPolicy {
	fn from_config(config: &Value) -> Result<Self, DeliveryError> {
		let get_u64 = |key: &str, default: u64| {
			config
				.get(key)
				.and_then(Value::as_u64)
				.unwrap_or(default)
		};

		let policy = Self {
			max_retries: get_u64("max_retries", 2) as u32,
			initial_backoff_ms: get_u64("initial_backoff_ms", 300),
			max_backoff_ms: get_u64("max_backoff_ms", 3_000),
			request_timeout_ms: get_u64("request_timeout_ms", 8_000),
		};

		if policy.initial_backoff_ms == 0 {
			return Err(DeliveryError::Network(
				"Tron RPC error: initial_backoff_ms must be > 0".to_string(),
			));
		}
		if policy.max_backoff_ms < policy.initial_backoff_ms {
			return Err(DeliveryError::Network(
				"Tron RPC error: max_backoff_ms must be >= initial_backoff_ms".to_string(),
			));
		}
		if policy.request_timeout_ms == 0 {
			return Err(DeliveryError::Network(
				"Tron RPC error: request_timeout_ms must be > 0".to_string(),
			));
		}

		Ok(policy)
	}
}

pub struct TronNativeDelivery {
	endpoints_by_chain: HashMap<u64, Vec<String>>,
	client: Client,
	retry_policy: RetryPolicy,
}

impl TronNativeDelivery {
	async fn new(
		network_ids: Vec<u64>,
		networks: &NetworksConfig,
		retry_policy: RetryPolicy,
	) -> Result<Self, DeliveryError> {
		if network_ids.is_empty() {
			return Err(DeliveryError::Network(
				"Tron RPC error: tron_native requires at least one network_id".to_string(),
			));
		}

		let mut endpoints_by_chain: HashMap<u64, Vec<String>> = HashMap::new();
		for network_id in network_ids {
			let network = networks.get(&network_id).ok_or_else(|| {
				DeliveryError::Network(format!(
					"Tron RPC error: network {network_id} not found in configuration"
				))
			})?;
			let urls = network
				.get_all_http_urls()
				.into_iter()
				.map(ToOwned::to_owned)
				.collect::<Vec<_>>();
			if urls.is_empty() {
				return Err(DeliveryError::Network(format!(
					"Tron RPC error: no HTTP RPC URL configured for network {network_id}"
				)));
			}
			endpoints_by_chain.insert(network_id, urls);
		}

		let client = Client::builder()
			.timeout(Duration::from_millis(retry_policy.request_timeout_ms))
			.build()
			.map_err(|e| DeliveryError::Network(format!("Tron RPC error: {e}")))?;

		let delivery = Self {
			endpoints_by_chain,
			client,
			retry_policy,
		};

		// Validate endpoint liveness and network identity using eth_chainId.
		let chain_ids = delivery.endpoints_by_chain.keys().copied().collect::<Vec<_>>();
		for chain_id in chain_ids {
			let reported = delivery.get_chain_id(chain_id).await?;
			if reported != chain_id {
				return Err(DeliveryError::Network(format!(
					"Tron RPC error: chain_id mismatch for configured chain {chain_id}, endpoint reported {reported}"
				)));
			}
		}

		Ok(delivery)
	}

	fn not_implemented(operation: &str) -> DeliveryError {
		DeliveryError::Network(format!(
			"NotImplemented: tron_native {operation} is not implemented"
		))
	}

	fn get_endpoints(&self, chain_id: u64) -> Result<&[String], DeliveryError> {
		self.endpoints_by_chain
			.get(&chain_id)
			.map(Vec::as_slice)
			.ok_or_else(|| {
				DeliveryError::Network(format!(
					"Tron RPC error: no configured endpoints for chain {chain_id}"
				))
			})
	}

	async fn rpc_call<T: DeserializeOwned>(
		&self,
		chain_id: u64,
		method: &str,
		params: Value,
	) -> Result<T, DeliveryError> {
		let endpoints = self.get_endpoints(chain_id)?;
		let mut failures: Vec<String> = Vec::new();

		for endpoint in endpoints {
			let mut backoff_ms = self.retry_policy.initial_backoff_ms;
			for attempt in 0..=self.retry_policy.max_retries {
				match self.rpc_call_single::<T>(endpoint, method, params.clone()).await {
					Ok(result) => return Ok(result),
					Err(AttemptError {
						message,
						retriable,
					}) => {
						let failure = format!(
							"endpoint={endpoint} method={method} attempt={attempt}: {message}"
						);
						if !retriable || attempt == self.retry_policy.max_retries {
							failures.push(failure);
							break;
						}

						tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
						backoff_ms = (backoff_ms.saturating_mul(2)).min(self.retry_policy.max_backoff_ms);
					},
				}
			}
		}

		Err(DeliveryError::Network(format!(
			"Tron RPC error: all endpoints failed on chain {chain_id} for {method}: {}",
			failures.join(" | ")
		)))
	}

	async fn rpc_call_single<T: DeserializeOwned>(
		&self,
		endpoint: &str,
		method: &str,
		params: Value,
	) -> Result<T, AttemptError> {
		let body = json!({
			"jsonrpc": "2.0",
			"id": 1,
			"method": method,
			"params": params,
		});

		let response = self
			.client
			.post(endpoint)
			.json(&body)
			.send()
			.await
			.map_err(|e| AttemptError::retriable(format!("request failed: {e}")))?;

		let status = response.status();
		if !status.is_success() {
			return Err(AttemptError::retriable(format!(
				"http status {}",
				status.as_u16()
			)));
		}

		let parsed = response
			.json::<RpcResponse<T>>()
			.await
			.map_err(|e| AttemptError::retriable(format!("invalid JSON-RPC body: {e}")))?;

		if let Some(err) = parsed.error {
			return Err(AttemptError::non_retriable(format!(
				"rpc code={} message={}",
				err.code, err.message
			)));
		}

		parsed
			.result
			.ok_or_else(|| AttemptError::non_retriable("missing result".to_string()))
	}

	async fn get_chain_id(&self, chain_id: u64) -> Result<u64, DeliveryError> {
		let raw: String = self.rpc_call(chain_id, "eth_chainId", json!([])).await?;
		parse_u64_quantity(&raw)
	}

	async fn get_block_timestamp(&self, chain_id: u64, block_number: u64) -> Result<Option<u64>, DeliveryError> {
		let block_num_hex = to_hex_quantity(block_number);
		let block: Option<RpcBlock> = self
			.rpc_call(chain_id, "eth_getBlockByNumber", json!([block_num_hex, false]))
			.await?;
		match block {
			Some(b) => b.timestamp.as_deref().map(parse_u64_quantity).transpose(),
			None => Ok(None),
		}
	}
}

pub struct TronNativeDeliverySchema;

impl TronNativeDeliverySchema {
	pub fn validate_config(
		config: &serde_json::Value,
	) -> Result<(), solver_types::ValidationError> {
		let instance = Self;
		instance.validate(config)
	}
}

impl ConfigSchema for TronNativeDeliverySchema {
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
			vec![
				Field::new(
					"max_retries",
					FieldType::Integer {
						min: Some(0),
						max: Some(20),
					},
				),
				Field::new(
					"initial_backoff_ms",
					FieldType::Integer {
						min: Some(1),
						max: None,
					},
				),
				Field::new(
					"max_backoff_ms",
					FieldType::Integer {
						min: Some(1),
						max: None,
					},
				),
				Field::new(
					"request_timeout_ms",
					FieldType::Integer {
						min: Some(1),
						max: None,
					},
				),
			],
		);
		schema.validate(config)?;

		let initial_backoff_ms = config
			.get("initial_backoff_ms")
			.and_then(Value::as_u64)
			.unwrap_or(300);
		let max_backoff_ms = config
			.get("max_backoff_ms")
			.and_then(Value::as_u64)
			.unwrap_or(3_000);

		if max_backoff_ms < initial_backoff_ms {
			return Err(solver_types::ValidationError::InvalidValue {
				field: "max_backoff_ms".to_string(),
				message: "max_backoff_ms must be >= initial_backoff_ms".to_string(),
			});
		}

		Ok(())
	}
}

#[async_trait]
impl DeliveryInterface for TronNativeDelivery {
	fn config_schema(&self) -> Box<dyn ConfigSchema> {
		Box::new(TronNativeDeliverySchema)
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
		hash: &TransactionHash,
		chain_id: u64,
	) -> Result<TransactionReceipt, DeliveryError> {
		let tx_hash = bytes_to_0x(&hash.0);
		let receipt: Option<RpcReceipt> = self
			.rpc_call(chain_id, "eth_getTransactionReceipt", json!([tx_hash]))
			.await?;

		let receipt = receipt.ok_or_else(|| {
			DeliveryError::Network(format!(
				"Tron RPC error: transaction receipt not found on chain {chain_id}"
			))
		})?;

		let block_number = parse_u64_quantity(&receipt.block_number)?;
		let block_timestamp = self.get_block_timestamp(chain_id, block_number).await?;

		let logs = receipt
			.logs
			.into_iter()
			.map(|entry| {
				let topics = entry
					.topics
					.into_iter()
					.map(|topic| parse_h256(&topic))
					.collect::<Result<Vec<_>, _>>()?;
				let data = parse_hex_bytes(&entry.data)?;
				let address = parse_address_20(&entry.address)?;

				Ok(Log {
					address,
					topics,
					data,
				})
			})
			.collect::<Result<Vec<_>, DeliveryError>>()?;

		Ok(TransactionReceipt {
			hash: TransactionHash(parse_hex_bytes(&receipt.transaction_hash)?),
			block_number,
			success: parse_status_success(receipt.status.as_deref()),
			logs,
			block_timestamp,
		})
	}

	async fn get_gas_price(&self, chain_id: u64) -> Result<String, DeliveryError> {
		let raw: String = self.rpc_call(chain_id, "eth_gasPrice", json!([])).await?;
		let value = parse_u256_quantity(&raw)?;
		Ok(value.to_string())
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

	async fn get_block_number(&self, chain_id: u64) -> Result<u64, DeliveryError> {
		let block: Option<RpcBlock> = self
			.rpc_call(chain_id, "eth_getBlockByNumber", json!(["latest", false]))
			.await?;
		let block = block.ok_or_else(|| {
			DeliveryError::Network(format!(
				"Tron RPC error: eth_getBlockByNumber returned null on chain {chain_id}"
			))
		})?;
		parse_u64_quantity(&block.number)
	}

	async fn estimate_gas(&self, _tx: Transaction) -> Result<u64, DeliveryError> {
		Err(Self::not_implemented("estimate_gas"))
	}

	async fn eth_call(&self, tx: Transaction) -> Result<Bytes, DeliveryError> {
		let call_obj = json!({
			"to": tx.to.as_ref().map(|a| bytes_to_0x(&a.0)),
			"data": bytes_to_0x(&tx.data),
			"value": to_hex_quantity_u256(tx.value),
			"gas": tx.gas_limit.map(to_hex_quantity),
			"gasPrice": tx.gas_price.map(to_hex_quantity_u128),
			"maxFeePerGas": tx.max_fee_per_gas.map(to_hex_quantity_u128),
			"maxPriorityFeePerGas": tx.max_priority_fee_per_gas.map(to_hex_quantity_u128),
			"nonce": tx.nonce.map(to_hex_quantity),
		});
		let raw: String = self
			.rpc_call(tx.chain_id, "eth_call", json!([call_obj, "latest"]))
			.await?;
		let data = parse_hex_bytes(&raw)?;
		Ok(Bytes::from(data))
	}

	async fn tx_exists(&self, hash: &TransactionHash, chain_id: u64) -> Result<bool, DeliveryError> {
		let tx_hash = bytes_to_0x(&hash.0);
		let receipt: Option<RpcReceipt> = self
			.rpc_call(chain_id, "eth_getTransactionReceipt", json!([tx_hash]))
			.await?;
		Ok(receipt.is_some())
	}

	async fn get_logs(&self, chain_id: u64, filter: LogFilter) -> Result<Vec<Log>, DeliveryError> {
		let topics = filter
			.topics()
			.iter()
			.map(|topic| match topic {
				Some(h) => Value::String(bytes_to_0x(&h.0)),
				None => Value::Null,
			})
			.collect::<Vec<_>>();

		let filter_json = json!({
			"address": bytes_to_0x(&filter.address.0),
			"fromBlock": to_hex_quantity(filter.from_block),
			"toBlock": filter.to_block.map(to_hex_quantity).unwrap_or_else(|| "latest".to_string()),
			"topics": topics,
		});

		let raw_logs: Vec<RpcLog> = self
			.rpc_call(chain_id, "eth_getLogs", json!([filter_json]))
			.await?;

		raw_logs
			.into_iter()
			.map(|entry| {
				let topics = entry
					.topics
					.into_iter()
					.map(|topic| parse_h256(&topic))
					.collect::<Result<Vec<_>, _>>()?;
				let data = parse_hex_bytes(&entry.data)?;
				let address = parse_address_20(&entry.address)?;

				Ok(Log {
					address,
					topics,
					data,
				})
			})
			.collect()
	}
}

pub fn create_tron_native_delivery(
	config: &serde_json::Value,
	networks: &NetworksConfig,
	_default_signer: &AccountSigner,
	_network_signers: &HashMap<u64, AccountSigner>,
) -> Result<Box<dyn DeliveryInterface>, DeliveryError> {
	TronNativeDeliverySchema::validate_config(config)
		.map_err(|e| DeliveryError::Network(format!("Tron RPC error: invalid configuration: {e}")))?;

	let retry_policy = RetryPolicy::from_config(config)?;
	let network_ids = config
		.get("network_ids")
		.and_then(Value::as_array)
		.map(|arr| {
			arr.iter()
				.filter_map(|v| v.as_i64().map(|i| i as u64))
				.collect::<Vec<_>>()
		})
		.ok_or_else(|| DeliveryError::Network("Tron RPC error: network_ids is required".to_string()))?;

	let delivery = tokio::task::block_in_place(|| {
		tokio::runtime::Handle::current().block_on(async {
			TronNativeDelivery::new(network_ids, networks, retry_policy).await
		})
	})?;

	Ok(Box::new(delivery))
}

pub struct Registry;

impl solver_types::ImplementationRegistry for Registry {
	const NAME: &'static str = "tron_native";
	type Factory = crate::DeliveryFactory;

	fn factory() -> Self::Factory {
		create_tron_native_delivery
	}
}

impl crate::DeliveryRegistry for Registry {}

#[derive(Debug)]
struct AttemptError {
	message: String,
	retriable: bool,
}

impl AttemptError {
	fn retriable(message: String) -> Self {
		Self {
			message,
			retriable: true,
		}
	}

	fn non_retriable(message: String) -> Self {
		Self {
			message,
			retriable: false,
		}
	}
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
	result: Option<T>,
	error: Option<RpcErrorObj>,
}

#[derive(Debug, Deserialize)]
struct RpcErrorObj {
	code: i64,
	message: String,
}

#[derive(Debug, Deserialize)]
struct RpcReceipt {
	#[serde(rename = "transactionHash")]
	transaction_hash: String,
	#[serde(rename = "blockNumber")]
	block_number: String,
	status: Option<String>,
	logs: Vec<RpcLog>,
}

#[derive(Debug, Deserialize)]
struct RpcLog {
	address: String,
	topics: Vec<String>,
	data: String,
}

#[derive(Debug, Deserialize)]
struct RpcBlock {
	number: String,
	timestamp: Option<String>,
}

fn parse_hex_bytes(value: &str) -> Result<Vec<u8>, DeliveryError> {
	let clean = value.strip_prefix("0x").unwrap_or(value);
	if clean.is_empty() {
		return Ok(Vec::new());
	}
	hex::decode(clean).map_err(|e| DeliveryError::Network(format!("Tron RPC error: invalid hex data: {e}")))
}

fn parse_u64_quantity(value: &str) -> Result<u64, DeliveryError> {
	let clean = value.trim();
	if let Some(hex) = clean.strip_prefix("0x") {
		u64::from_str_radix(hex, 16).map_err(|e| {
			DeliveryError::Network(format!("Tron RPC error: invalid hex quantity '{value}': {e}"))
		})
	} else {
		clean.parse::<u64>().map_err(|e| {
			DeliveryError::Network(format!("Tron RPC error: invalid decimal quantity '{value}': {e}"))
		})
	}
}

fn parse_u256_quantity(value: &str) -> Result<U256, DeliveryError> {
	let clean = value.trim();
	if let Some(hex) = clean.strip_prefix("0x") {
		U256::from_str_radix(hex, 16).map_err(|e| {
			DeliveryError::Network(format!("Tron RPC error: invalid U256 hex quantity '{value}': {e}"))
		})
	} else {
		U256::from_str_radix(clean, 10).map_err(|e| {
			DeliveryError::Network(format!(
				"Tron RPC error: invalid U256 decimal quantity '{value}': {e}"
			))
		})
	}
}

fn parse_h256(value: &str) -> Result<H256, DeliveryError> {
	let bytes = parse_hex_bytes(value)?;
	if bytes.len() != 32 {
		return Err(DeliveryError::Network(format!(
			"Tron RPC error: topic length must be 32 bytes, got {}",
			bytes.len()
		)));
	}
	let mut out = [0u8; 32];
	out.copy_from_slice(&bytes);
	Ok(H256(out))
}

fn parse_address_20(value: &str) -> Result<Address, DeliveryError> {
	let bytes = parse_hex_bytes(value)?;
	if bytes.len() != 20 {
		return Err(DeliveryError::Network(format!(
			"Tron RPC error: address length must be 20 bytes, got {}",
			bytes.len()
		)));
	}
	Ok(Address(bytes))
}

fn parse_status_success(status: Option<&str>) -> bool {
	matches!(status, Some("0x1") | Some("1"))
}

fn bytes_to_0x(bytes: &[u8]) -> String {
	format!("0x{}", hex::encode(bytes))
}

fn to_hex_quantity(value: u64) -> String {
	format!("0x{value:x}")
}

fn to_hex_quantity_u128(value: u128) -> String {
	format!("0x{value:x}")
}

fn to_hex_quantity_u256(value: U256) -> String {
	format!("0x{value:x}")
}

#[cfg(test)]
mod tests {
	use super::*;
	use solver_types::networks::RpcEndpoint;
	use solver_types::utils::tests::builders::{NetworkConfigBuilder, NetworksConfigBuilder};

	#[test]
	fn test_registry_name() {
		assert_eq!(
			<Registry as solver_types::ImplementationRegistry>::NAME,
			"tron_native"
		);
	}

	#[test]
	fn test_retry_policy_defaults() {
		let policy = RetryPolicy::default();
		assert_eq!(policy.max_retries, 2);
		assert_eq!(policy.initial_backoff_ms, 300);
		assert_eq!(policy.max_backoff_ms, 3_000);
		assert_eq!(policy.request_timeout_ms, 8_000);
	}

	#[test]
	fn test_schema_validation_rejects_invalid_backoff_order() {
		let config = json!({
			"network_ids": [2494104990u64],
			"initial_backoff_ms": 1000,
			"max_backoff_ms": 100
		});
		let result = TronNativeDeliverySchema.validate(&config);
		assert!(result.is_err());
	}

	#[test]
	fn test_parse_u64_quantity() {
		assert_eq!(parse_u64_quantity("0x10").unwrap(), 16);
		assert_eq!(parse_u64_quantity("42").unwrap(), 42);
	}

	#[test]
	fn test_parse_u256_quantity() {
		assert_eq!(parse_u256_quantity("0x0").unwrap(), U256::ZERO);
		assert_eq!(parse_u256_quantity("10").unwrap(), U256::from(10u64));
	}

	#[test]
	fn test_not_implemented_error_prefix() {
		let err = TronNativeDelivery::not_implemented("submit");
		assert!(err
			.to_string()
			.contains("NotImplemented: tron_native submit is not implemented"));
	}

	#[test]
	fn test_endpoints_builder_uses_all_http_urls() {
		let networks = NetworksConfigBuilder::new()
			.add_network(
				2494104990,
				NetworkConfigBuilder::new()
					.rpc_endpoints(vec![
						RpcEndpoint::http_only("https://primary.example".to_string()),
						RpcEndpoint::http_only("https://backup.example".to_string()),
					])
					.build(),
			)
			.build();
		let urls = networks
			.get(&2494104990)
			.unwrap()
			.get_all_http_urls()
			.into_iter()
			.collect::<Vec<_>>();
		assert!(urls.contains(&"https://primary.example"));
		assert!(urls.contains(&"https://backup.example"));
	}
}
