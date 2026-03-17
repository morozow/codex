//! Configuration generation for stdio_bus.

use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::path::Path;

/// stdio_bus pool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    pub id: String,
    pub command: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    pub instances: u32,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
}

/// stdio_bus limits configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitsConfig {
    #[serde(default = "default_max_input_buffer")]
    pub max_input_buffer: usize,
    #[serde(default = "default_max_output_queue")]
    pub max_output_queue: usize,
    #[serde(default = "default_max_restarts")]
    pub max_restarts: u32,
    #[serde(default = "default_restart_window_sec")]
    pub restart_window_sec: u32,
    #[serde(default = "default_drain_timeout_sec")]
    pub drain_timeout_sec: u32,
    #[serde(default = "default_backpressure_timeout_sec")]
    pub backpressure_timeout_sec: u32,
}

fn default_max_input_buffer() -> usize {
    1_048_576
} // 1MB
fn default_max_output_queue() -> usize {
    4_194_304
} // 4MB
fn default_max_restarts() -> u32 {
    5
}
fn default_restart_window_sec() -> u32 {
    60
}
fn default_drain_timeout_sec() -> u32 {
    30
}
fn default_backpressure_timeout_sec() -> u32 {
    60
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_input_buffer: default_max_input_buffer(),
            max_output_queue: default_max_output_queue(),
            max_restarts: default_max_restarts(),
            restart_window_sec: default_restart_window_sec(),
            drain_timeout_sec: default_drain_timeout_sec(),
            backpressure_timeout_sec: default_backpressure_timeout_sec(),
        }
    }
}

/// stdio_bus routing configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    #[serde(default = "default_session_id_field")]
    pub session_id_field: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_pool: Option<String>,
}

fn default_session_id_field() -> String {
    "sessionId".to_string()
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            session_id_field: default_session_id_field(),
            default_pool: None,
        }
    }
}

/// Complete stdio_bus configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StdioBusConfig {
    pub pools: Vec<PoolConfig>,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub routing: RoutingConfig,
}

/// Configuration generation error.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Invalid configuration: {0}")]
    Invalid(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl StdioBusConfig {
    /// Generate development configuration.
    pub fn development(codex_home: &Path) -> Self {
        Self {
            pools: vec![PoolConfig {
                id: "app-server".to_string(),
                command: "cargo".to_string(),
                args: vec![
                    "run".to_string(),
                    "-p".to_string(),
                    "codex-app-server".to_string(),
                    "--".to_string(),
                    "--worker".to_string(),
                ],
                instances: 1,
                env: HashMap::from([
                    ("CODEX_HOME".to_string(), codex_home.display().to_string()),
                    ("RUST_LOG".to_string(), "debug".to_string()),
                ]),
                cwd: None,
            }],
            limits: LimitsConfig {
                max_restarts: 10,
                restart_window_sec: 300,
                ..Default::default()
            },
            routing: RoutingConfig::default(),
        }
    }

    /// Generate production configuration.
    pub fn production(app_server_path: &str, instances: u32) -> Self {
        Self {
            pools: vec![PoolConfig {
                id: "app-server".to_string(),
                command: app_server_path.to_string(),
                args: vec!["--worker".to_string()],
                instances,
                env: HashMap::from([("RUST_LOG".to_string(), "warn".to_string())]),
                cwd: None,
            }],
            limits: LimitsConfig::default(),
            routing: RoutingConfig {
                default_pool: Some("app-server".to_string()),
                ..Default::default()
            },
        }
    }

    /// Substitute environment variables in config.
    pub fn substitute_env_vars(&mut self) {
        for pool in &mut self.pools {
            pool.command = substitute_env(&pool.command);
            pool.args = pool.args.iter().map(|a| substitute_env(a)).collect();
            if let Some(cwd) = &pool.cwd {
                pool.cwd = Some(substitute_env(cwd));
            }
            pool.env = pool
                .env
                .iter()
                .map(|(k, v)| (k.clone(), substitute_env(v)))
                .collect();
        }
    }

    /// Validate configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.pools.is_empty() {
            return Err(ConfigError::Invalid(
                "At least one pool is required".to_string(),
            ));
        }

        for pool in &self.pools {
            if pool.id.is_empty() {
                return Err(ConfigError::Invalid("Pool ID cannot be empty".to_string()));
            }
            if pool.command.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "Pool '{}' command cannot be empty",
                    pool.id
                )));
            }
            if pool.instances == 0 {
                return Err(ConfigError::Invalid(format!(
                    "Pool '{}' must have at least 1 instance",
                    pool.id
                )));
            }
        }

        Ok(())
    }

    /// Write configuration to file.
    pub fn write_to_file(&self, path: &Path) -> Result<(), ConfigError> {
        self.validate()?;
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

fn substitute_env(s: &str) -> String {
    let mut result = s.to_string();
    for (key, value) in std::env::vars() {
        result = result.replace(&format!("${{{key}}}"), &value);
        result = result.replace(&format!("${key}"), &value);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// **Validates: Requirements 6.1, 6.2, 6.3**
    ///
    /// Property 10: Configuration Generation and Validation
    /// Tests that generated configs are valid JSON and pass schema validation.
    /// Tests environment variable substitution.
    mod property_config_generation {
        use super::*;

        /// Strategy for generating valid pool IDs.
        fn pool_id_strategy() -> impl Strategy<Value = String> {
            "[a-zA-Z][a-zA-Z0-9_-]{0,31}".prop_map(String::from)
        }

        /// Strategy for generating valid commands.
        fn command_strategy() -> impl Strategy<Value = String> {
            "[a-zA-Z][a-zA-Z0-9/_.-]{0,63}".prop_map(String::from)
        }

        /// Strategy for generating valid arguments.
        fn args_strategy() -> impl Strategy<Value = Vec<String>> {
            prop::collection::vec("[a-zA-Z0-9_=-]{1,32}", 0..5)
        }

        /// Strategy for generating valid environment variable names.
        fn env_key_strategy() -> impl Strategy<Value = String> {
            "[A-Z][A-Z0-9_]{0,31}".prop_map(String::from)
        }

        /// Strategy for generating valid environment variable values.
        fn env_value_strategy() -> impl Strategy<Value = String> {
            "[a-zA-Z0-9_/.-]{0,64}".prop_map(String::from)
        }

        /// Strategy for generating valid pool configurations.
        fn pool_config_strategy() -> impl Strategy<Value = PoolConfig> {
            (
                pool_id_strategy(),
                command_strategy(),
                args_strategy(),
                1u32..=16u32,
                prop::collection::hash_map(env_key_strategy(), env_value_strategy(), 0..3),
                prop::option::of("[a-zA-Z0-9/_.-]{1,64}".prop_map(String::from)),
            )
                .prop_map(|(id, command, args, instances, env, cwd)| PoolConfig {
                    id,
                    command,
                    args,
                    instances,
                    env,
                    cwd,
                })
        }

        /// Strategy for generating valid limits configurations.
        fn limits_config_strategy() -> impl Strategy<Value = LimitsConfig> {
            (
                1usize..=10_000_000usize,
                1usize..=10_000_000usize,
                1u32..=100u32,
                1u32..=3600u32,
                1u32..=300u32,
                1u32..=300u32,
            )
                .prop_map(
                    |(
                        max_input_buffer,
                        max_output_queue,
                        max_restarts,
                        restart_window_sec,
                        drain_timeout_sec,
                        backpressure_timeout_sec,
                    )| LimitsConfig {
                        max_input_buffer,
                        max_output_queue,
                        max_restarts,
                        restart_window_sec,
                        drain_timeout_sec,
                        backpressure_timeout_sec,
                    },
                )
        }

        /// Strategy for generating valid routing configurations.
        fn routing_config_strategy() -> impl Strategy<Value = RoutingConfig> {
            (
                "[a-zA-Z][a-zA-Z0-9_]{0,31}".prop_map(String::from),
                prop::option::of(pool_id_strategy()),
            )
                .prop_map(|(session_id_field, default_pool)| RoutingConfig {
                    session_id_field,
                    default_pool,
                })
        }

        /// Strategy for generating valid stdio_bus configurations.
        fn stdio_bus_config_strategy() -> impl Strategy<Value = StdioBusConfig> {
            (
                prop::collection::vec(pool_config_strategy(), 1..=4),
                limits_config_strategy(),
                routing_config_strategy(),
            )
                .prop_map(|(pools, limits, routing)| StdioBusConfig {
                    pools,
                    limits,
                    routing,
                })
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(500))]

            /// Test that generated configs serialize to valid JSON.
            #[test]
            fn generated_configs_serialize_to_valid_json(config in stdio_bus_config_strategy()) {
                // Serialize to JSON
                let json_result = serde_json::to_string(&config);
                prop_assert!(json_result.is_ok(), "Config should serialize to JSON");

                let json = json_result.unwrap();

                // Verify it's valid JSON by parsing it back
                let parsed: Result<serde_json::Value, _> = serde_json::from_str(&json);
                prop_assert!(parsed.is_ok(), "Serialized JSON should be parseable");

                // Verify it's a JSON object
                let value = parsed.unwrap();
                prop_assert!(value.is_object(), "Config should serialize to a JSON object");
            }

            /// Test that generated configs pass validation.
            #[test]
            fn generated_configs_pass_validation(config in stdio_bus_config_strategy()) {
                let result = config.validate();
                prop_assert!(result.is_ok(), "Valid config should pass validation: {:?}", result.err());
            }

            /// Test that configs round-trip through JSON serialization.
            #[test]
            fn configs_roundtrip_through_json(config in stdio_bus_config_strategy()) {
                let json = serde_json::to_string(&config).unwrap();
                let deserialized: StdioBusConfig = serde_json::from_str(&json).unwrap();

                // Verify key fields match
                prop_assert_eq!(config.pools.len(), deserialized.pools.len());
                for (orig, deser) in config.pools.iter().zip(deserialized.pools.iter()) {
                    prop_assert_eq!(&orig.id, &deser.id);
                    prop_assert_eq!(&orig.command, &deser.command);
                    prop_assert_eq!(orig.instances, deser.instances);
                }
                prop_assert_eq!(config.limits.max_input_buffer, deserialized.limits.max_input_buffer);
                prop_assert_eq!(config.limits.max_restarts, deserialized.limits.max_restarts);
                prop_assert_eq!(&config.routing.session_id_field, &deserialized.routing.session_id_field);
            }

            /// Test that development preset generates valid config.
            #[test]
            fn development_preset_generates_valid_config(
                home_path in "[a-zA-Z0-9/_.-]{1,64}"
            ) {
                let path = Path::new(&home_path);
                let config = StdioBusConfig::development(path);

                prop_assert!(config.validate().is_ok());
                prop_assert!(!config.pools.is_empty());
                prop_assert_eq!(&config.pools[0].id, "app-server");
            }

            /// Test that production preset generates valid config.
            #[test]
            fn production_preset_generates_valid_config(
                app_server_path in "[a-zA-Z0-9/_.-]{1,64}",
                instances in 1u32..=16u32,
            ) {
                let config = StdioBusConfig::production(&app_server_path, instances);

                prop_assert!(config.validate().is_ok());
                prop_assert!(!config.pools.is_empty());
                prop_assert_eq!(&config.pools[0].id, "app-server");
                prop_assert_eq!(config.pools[0].instances, instances);
                prop_assert_eq!(&config.routing.default_pool, &Some("app-server".to_string()));
            }
        }
    }

    /// **Validates: Requirements 6.2**
    ///
    /// Property: Environment Variable Substitution
    /// Tests that environment variable substitution works correctly.
    /// Note: These tests use unit tests instead of property tests because
    /// std::env::set_var/remove_var are unsafe in Rust 2024 edition and
    /// cannot be safely used in property tests that may run concurrently.
    mod property_env_substitution {
        use super::*;

        #[test]
        fn substitutes_braced_env_vars() {
            let var_name = "TEST_CONFIG_BRACED_VAR";
            let var_value = "test_value_123";

            // SAFETY: This test runs single-threaded and we clean up after.
            unsafe {
                std::env::set_var(var_name, var_value);
            }

            let input = format!("prefix/${{{var_name}}}/suffix");
            let result = substitute_env(&input);

            assert_eq!(result, format!("prefix/{var_value}/suffix"));

            // Clean up
            unsafe {
                std::env::remove_var(var_name);
            }
        }

        #[test]
        fn substitutes_unbraced_env_vars() {
            let var_name = "TEST_CONFIG_UNBRACED_VAR";
            let var_value = "unbraced_value";

            // SAFETY: This test runs single-threaded and we clean up after.
            unsafe {
                std::env::set_var(var_name, var_value);
            }

            let input = format!("prefix/${var_name}/suffix");
            let result = substitute_env(&input);

            assert_eq!(result, format!("prefix/{var_value}/suffix"));

            // Clean up
            unsafe {
                std::env::remove_var(var_name);
            }
        }

        #[test]
        fn config_substitute_env_vars_works() {
            let var_name = "TEST_CONFIG_SUBST_VAR";
            let var_value = "substituted";

            // SAFETY: This test runs single-threaded and we clean up after.
            unsafe {
                std::env::set_var(var_name, var_value);
            }

            let mut config = StdioBusConfig {
                pools: vec![PoolConfig {
                    id: "test".to_string(),
                    command: format!("${{{var_name}}}"),
                    args: vec![format!("--path=${var_name}")],
                    instances: 1,
                    env: HashMap::from([("TEST_VAR".to_string(), format!("${{{var_name}}}"))]),
                    cwd: Some(format!("${{{var_name}}}/work")),
                }],
                limits: LimitsConfig::default(),
                routing: RoutingConfig::default(),
            };

            config.substitute_env_vars();

            assert_eq!(&config.pools[0].command, var_value);
            assert_eq!(&config.pools[0].args[0], &format!("--path={var_value}"));
            assert_eq!(
                config.pools[0].env.get("TEST_VAR"),
                Some(&var_value.to_string())
            );
            assert_eq!(config.pools[0].cwd, Some(format!("{var_value}/work")));

            // Clean up
            unsafe {
                std::env::remove_var(var_name);
            }
        }

        #[test]
        fn handles_missing_env_vars() {
            let input = "${NONEXISTENT_VAR_12345}/path";
            let result = substitute_env(input);

            // Missing vars should remain unchanged
            assert_eq!(result, "${NONEXISTENT_VAR_12345}/path");
        }

        #[test]
        fn handles_multiple_env_vars() {
            // SAFETY: This test runs single-threaded and we clean up after.
            unsafe {
                std::env::set_var("TEST_CONFIG_VAR_A", "value_a");
                std::env::set_var("TEST_CONFIG_VAR_B", "value_b");
            }

            let input = "${TEST_CONFIG_VAR_A}/${TEST_CONFIG_VAR_B}";
            let result = substitute_env(input);

            assert_eq!(result, "value_a/value_b");

            // Clean up
            unsafe {
                std::env::remove_var("TEST_CONFIG_VAR_A");
                std::env::remove_var("TEST_CONFIG_VAR_B");
            }
        }
    }

    /// **Validates: Requirements 6.3**
    ///
    /// Property: Invalid Configuration Rejection
    /// Tests that invalid configs are rejected by validation.
    mod property_invalid_config_rejection {
        use super::*;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(200))]

            /// Test that configs with empty pools are rejected.
            #[test]
            fn rejects_empty_pools(
                limits in (1usize..1000usize, 1usize..1000usize, 1u32..10u32, 1u32..100u32, 1u32..100u32, 1u32..100u32)
                    .prop_map(|(a, b, c, d, e, f)| LimitsConfig {
                        max_input_buffer: a,
                        max_output_queue: b,
                        max_restarts: c,
                        restart_window_sec: d,
                        drain_timeout_sec: e,
                        backpressure_timeout_sec: f,
                    }),
            ) {
                let config = StdioBusConfig {
                    pools: vec![],
                    limits,
                    routing: RoutingConfig::default(),
                };

                let result = config.validate();
                prop_assert!(result.is_err());
                if let Err(ConfigError::Invalid(msg)) = result {
                    prop_assert!(msg.contains("pool"));
                }
            }

            /// Test that pools with empty ID are rejected.
            #[test]
            fn rejects_empty_pool_id(
                command in "[a-zA-Z][a-zA-Z0-9/_.-]{0,31}",
                instances in 1u32..=16u32,
            ) {
                let config = StdioBusConfig {
                    pools: vec![PoolConfig {
                        id: String::new(),
                        command,
                        args: vec![],
                        instances,
                        env: HashMap::new(),
                        cwd: None,
                    }],
                    limits: LimitsConfig::default(),
                    routing: RoutingConfig::default(),
                };

                let result = config.validate();
                prop_assert!(result.is_err());
                if let Err(ConfigError::Invalid(msg)) = result {
                    prop_assert!(msg.contains("ID"));
                }
            }

            /// Test that pools with empty command are rejected.
            #[test]
            fn rejects_empty_command(
                id in "[a-zA-Z][a-zA-Z0-9_-]{0,31}",
                instances in 1u32..=16u32,
            ) {
                let config = StdioBusConfig {
                    pools: vec![PoolConfig {
                        id,
                        command: String::new(),
                        args: vec![],
                        instances,
                        env: HashMap::new(),
                        cwd: None,
                    }],
                    limits: LimitsConfig::default(),
                    routing: RoutingConfig::default(),
                };

                let result = config.validate();
                prop_assert!(result.is_err());
                if let Err(ConfigError::Invalid(msg)) = result {
                    prop_assert!(msg.contains("command"));
                }
            }

            /// Test that pools with zero instances are rejected.
            #[test]
            fn rejects_zero_instances(
                id in "[a-zA-Z][a-zA-Z0-9_-]{0,31}",
                command in "[a-zA-Z][a-zA-Z0-9/_.-]{0,31}",
            ) {
                let config = StdioBusConfig {
                    pools: vec![PoolConfig {
                        id,
                        command,
                        args: vec![],
                        instances: 0,
                        env: HashMap::new(),
                        cwd: None,
                    }],
                    limits: LimitsConfig::default(),
                    routing: RoutingConfig::default(),
                };

                let result = config.validate();
                prop_assert!(result.is_err());
                if let Err(ConfigError::Invalid(msg)) = result {
                    prop_assert!(msg.contains("instance"));
                }
            }
        }
    }

    /// Unit tests for specific edge cases.
    mod unit_tests {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn default_limits_config_has_expected_values() {
            let limits = LimitsConfig::default();

            assert_eq!(limits.max_input_buffer, 1_048_576);
            assert_eq!(limits.max_output_queue, 4_194_304);
            assert_eq!(limits.max_restarts, 5);
            assert_eq!(limits.restart_window_sec, 60);
            assert_eq!(limits.drain_timeout_sec, 30);
            assert_eq!(limits.backpressure_timeout_sec, 60);
        }

        #[test]
        fn default_routing_config_has_expected_values() {
            let routing = RoutingConfig::default();

            assert_eq!(routing.session_id_field, "sessionId");
            assert_eq!(routing.default_pool, None);
        }

        #[test]
        fn development_config_has_debug_settings() {
            let config = StdioBusConfig::development(Path::new("/home/user/.codex"));

            assert_eq!(config.pools.len(), 1);
            assert_eq!(config.pools[0].id, "app-server");
            assert_eq!(config.pools[0].command, "cargo");
            assert_eq!(config.pools[0].instances, 1);
            assert_eq!(
                config.pools[0].env.get("RUST_LOG"),
                Some(&"debug".to_string())
            );
            assert_eq!(config.limits.max_restarts, 10);
            assert_eq!(config.limits.restart_window_sec, 300);
        }

        #[test]
        fn production_config_has_production_settings() {
            let config = StdioBusConfig::production("/usr/bin/codex-app-server", 4);

            assert_eq!(config.pools.len(), 1);
            assert_eq!(config.pools[0].id, "app-server");
            assert_eq!(config.pools[0].command, "/usr/bin/codex-app-server");
            assert_eq!(config.pools[0].instances, 4);
            assert_eq!(
                config.pools[0].env.get("RUST_LOG"),
                Some(&"warn".to_string())
            );
            assert_eq!(config.routing.default_pool, Some("app-server".to_string()));
        }

        #[test]
        fn config_serializes_to_pretty_json() {
            let config = StdioBusConfig::production("/usr/bin/app", 2);
            let json = serde_json::to_string_pretty(&config).unwrap();

            // Verify it contains expected structure
            assert!(json.contains("\"pools\""));
            assert!(json.contains("\"limits\""));
            assert!(json.contains("\"routing\""));
            assert!(json.contains("\"app-server\""));
        }

        #[test]
        fn validate_catches_multiple_invalid_pools() {
            let config = StdioBusConfig {
                pools: vec![
                    PoolConfig {
                        id: "valid".to_string(),
                        command: "cmd".to_string(),
                        args: vec![],
                        instances: 1,
                        env: HashMap::new(),
                        cwd: None,
                    },
                    PoolConfig {
                        id: "".to_string(), // Invalid
                        command: "cmd".to_string(),
                        args: vec![],
                        instances: 1,
                        env: HashMap::new(),
                        cwd: None,
                    },
                ],
                limits: LimitsConfig::default(),
                routing: RoutingConfig::default(),
            };

            let result = config.validate();
            assert!(result.is_err());
        }

        #[test]
        fn empty_args_and_env_are_not_serialized() {
            let pool = PoolConfig {
                id: "test".to_string(),
                command: "cmd".to_string(),
                args: vec![],
                instances: 1,
                env: HashMap::new(),
                cwd: None,
            };

            let json = serde_json::to_string(&pool).unwrap();

            // Empty collections should be skipped
            assert!(!json.contains("\"args\""));
            assert!(!json.contains("\"env\""));
            assert!(!json.contains("\"cwd\""));
        }

        #[test]
        fn non_empty_args_and_env_are_serialized() {
            let pool = PoolConfig {
                id: "test".to_string(),
                command: "cmd".to_string(),
                args: vec!["--flag".to_string()],
                instances: 1,
                env: HashMap::from([("KEY".to_string(), "value".to_string())]),
                cwd: Some("/work".to_string()),
            };

            let json = serde_json::to_string(&pool).unwrap();

            assert!(json.contains("\"args\""));
            assert!(json.contains("\"env\""));
            assert!(json.contains("\"cwd\""));
        }
    }
}
