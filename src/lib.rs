//!
//! **A library to filter streams of data with Lua scripts.**
//!
//! This library is a wrapper around the Lua runtime to make it easier to use for filtering.
//! It is designed to be used in a server environment where the filter scripts are loaded from
//! a configuration file.
//!

use std::{collections::HashMap, path::PathBuf};

use mlua::{prelude::LuaUserData, Lua, LuaSerdeExt};
use serde::{Deserialize, Serialize};

/// The filter configuration file structure.
#[derive(Deserialize)]
pub struct Config {
    chains: HashMap<String, Vec<FilterConfig>>,
}

/// The name and script location of a filter.
#[derive(Deserialize)]
pub struct FilterConfig {
    name: String,
    script: PathBuf,
}

/// A filter backed by a Lua function.
pub struct Filter<'lua, T> {
    pub name: String,
    filter: mlua::Function<'lua>,
    _marker: std::marker::PhantomData<T>,
}

impl<'lua, T> Filter<'lua, T>
where
    T: LuaUserData + Serialize + Clone + Send + Sync + 'lua,
{
    /// Create a new filter.
    pub fn new(name: String, filter: mlua::Function<'lua>) -> Self {
        Self {
            name,
            filter,
            _marker: std::marker::PhantomData,
        }
    }

    /// Filter a transaction by a value.
    pub fn filter(&self, lua: &'lua Lua, value: T) -> Result<bool, mlua::Error> {
        let value = lua.to_value(&value)?;
        let result = self.filter.call(value)?;
        Ok(result)
    }
}

/// The filter runtime (Lua).
pub struct FilterRuntime<T> {
    runtime: Lua,
    _marker: std::marker::PhantomData<T>,
}

impl<T> FilterRuntime<T>
where
    T: LuaUserData + Serialize + Clone + Send + Sync,
{
    /// Create a new filter runtime.
    pub fn new() -> Self {
        Self {
            runtime: Lua::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Load a filter configuration.
    pub fn load(&self, config: Config) -> Result<FilterSystem<'_, T>, mlua::Error> {
        let mut system = FilterSystem::new(&self.runtime);
        system.load(config)?;
        Ok(system)
    }
}

/// A Lua runtime to filter incoming values
pub struct FilterSystem<'lua, T> {
    runtime: &'lua Lua,
    filters: Vec<Filter<'lua, T>>,
}

impl<'lua, T> FilterSystem<'lua, T>
where
    T: LuaUserData + Serialize + Clone + Send + Sync + 'lua,
{
    /// Create a new filter system.
    pub fn new(runtime: &'lua Lua) -> Self {
        Self {
            runtime,
            filters: Vec::new(),
        }
    }

    /// Load a filter configuration.
    pub fn load(&mut self, config: Config) -> Result<(), mlua::Error> {
        for (_chain, filters) in config.chains {
            for filter in filters {
                let script = std::fs::read_to_string(filter.script)?;
                let module: mlua::Table = self.runtime.load(&script).eval()?;
                for pair in module.pairs::<String, mlua::Function>() {
                    let (name, filter) = pair?;
                    let filter = Filter::new(name, filter);
                    self.filters.push(filter);
                    // q: How do I make self.filters.push work?
                    // a: https://stackoverflow.com/a/30353928/1123955
                }
            }
        }
        Ok(())
    }

    /// Filter a single value.
    pub fn filter_one(&self, value: T) -> Result<bool, mlua::Error> {
        let mut filtered = false;
        for filter in &self.filters {
            if filter.filter(&self.runtime, value.clone())? {
                filtered = true
            }
        }
        Ok(filtered)
    }

    /// Filter a list of values.
    pub fn filter(&self, values: Vec<T>) -> Result<Vec<T>, mlua::Error> {
        let mut result = Vec::new();
        for tx in values {
            if self.filter_one(tx.clone())? {
                result.push(tx);
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use serde::Serialize;

    use super::*;

    #[derive(Clone, Serialize, Deserialize)]
    pub struct MockTx {
        pub chain: String,
        pub from: String,
        pub to: String,
        pub amount: u64,
    }
    impl mlua::UserData for MockTx {}

    macro_rules! test_filter {
        ($name:ident, $script:expr, $expected:expr) => {
            #[test]
            fn $name() {
                let lua = mlua::Lua::new();
                let module: mlua::Table = lua.load($script).eval().unwrap();

                for pair in module.pairs::<String, mlua::Function>() {
                    let (_name, filter) = pair.unwrap();
                    let filter = Filter::new("$name".to_string(), filter);
                    let tx = MockTx {
                        chain: "uni-5".to_string(),
                        from: "0xDEADBEEF".to_string(),
                        to: "0xBEEFFEEF".to_string(),
                        amount: 0,
                    };
                    let result = filter.filter(&lua, tx).unwrap();
                    assert_eq!(result, $expected);
                }
            }
        };
    }

    #[test]
    fn config() {
        let input = indoc! {r#"
        chains:
            uni-5:
                - name: Testnet Manager
                  script: filters/uni-5-manager.lua
        "#};

        let config: Config = serde_yaml::from_str(input).unwrap();
        assert_eq!(config.chains.len(), 1);
        assert_eq!(config.chains["uni-5"].len(), 1);
        assert_eq!(config.chains["uni-5"][0].name, "Testnet Manager");
        assert_eq!(
            config.chains["uni-5"][0].script.to_str().unwrap(),
            "filters/uni-5-manager.lua"
        );
    }

    test_filter!(
        simple_filter,
        indoc! {r#"
        function filter(tx)
            return tx.from == "0xDEADBEEF"
        end

        return {
            filter = filter
        }
        "#},
        true
    );

    #[test]
    fn filter_system() {
        let config = Config {
            chains: {
                let mut chains = HashMap::new();
                chains.insert(
                    "uni-5".to_string(),
                    vec![FilterConfig {
                        name: "Testnet Manager".to_string(),
                        script: PathBuf::from("filters/uni-5-manager.lua"),
                    }],
                );
                chains
            },
        };

        let filter_runtime = FilterRuntime::new();
        let filter_system = filter_runtime.load(config).unwrap();

        let txs = vec![
            MockTx {
                chain: "uni-5".to_string(),
                from: "0xDEADBEEF".to_string(),
                to: "0xBEEFFEEF".to_string(),
                amount: 0,
            },
            MockTx {
                chain: "uni-5".to_string(),
                from: "0xBEEFFEEF".to_string(),
                to: "0xDEADDEAD".to_string(),
                amount: 0,
            },
        ];

        let filtered_txs = filter_system.filter(txs).unwrap();

        assert_eq!(filtered_txs.len(), 1);
        assert_eq!(filtered_txs[0].from, "0xDEADBEEF");
        assert_eq!(filtered_txs[0].to, "0xBEEFFEEF");
    }
}
