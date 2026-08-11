//! Runtime configuration.
//!
//! Configuration and secrets (notably `PRE_SHARED_SECURITY_TOKEN`) are loaded
//! from a config file at runtime only — nothing is baked into the binary. The
//! caller is expected to treat a missing or unreadable config file as fatal,
//! so the process never runs with an empty/embedded fallback configuration.

/// Loads `env_file` (a `KEY=value` file, `#` comments allowed) into the process
/// environment. Returns an error if the file cannot be read.
pub fn load_config(env_file: &str) -> std::io::Result<()> {
    let config = std::fs::read_to_string(env_file)?;
    read(config);
    Ok(())
}

fn read(config: String) {
    for line in config.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, '=');
        if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
            let value = value.trim();
            // Strip surrounding double quotes if present.
            let value = if value.len() >= 2 && value.starts_with('"') && value.ends_with('"') {
                &value[1..value.len() - 1]
            } else {
                value
            };
            std::env::set_var(key.trim(), value);
        }
    }
}

pub fn var(v: &str) -> Result<String, std::env::VarError> {
    std::env::var(v)
}
