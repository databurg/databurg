pub const ENV_FILE: &str = include_str!("../../.env");

pub fn init() {
    read(ENV_FILE.to_string());
}

pub fn load_config(env_file: String) {
    if let Ok(config) = std::fs::read_to_string(env_file) {
        read(config);
    } else {
        eprintln!("Failed to read .env file. Using embedded config.");
    }
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
            let value = if value.starts_with('"') && value.ends_with('"') {
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
