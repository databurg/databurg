#[allow(unused_imports)]
use log::{debug, info};
use std::path::PathBuf;
use walkdir::WalkDir;

pub async fn get_files(base: Option<&str>) -> Vec<PathBuf> {
    let mut files_vec = vec![];
    for e in WalkDir::new(base.unwrap_or("."))
        .into_iter()
        .filter_map(|e| e.ok())
    {
        if e.metadata().unwrap().is_file() {
            files_vec.push(e.path().to_path_buf());
        }
    }
    files_vec
}
