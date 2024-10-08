use serde::{Deserialize, Serialize};
use serde_json::Value;
use flate2::{write::GzEncoder, Compression};
use std::collections::HashMap;
use std::io::Write;
use wasm_bindgen::prelude::*;

// Enable better error messages in JavaScript console
extern crate console_error_panic_hook;
use std::panic;

#[derive(Serialize, Deserialize, Debug)]
struct Record {
    key: Option<Vec<u8>>,  // Kafka keys can be optional
    value: Vec<u8>,
    headers: Option<HashMap<String, Vec<u8>>>,
}

// Initialize panic hook for better error messages
#[wasm_bindgen(start)]
pub fn main_js() {
    panic::set_hook(Box::new(console_error_panic_hook::hook));
}

/// Transforms a JSON string by filtering and compressing it.
///
/// # Arguments
///
/// * `input` - A JSON string representing the record.
///
/// # Returns
///
/// * A compressed JSON string if transformation is successful.
/// * An empty string if the record is filtered out or an error occurs.
#[wasm_bindgen]
pub fn transform(input: &str) -> String {
    // Parse the record from input
    let record: Record = match serde_json::from_str(input) {
        Ok(rec) => rec,
        Err(_) => return "".to_string(), // Return empty string on parse error
    };

    // Parse the log message from the record value
    let mut log_entry: Value = match serde_json::from_slice(&record.value) {
        Ok(entry) => entry,
        Err(_) => return "".to_string(),
    };

    // Filtering Logic: discard DEBUG and INFO logs
    if let Some(level) = log_entry.get("level").and_then(Value::as_str) {
        if level == "DEBUG" || level == "INFO" {
            return "".to_string(); // Indicate filtering
        }
    }

    // Remove unnecessary fields
    if let Some(obj) = log_entry.as_object_mut() {
        obj.remove("unnecessaryField");
    } else {
        return "".to_string();
    }

    // Serialize the modified log entry back to JSON
    let modified_log_entry = match serde_json::to_vec(&log_entry) {
        Ok(vec) => vec,
        Err(_) => return "".to_string(),
    };

    // Compress the log entry using GZIP
    let mut compressed_log = Vec::new();
    let mut encoder = GzEncoder::new(&mut compressed_log, Compression::default());
    if encoder.write_all(&modified_log_entry).is_err() {
        return "".to_string();
    }
    if encoder.finish().is_err() {
        return "".to_string();
    }

    // Create a new Record with compressed data and gzip header
    let transformed_record = Record {
        key: record.key,
        value: compressed_log,
        headers: Some(HashMap::from([("Content-Encoding".to_string(), b"gzip".to_vec())])),
    };

    // Serialize the transformed record back to JSON
    match serde_json::to_string(&transformed_record) {
        Ok(json) => json,
        Err(_) => "".to_string(),
    }
}
