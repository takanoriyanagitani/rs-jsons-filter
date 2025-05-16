use std::process::ExitCode;

use std::io;

use std::collections::BTreeSet;

use rs_jsons_filter::serde_json;

use rs_jsons_filter::filters2filter;
use rs_jsons_filter::set2value_selector;
use rs_jsons_filter::stdin2jsons2filtered2stdout_default;
use rs_jsons_filter::value_filter;
use rs_jsons_filter::value_remove_filter;

use serde_json::Map;
use serde_json::Value;

fn remove_blob(blob_name: String) -> impl Fn(&str) -> bool {
    set2value_selector(BTreeSet::from_iter(vec![blob_name]))
}

fn only_level(
    severity_key: String,
    severity_value: String,
) -> impl Fn(Map<String, Value>) -> Option<Map<String, Value>> {
    value_filter(move |original: &Map<String, Value>| {
        let level_value: Option<&Value> = original.get(&severity_key);
        let level_string: Option<&String> = level_value.and_then(|v| match v {
            Value::String(s) => Some(s),
            _ => None,
        });
        let level_str: &str = match level_string {
            Some(s) => s.as_str(),
            _ => "",
        };
        severity_value.eq(level_str)
    })
}

fn remove_filter() -> impl Fn(Map<String, Value>) -> Option<Map<String, Value>> {
    value_remove_filter(remove_blob("blob".into()))
}

fn final_filter() -> impl Fn(Map<String, Value>) -> Option<Map<String, Value>> {
    filters2filter(
        remove_filter(),
        only_level("severity".into(), "error".into()),
    )
}

fn stdin2jsons2filtered2stdout() -> Result<(), io::Error> {
    stdin2jsons2filtered2stdout_default(final_filter())
}

fn main() -> ExitCode {
    stdin2jsons2filtered2stdout()
        .map(|_| ExitCode::SUCCESS)
        .unwrap_or_else(|e| {
            eprintln!("{e}");
            ExitCode::FAILURE
        })
}
