use std::io;

use io::BufRead;

use io::BufWriter;
use io::Write;

use std::collections::BTreeSet;

pub use serde_json;

use serde_json::Map;
use serde_json::Value;

pub fn objects2filtered<I, F>(jsons: I, filter: F) -> impl Iterator<Item = Map<String, Value>>
where
    I: Iterator<Item = Map<String, Value>>,
    F: Fn(Map<String, Value>) -> Option<Map<String, Value>>,
{
    jsons.filter_map(filter)
}

pub fn nop_filter(original: Map<String, Value>) -> Option<Map<String, Value>> {
    Some(original)
}

/// Creates a new filter using the specified selector.
pub fn value_select_filter<S>(
    select_value_by_key: S,
) -> impl Fn(Map<String, Value>) -> Option<Map<String, Value>>
where
    S: Fn(&str) -> bool,
{
    move |mut original: Map<String, Value>| {
        original.retain(|key, _val| select_value_by_key(key));
        Some(original)
    }
}

/// Creates a new filter using the specified remover.
pub fn value_remove_filter<S>(
    remove_value_by_key: S,
) -> impl Fn(Map<String, Value>) -> Option<Map<String, Value>>
where
    S: Fn(&str) -> bool,
{
    move |mut original: Map<String, Value>| {
        original.retain(|key, _val| !remove_value_by_key(key));
        Some(original)
    }
}

/// Creates a new selector using the specified key set.
pub fn set2value_selector(s: BTreeSet<String>) -> impl Fn(&str) -> bool {
    move |key: &str| s.contains(key)
}

/// Creates a new remover using the specified key set.
pub fn set2value_remover(s: BTreeSet<String>) -> impl Fn(&str) -> bool {
    move |key: &str| s.contains(key)
}

/// Creates a new filter using the specified keep filter.
pub fn value_filter<F>(keep_filter: F) -> impl Fn(Map<String, Value>) -> Option<Map<String, Value>>
where
    F: Fn(&Map<String, Value>) -> bool,
{
    move |original: Map<String, Value>| {
        let keep: bool = keep_filter(&original);
        keep.then_some(original)
    }
}

/// Creates a new filter using the specified filters.
pub fn filters2filter<F, G>(f: F, g: G) -> impl Fn(Map<String, Value>) -> Option<Map<String, Value>>
where
    F: Fn(Map<String, Value>) -> Option<Map<String, Value>>,
    G: Fn(Map<String, Value>) -> Option<Map<String, Value>>,
{
    move |original: Map<String, Value>| {
        let omap: Option<Map<String, Value>> = f(original);
        omap.and_then(&g)
    }
}

pub fn blobs2objects<I, P>(blobs: I, parser: P) -> impl Iterator<Item = Map<String, Value>>
where
    I: Iterator<Item = Vec<u8>>,
    P: Fn(Vec<u8>) -> Map<String, Value>,
{
    blobs.map(parser)
}

pub fn reader2jsonl<R>(rdr: R) -> impl Iterator<Item = Vec<u8>>
where
    R: BufRead,
{
    let lines = rdr.split(b'\n');
    lines.map_while(Result::ok)
}

pub fn stdin2jsonl() -> impl Iterator<Item = Vec<u8>> {
    reader2jsonl(io::stdin().lock())
}

pub fn maps2writer<I, W>(maps: I, mut wtr: W) -> Result<(), io::Error>
where
    I: Iterator<Item = Map<String, Value>>,
    W: Write,
{
    for m in maps {
        serde_json::to_writer(&mut wtr, &m)?;
    }
    Ok(())
}

pub fn maps2stdout<I>(maps: I) -> Result<(), io::Error>
where
    I: Iterator<Item = Map<String, Value>>,
{
    let o = io::stdout();
    let mut ol = o.lock();
    let bw = BufWriter::new(&mut ol);
    maps2writer(maps, bw)?;
    ol.flush()
}

/// Gets jsons from stdin and outputs filtered jsons to stdout.
pub fn stdin2jsons2filtered2stdout<P, F>(parser: P, filter: F) -> Result<(), io::Error>
where
    P: Fn(Vec<u8>) -> Map<String, Value>,
    F: Fn(Map<String, Value>) -> Option<Map<String, Value>>,
{
    // Iterator<Item=Vec<u8>>
    let jsons = stdin2jsonl();

    // Iterator<Item=Map<String, Value>>
    let parsed = blobs2objects(jsons, parser);

    // Iterator<Item=Map<String, Value>>
    let filtered = objects2filtered(parsed, filter);

    maps2stdout(filtered)
}

pub fn parser_default(json: Vec<u8>) -> Map<String, Value> {
    serde_json::from_slice(&json).unwrap_or_default()
}

/// Stdin -> jsons -> filtered -> stdout using the default parser.
pub fn stdin2jsons2filtered2stdout_default<F>(filter: F) -> Result<(), io::Error>
where
    F: Fn(Map<String, Value>) -> Option<Map<String, Value>>,
{
    stdin2jsons2filtered2stdout(parser_default, filter)
}
