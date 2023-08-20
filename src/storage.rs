use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Storage {
    table: HashMap<String, String>,
}
