use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Storage {
    pub table: HashMap<String, Vec<u8>>,
}
