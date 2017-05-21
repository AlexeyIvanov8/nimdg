
#![feature(rustc_private)]
#[macro_use]
extern crate log;
extern crate env_logger;

extern crate nimdg;
extern crate rustless;

use nimdg::data_base::DataBaseManager;
use rustless::json::JsonValue;
use std::str::FromStr;
use nimdg::data_base::meta::TableDescriptionView;

mod data_base_test;

#[test]
fn put_test() {
    env_logger::init().unwrap();
    let data_base_manager: DataBaseManager = DataBaseManager::new().unwrap();
    let table_desc = rustless::json::JsonValue::from_str("{
        \"data\": {
            \"name\": \"Client\", 
            \"key\": {
                \"fields\": {
                    \"id\": \"u64\"
                } 
            },
            \"value\": {
                \"fields\": {
                    \"full_name\": \"String\",
                    \"age\": \"u64\"
                }
            }
        } 
    }");
    let table_desc_json = table_desc.unwrap();
    info!("Table desc json = {}", table_desc_json);
    let table_desc_view = TableDescriptionView::from_json(&table_desc_json);
    info!("Table desc view = {:?}", table_desc_view);
    data_base_manager.add_table(table_desc_view);
}
