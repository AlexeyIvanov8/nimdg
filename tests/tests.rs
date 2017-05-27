
#![feature(rustc_private)]
#[macro_use]
extern crate log;
//extern crate env_logger;
extern crate log4rs;

extern crate nimdg;
extern crate rustless;

use nimdg::data_base::DataBaseManager;
use rustless::json::JsonValue;
use std::str::FromStr;
use nimdg::data_base::meta::TableDescriptionView;

mod data_base_test;

#[test]
fn put_test() {
    log4rs::init_file("config/log4rs.yml", Default::default()).unwrap();
    //env_logger::init().unwrap();
    let data_base_manager: DataBaseManager = DataBaseManager::new().unwrap();
    let client_table_name = String::from("Client");
    let table_desc = rustless::json::JsonValue::from_str("{
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
    }");

    let key_one = rustless::json::JsonValue::from_str("{\"id\": 2 }").unwrap();
    let value_one = rustless::json::JsonValue::from_str("{
        \"full_name\": \"John Doe\",
        \"age\": 23
    }").unwrap();

    let key_two = rustless::json::JsonValue::from_str("{\"id\": 3 }").unwrap();
    let value_two = rustless::json::JsonValue::from_str("{
        \"full_name\": \"David K\",
        \"age\": 45
    }").unwrap();

    let table_desc_json = table_desc.unwrap();
    info!("Table desc json = {}", table_desc_json);
    let table_desc_view_res = TableDescriptionView::from_json(&table_desc_json);
    let table_desc_view = table_desc_view_res.unwrap();
    info!("Table desc view = {:?}", table_desc_view);
    data_base_manager.add_table(table_desc_view);
    info!("Added table {}", data_base_manager.get_table(&client_table_name).unwrap());

    let tx_id = data_base_manager.tx_start().unwrap();
    let none_data = data_base_manager.get_data(&tx_id, &client_table_name, &key_one).unwrap();
    assert!(none_data.is_none());
    data_base_manager.add_data(&tx_id, &client_table_name, &key_one, &value_one).unwrap();
    let res_value = data_base_manager.get_data(&tx_id, &client_table_name, &key_one).unwrap().unwrap();
    info!("After insert one found value = {}, tx id = {}", res_value, tx_id);
    data_base_manager.tx_stop(&tx_id);

    let tx_id = data_base_manager.tx_start().unwrap();
    data_base_manager.add_data(&tx_id, &client_table_name, &key_one, &value_two);
    let res_value = data_base_manager.get_data(&tx_id, &client_table_name, &key_one).unwrap().unwrap();
    info!("After insert two found value = {}, tx id = {}", res_value, tx_id);
    data_base_manager.tx_stop(&tx_id);
}
