
extern crate log4rs;

extern crate serde;
extern crate serde_json;

use nimdg::data_base::DataBaseManager;
use rustless::json::JsonValue;
use std::str::FromStr;
use nimdg::data_base::meta::TableDescriptionView;
use nimdg::data_base::transaction::LockMode;

use data_base_test::create_test_data_base;
use data_base_test::{IdKey, Client};

use std::fs::{File, OpenOptions};
use std::io::prelude::*;

#[test]
fn fs_test() {
    log4rs::init_file("config/log4rs.yml", Default::default());
    let client_table_name: String = String::from("Client");
    let data_base_manager = DataBaseManager::from_file(String::from("fs_test"), 10000).unwrap();
    // create_test_data_base(&data_base_manager);

    let tx_id = data_base_manager.tx_start(LockMode::Optimistic).map_err(|error| println!("Tx start error = {}", error)).unwrap();

    let key = IdKey { id: 1 };
    let client = Client {
        full_name: String::from("John Success"),
        age: 35,
    };

    let key_json = serde_json::to_value(key);
    data_base_manager.add_data(&tx_id,
                               &client_table_name,
                               &key_json,
                               &serde_json::to_value(client));
    let readed = data_base_manager.get_data(&tx_id, &client_table_name, &key_json).unwrap();
    data_base_manager.tx_stop(&tx_id).map_err(|err| println!("Tx commit error = {}", err)).unwrap();

    let another_data_base_manager = DataBaseManager::from_file(String::from("fs_test"), 10000).unwrap();
    let load_tables = another_data_base_manager.get_tables();
    info!("Load teables count = {}, tables = {:?}",
          load_tables.len(),
          load_tables);
    assert!(load_tables.len() == 1);
    assert!(load_tables.contains_key("Client"));
}
