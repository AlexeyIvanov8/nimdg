
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

#[test]
fn rollback_test() {
    log4rs::init_file("config/log4rs.yml", Default::default());

    let client_table_name: String = String::from("Client");
    let data_base_manager = create_test_data_base();

    let key_one = JsonValue::from_str("{\"id\": 2 }").unwrap();
    let value_one = JsonValue::from_str("{
        \"full_name\": \"John Doe\",
        \"age\": 23
    }")
        .unwrap();

    info!("Begin rollback test");
    let tx_id = data_base_manager.tx_start(LockMode::Pessimistic).unwrap();
    let res = data_base_manager.add_data(&tx_id, &client_table_name, &key_one, &value_one);
    info!("Add data = {:?}", res);
    assert!(res.is_ok());
    let stored_value_one = data_base_manager.get_data(&tx_id, &client_table_name, &key_one);
    info!("Getting uncommited data = {:?}", stored_value_one);
    assert!(stored_value_one.is_ok());
    assert!(stored_value_one.unwrap().is_some());

    data_base_manager.tx_rollback(&tx_id);
    info!("Tx is rollback {}", tx_id);

    let tx_id_2 = data_base_manager.tx_start(LockMode::Pessimistic).unwrap();
    let stored_value_one_after = data_base_manager.get_data(&tx_id_2, &client_table_name, &key_one);
    info!("Getting uncommited data after rollback = {:?}",
          stored_value_one_after);
    assert!(stored_value_one_after.is_ok());
    assert!(stored_value_one_after.unwrap().is_none());
    data_base_manager.tx_stop(&tx_id_2);
}

#[test]
fn optimistic_tx_success_test() {
    log4rs::init_file("config/log4rs.yml", Default::default());
    let client_table_name: String = String::from("Client");
    let data_base_manager = create_test_data_base();
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
}

#[test]
fn optimistic_tx_fail_test() {
    log4rs::init_file("config/log4rs.yml", Default::default());
    let client_table_name: String = String::from("Client");
    let data_base_manager = create_test_data_base();

    let key = IdKey { id: 1 };
    let client = Client {
        full_name: String::from("John Success"),
        age: 35,
    };

    let key_json = serde_json::to_value(key);

    let tx_id = data_base_manager.tx_start(LockMode::Optimistic).map_err(|error| println!("Tx start error = {}", error)).unwrap();
    data_base_manager.add_data(&tx_id,
                               &client_table_name,
                               &key_json,
                               &serde_json::to_value(client));
    let readed = data_base_manager.get_data(&tx_id, &client_table_name, &key_json).unwrap();
    data_base_manager.tx_stop(&tx_id).map_err(|err| println!("Tx commit error = {}", err)).unwrap();

    let client_update = Client {
        full_name: String::from("John Fail"),
        age: 45,
    };
    let client_update_json = &serde_json::to_value(client_update);

    let tx_id_fail = data_base_manager.tx_start(LockMode::Optimistic).map_err(|error| println!("Tx start error = {}", error)).unwrap();
    let tx_id_first_lock = data_base_manager.tx_start(LockMode::Optimistic).map_err(|error| println!("Tx start error = {}", error)).unwrap();
    data_base_manager.add_data(&tx_id_first_lock,
                  &client_table_name,
                  &key_json,
                  &client_update_json)
        .unwrap();
    let fail_result = data_base_manager.add_data(&tx_id_fail,
                                                 &client_table_name,
                                                 &key_json,
                                                 client_update_json);
    info!("optimistic fail result = {:?}", fail_result);
    assert!(!fail_result.is_ok());
}
