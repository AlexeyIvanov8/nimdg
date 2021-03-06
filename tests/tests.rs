#![feature(rustc_private, pub_restricted, field_init_shorthand)]
#[macro_use]
extern crate log;
// extern crate env_logger;
extern crate log4rs;

extern crate nimdg;
extern crate rustless;

extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

use nimdg::data_base::DataBaseManager;
use rustless::json::JsonValue;
use std::str::FromStr;
use nimdg::data_base::meta::TableDescriptionView;
use nimdg::data_base::transaction::LockMode;

mod data_base_test;

use data_base_test::create_test_data_base;
use data_base_test::{IdKey, Client};

fn fill_test_data_base(data_base_manager: &DataBaseManager) {}

#[test]
fn put_test() {
    log4rs::init_file("config/log4rs.yml", Default::default());

    let client_table_name: String = String::from("Client");

    let data_base_manager = create_test_data_base();

    let key_one = JsonValue::from_str("{\"id\": 2 }").unwrap();
    let value_one = JsonValue::from_str("{
        \"full_name\": \"John Doe\",
        \"age\": 23
    }")
        .unwrap();

    let key_two = JsonValue::from_str("{\"id\": 3 }").unwrap();
    let value_two = JsonValue::from_str("{
        \"full_name\": \"David K\",
        \"age\": 45
    }")
        .unwrap();

    let tx_id = data_base_manager.tx_start(LockMode::Pessimistic).unwrap();
    let none_data = data_base_manager.get_data(&tx_id, &client_table_name, &key_one).unwrap();
    assert!(none_data.is_none());
    data_base_manager.add_data(&tx_id, &client_table_name, &key_one, &value_one).unwrap();
    let res_value = data_base_manager.get_data(&tx_id, &client_table_name, &key_one).unwrap();
    info!("After insert one found value = {:?}, tx id = {}",
          res_value,
          tx_id);
    data_base_manager.tx_stop(&tx_id).unwrap();

    let tx_id = data_base_manager.tx_start(LockMode::Pessimistic).unwrap();
    data_base_manager.add_data(&tx_id, &client_table_name, &key_one, &value_two);
    let res_value = data_base_manager.get_data(&tx_id, &client_table_name, &key_one).unwrap().unwrap();
    info!("After insert two found value = {}, tx id = {}",
          res_value,
          tx_id);
    data_base_manager.tx_stop(&tx_id);

    let tx_id_1 = data_base_manager.tx_start(LockMode::Pessimistic).unwrap();
    data_base_manager.add_data(&tx_id_1, &client_table_name, &key_two, &value_two).unwrap();

    let tx_id_2 = data_base_manager.tx_start(LockMode::Pessimistic).unwrap();
    let res_in_tx_1 = data_base_manager.get_data(&tx_id_1, &client_table_name, &key_two).unwrap().unwrap();
    info!("Value in tx 1 id = {}, value = {}", tx_id_1, res_in_tx_1);
    let res_in_tx_2 = data_base_manager.get_data(&tx_id_2, &client_table_name, &key_two);
    info!("Value in tx 2 id = {}, value = {:?}", tx_id_2, res_in_tx_2);

    data_base_manager.tx_stop(&tx_id_1).unwrap();
    let res_in_tx_2 = data_base_manager.get_data(&tx_id_2, &client_table_name, &key_two).unwrap().unwrap();
    info!("Value in tx 2 after commit tx 1 id = {}, value = {}",
          tx_id_2,
          res_in_tx_2);
    data_base_manager.tx_stop(&tx_id_2).unwrap();
}

#[test]
fn date_test() {
    log4rs::init_file("config/log4rs.yml", Default::default());

    let data_base_manager: DataBaseManager = DataBaseManager::new().unwrap();
    let table_desc = JsonValue::from_str("{
        \"name\": \"Times\",
        \"key\": {
            \"fields\": {
                \"id\": \
                                          \"u64\"
            }
        },
        \"value\": {
            \"fields\": {
                \
                                          \"date\": \"date\"
                \"date_time\": \"date_time\"
             }
        }
    }");

    match table_desc {
        Ok(table_desc_json) => {
            info!("***************Table desc json = {}", table_desc_json);
            let table_desc_view_res = TableDescriptionView::from_json(&table_desc_json);
            let table_desc_view = table_desc_view_res.unwrap();
            info!("Table desc view = {:?}", table_desc_view);
            data_base_manager.add_table(table_desc_view).map_err(|error| info!("Error add table {}", error));
            println!("add table");
            info!("Added table {}",
                  data_base_manager.get_table_json(&String::from("Times")).unwrap());

            let key = JsonValue::from_str("{\"id\": 2 }").unwrap();
            let value = JsonValue::from_str("{
                \"date\": \"2016-02-03\",
                \"date_time\": \
                                             \"2017-05-21T13:41:00+03:00\"
            }")
                .unwrap();
            println!("prepare datas");
            let tx_id = data_base_manager.tx_start(LockMode::Pessimistic).unwrap();
            info!("Begin insert date value = {}", value);
            data_base_manager.add_data(&tx_id, &String::from("Times"), &key, &value).unwrap();
            data_base_manager.tx_stop(&tx_id);

            let tx_id = data_base_manager.tx_start(LockMode::Pessimistic).unwrap();
            let after = data_base_manager.get_data(&tx_id, &String::from("Times"), &key).unwrap().unwrap();
            info!("After date = {}", after);
            data_base_manager.tx_stop(&tx_id);
        }
        Err(error) => info!("Error ={}", error),
    }

}

#[test]
fn get_list_test() {
    log4rs::init_file("config/log4rs.yml", Default::default());

    let client_table_name: String = String::from("Client");
    let data_base_manager = create_test_data_base();

    let tx_id = data_base_manager.tx_start(LockMode::Pessimistic).map_err(|err| println!("Tx start error = {}", err)).unwrap();
    for i in 1..100 {
        let key = IdKey { id: i };
        let value = Client {
            full_name: String::from(format!("TestName{}", i)),
            age: 25 + i,
        };

        let key_json = serde_json::to_value(key);
        let value_json = serde_json::to_value(value);
        data_base_manager.add_data(&tx_id, &client_table_name, &key_json, &value_json);
    }
    data_base_manager.tx_stop(&tx_id).map_err(|err| println!("Tx commit error = {}", err)).unwrap();

    get_and_print_list_entities(&data_base_manager, &client_table_name);
    get_and_print_list_entities(&data_base_manager, &client_table_name);
}

fn get_and_print_list_entities(data_base_manager: &DataBaseManager, client_table_name: &String) {
    let tx_id2 = data_base_manager.tx_start(LockMode::Pessimistic).map_err(|err| println!("Tx start error = {}", err)).unwrap();
    let list_5 = data_base_manager.get_list(tx_id2.clone(), client_table_name, 0, 5).map_err(|err| println!("Failed get list = {}", err)).unwrap();
    info!("Found {} of 0 to 5 elements", list_5.len());
    for pair in list_5 {
        info!("  05:{}", pair);
    }
    data_base_manager.tx_stop(&tx_id2).map_err(|err| println!("Tx commit error = {}", err)).unwrap();
}
