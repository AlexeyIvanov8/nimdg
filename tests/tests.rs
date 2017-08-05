
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

fn create_test_data_base() -> DataBaseManager {
    let client_table_name: String = String::from("Client");
    let data_base_manager: DataBaseManager = DataBaseManager::new().unwrap();
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

    let table_desc_json = table_desc.unwrap();
    info!("Table desc json = {}", table_desc_json);
    let table_desc_view_res = TableDescriptionView::from_json(&table_desc_json);
    let table_desc_view = table_desc_view_res.unwrap();
    info!("Table desc view = {:?}", table_desc_view);
    data_base_manager.add_table(table_desc_view);
    info!("Added table {}", data_base_manager.get_table_json(&client_table_name).unwrap());

    data_base_manager
}

#[test]
fn put_test() {
    log4rs::init_file("config/log4rs.yml", Default::default());
    
    let client_table_name: String = String::from("Client");

    let data_base_manager = create_test_data_base();

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

    let tx_id = data_base_manager.tx_start().unwrap();
    let none_data = data_base_manager.get_data(&tx_id, &client_table_name, &key_one).unwrap();
    assert!(none_data.is_none());
    data_base_manager.add_data(&tx_id, &client_table_name, &key_one, &value_one).unwrap();
    let res_value = data_base_manager.get_data(&tx_id, &client_table_name, &key_one).unwrap();
    info!("After insert one found value = {:?}, tx id = {}", res_value, tx_id);
    data_base_manager.tx_stop(&tx_id).unwrap();

    let tx_id = data_base_manager.tx_start().unwrap();
    data_base_manager.add_data(&tx_id, &client_table_name, &key_one, &value_two);
    let res_value = data_base_manager.get_data(&tx_id, &client_table_name, &key_one).unwrap().unwrap();
    info!("After insert two found value = {}, tx id = {}", res_value, tx_id);
    data_base_manager.tx_stop(&tx_id);

    let tx_id_1 = data_base_manager.tx_start().unwrap();
    data_base_manager.add_data(&tx_id_1, &client_table_name, &key_two, &value_two).unwrap();

    let tx_id_2 = data_base_manager.tx_start().unwrap();
    let res_in_tx_1 = data_base_manager.get_data(&tx_id_1, &client_table_name, &key_two).unwrap().unwrap();
    info!("Value in tx 1 id = {}, value = {}", tx_id_1, res_in_tx_1);
    let res_in_tx_2 = data_base_manager.get_data(&tx_id_2, &client_table_name, &key_two);
    info!("Value in tx 2 id = {}, value = {:?}", tx_id_2, res_in_tx_2);

    data_base_manager.tx_stop(&tx_id_1).unwrap();
    let res_in_tx_2 = data_base_manager.get_data(&tx_id_2, &client_table_name, &key_two).unwrap().unwrap();
    info!("Value in tx 2 after commit tx 1 id = {}, value = {}", tx_id_2, res_in_tx_2);
    data_base_manager.tx_stop(&tx_id_2).unwrap();
}

#[test]
fn rollback_test() {
    log4rs::init_file("config/log4rs.yml", Default::default());

    let client_table_name: String = String::from("Client");
    let data_base_manager = create_test_data_base();

    let key_one = rustless::json::JsonValue::from_str("{\"id\": 2 }").unwrap();
    let value_one = rustless::json::JsonValue::from_str("{
        \"full_name\": \"John Doe\",
        \"age\": 23
    }").unwrap();
    
    info!("Begin rollback test");
    let tx_id = data_base_manager.tx_start().unwrap();
    let res = data_base_manager.add_data(&tx_id, &client_table_name, &key_one, &value_one);
    info!("Add data = {:?}", res);
    assert!(res.is_ok());
    let stored_value_one = data_base_manager.get_data(&tx_id, &client_table_name, &key_one);
    info!("Getting uncommited data = {:?}", stored_value_one);
    assert!(stored_value_one.is_ok());
    assert!(stored_value_one.unwrap().is_some());
    
    data_base_manager.tx_rollback(&tx_id);
    info!("Tx is rollback {}", tx_id);

    let tx_id_2 = data_base_manager.tx_start().unwrap();
    let stored_value_one_after = data_base_manager.get_data(&tx_id_2, &client_table_name, &key_one);
    info!("Getting uncommited data after rollback = {:?}", stored_value_one_after);
    assert!(stored_value_one_after.is_ok());
    assert!(stored_value_one_after.unwrap().is_none());
    data_base_manager.tx_stop(&tx_id_2);
}

#[test]
fn date_test() {
    log4rs::init_file("config/log4rs.yml", Default::default());

    let data_base_manager: DataBaseManager = DataBaseManager::new().unwrap();
    let table_desc = rustless::json::JsonValue::from_str("{
        \"name\": \"Times\", 
        \"key\": {
            \"fields\": {
                \"id\": \"u64\"
            } 
        },
        \"value\": {
            \"fields\": {
                \"date\": \"date\"
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
            info!("Added table {}", data_base_manager.get_table_json(&String::from("Times")).unwrap());

            let key = rustless::json::JsonValue::from_str("{\"id\": 2 }").unwrap();
            let value = rustless::json::JsonValue::from_str("{
                \"date\": \"02-03-2016\"
            }").unwrap();
            println!("prepare datas");
            let tx_id = data_base_manager.tx_start().unwrap();
            info!("Begin insert date value = {}", value);
            data_base_manager.add_data(&tx_id, &String::from("Times"), &key, &value).unwrap();
            data_base_manager.tx_stop(&tx_id);
            
            let tx_id = data_base_manager.tx_start().unwrap();
            let after = data_base_manager.get_data(&tx_id, &String::from("Times"), &key).unwrap().unwrap();
            info!("After date = {}", after);
            data_base_manager.tx_stop(&tx_id);
        },
        Err(error) => info!("Error ={}", error)
    }

}
