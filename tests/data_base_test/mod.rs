
use nimdg::data_base::DataBaseManager;
use nimdg::data_base::meta::TableDescriptionView;
use rustless::json::JsonValue;
use std::str::FromStr;

mod transaction_test;

#[derive(Serialize, Deserialize)]
pub struct IdKey {
    pub id: u64,
}

#[derive(Serialize, Deserialize)]
pub struct Client {
    pub full_name: String,
    pub age: u64,
}

pub static CLIENT_TABLE_NAME: &'static str = "Client";

pub fn create_test_data_base() -> DataBaseManager {
    let client_table_name: String = String::from(CLIENT_TABLE_NAME);
    let data_base_manager: DataBaseManager = DataBaseManager::new().unwrap();
    let table_desc = JsonValue::from_str("{
        \"name\": \"Client\",
        \"key\": {
            \"fields\": {
                \"id\": {\"type_name\": \"u64\"}
            }
        },
        \"value\": {
            \"fields\": {
                \"full_name\":  {\"type_name\": \"string\"},
                \"age\":  {\"type_name\": \"u64\"}
             }
        }
    }");

    let table_desc_json = table_desc.unwrap();
    info!("Table desc json = {}", table_desc_json);
    let table_desc_view_res = TableDescriptionView::from_json(&table_desc_json);
    let table_desc_view = table_desc_view_res.unwrap();
    info!("Table desc view = {:?}", table_desc_view);
    data_base_manager.add_table(table_desc_view);
    info!("Added table {}",
          data_base_manager.get_table_json(&client_table_name).unwrap());

    data_base_manager
}
