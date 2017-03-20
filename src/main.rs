
extern crate rustc_serialize;
extern crate concurrent_hashmap;
extern crate bincode;
extern crate valico;
extern crate hyper;
#[macro_use]
extern crate iron;
#[macro_use]
extern crate rustless;
extern crate serde_json;

use std::collections::BTreeMap;

use valico::json_dsl;
use rustless::batteries::swagger;
use std::str::FromStr;

use rustless::{Application, Api, Nesting, Versioning};

mod data_base;

use self::data_base::app_extension::DataBaseExtension;
use self::data_base::meta::{EntityDescriptionView, TableDescriptionView};

// reading views from rustless json
fn read_entity_description_view(json: &BTreeMap<String, rustless::json::JsonValue>)
                                -> EntityDescriptionView {
    let fields_object = json.get("fields").unwrap().as_object().unwrap();
    let fields =
        fields_object.iter().map(|(k, v)| (k.clone(), String::from(v.as_str().unwrap()))).collect();
    // for (k, v) in fields { println!("Field {} = {}", k, v) };
    EntityDescriptionView { fields: fields }
}

fn read_table_description_view(json: &rustless::json::JsonValue) -> TableDescriptionView {
    let name = json.find("name").unwrap().as_str().unwrap();
    println!("Found cache desc with name = {}", name);
    let key = read_entity_description_view(json.find("key").unwrap().as_object().unwrap());
    let value = read_entity_description_view(json.find("value").unwrap().as_object().unwrap());
    TableDescriptionView {
        name: String::from(name),
        key: key,
        value: value,
    }
}

fn run_data_base_manager(app: &mut rustless::Application) {
    let data_base_manager = data_base::DataBaseManager::new();
    app.ext.insert::<data_base::app_extension::AppDataBase>(data_base_manager.unwrap());
}

#[derive(RustcDecodable, RustcEncodable)]
pub struct TestStruct {
    data_int: u8,
    data_str: String,
    data_vector: Vec<u8>,
}

// For show errors on client side
#[derive(Debug)]
enum ClientError {
    GettingParamsError(Vec<String>)
}

/*pub struct GettingParamsError {
    param_names: Vec<String>,
}*/

impl ClientError::GettingParamsError {
    fn get_description(&self) -> String {
        self.param_names.iter().fold(String::from("Getting params error: "),
                                     |acc, name| acc + name + ";")
    }
}

impl std::error::Error for ClientError::GettingParamsError {
    fn description(&self) -> &str {
        // let desc = self.get_description().clone();
        // &desc.as_str()
        return "";
    }
}

impl std::fmt::Display for ClientError::GettingParamsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.get_description())
    }
}

fn handle_response(Fn() -> Result<)
fn get_key_and_value
    (params: &rustless::json::JsonValue)
     -> Result<(&rustless::json::JsonValue, &rustless::json::JsonValue), String> {
    let data =
        try!(params.find("data").and_then(|data| data.as_object()).ok_or("Param data not found"));
    let key = try!(data.get("key").ok_or("Attribute key not found"));
    let value = try!(data.get("value").ok_or("Attribute value not found"));
    Ok((&key, &value))
}

fn main() {

    println!("Hello, world!");

    let api = Api::build(|api| {
        api.version("v1", Versioning::Path);
        api.prefix("api");

        api.mount(swagger::create_api("api-docs"));

        api.mount(Api::build(|cache_api| {

            cache_api.after(|client, _params| {
                client.set_status(iron::status::Status::NotFound);
                Ok(())
            });

            cache_api.get("info", |endpoint| {
                endpoint.handle(|client, _| {
                    let db_manager = client.app.get_data_base_manager();
                    db_manager.print_info();
                    // client.text("Some usefull info".to_string())
                    client.json(&db_manager.get_tables_list())
                })
            });

            cache_api.post("/tx_start", |endpoint| {
                endpoint.handle(|client, params| {
                    let db_manager = client.app.get_data_base_manager();
                    client.json(&JsonValue::U64(db_manager.tx_start()))
                })
            });

            cache_api.post("put/:table_name", |endpoint| {
                endpoint.params(|params| {
                    params.req_typed("table_name", json_dsl::string());
                    params.req_typed("tx_id", json_dsl::u64());
                    params.req("data", |_| {})
                });
                endpoint.handle(|client, params| {
                    match get_key_and_value(params) {
                        Ok((key, value)) => {
                            let db_manager = client.app.get_data_base_manager();
                            let tx_id = params.find().map(|value| value.as_str());
                            let res = db_manager.add_data(&String::from(params.find("table_name")
                                                              .unwrap()
                                                              .as_str()
                                                              .unwrap()),
                                                          &key,
                                                          &value);
                            match res {
                                Ok(_) => client.text("Done".to_string()),
                                Err(err) => client.text(err.to_string()),
                            }
                        }
                        Err(message) => client.text(message),
                    }
                })
            });

            cache_api.get("get/:table_name", |endpoint| {
                endpoint.params(|params| {
                    params.req_typed("table_name", json_dsl::string());
                    params.req("key", |_| {})
                });

                endpoint.handle(|client, params| {
                    let table_name = params.find("table_name")
                        .and_then(|table_name| table_name.as_str());
                    let key = params.find("key")
                        .and_then(|key| key.as_str())
                        .map(|key| rustless::json::JsonValue::from_str(key));
                    match key.and_then(|key| table_name.map(|table_name| (table_name, key))) {
                        Some((table_name, key)) => {
                            match key {
                                Ok(key) => {
                                    let db_manager = client.app.get_data_base_manager();
                                    let value =
                                        db_manager.get_data(&String::from(table_name), &key);
                                    match value {
                                        Ok(value) => {
                                            match value {
                                                Some(value) => client.json(&value),
                                                None => {
                                                    client.text("Entity with key ".to_string() +
                                                                key.to_string().as_str() +
                                                                " not found")
                                                }
                                            }
                                        }
                                        Err(message) => client.text(message.to_string()),
                                    }
                                }
                                Err(message) => client.error(message),
                            }
                        }
                        None => {
                            client.error(GettingParamsError {
                                param_names: vec![String::from("table_name"), String::from("key")],
                            })
                        }

                    }
                })
            });

            cache_api.namespace("meta", |meta_ns| {
                meta_ns.post("table", |endpoint| {
                    println!("Table update");
                    endpoint.desc("Update description");
                    endpoint.params(|params| {
                        params.req("data", |data| {
                            data.desc("Data of cache structure");
                            data.schema(|cache_desc| {
                                cache_desc.object();
                                cache_desc.properties(|props| {
                                    props.insert("name", |name| {
                                        name.string();
                                    });
                                    props.insert("key", |key| {
                                        key.object();
                                    });
                                });
                            })
                        })
                    });

                    endpoint.handle(|mut client, _params| {
                        let cache_desc = _params.find("data").unwrap();
                        let table_desc = read_table_description_view(cache_desc);
                        match client.app.get_data_base_manager().add_table(table_desc) {
                            Ok(name) => {
                                client.set_status(rustless::server::status::StatusCode::Ok);
                                client.text("Table with name ".to_string() + name.as_str() +
                                            " succefully added")
                            }
                            Err(message) => {
                                client.set_status(rustless::server::status::StatusCode::BadRequest);
                                client.text(message)
                            }
                        }
                    })
                });

                meta_ns.get("table/:name", |endpoint| {
                    endpoint.params(|params| params.req_typed("name", json_dsl::string()));

                    endpoint.handle(|client, params| {
                        match params.find("name")
                            .and_then(|name| name.as_str()) {
                            Some(name) => {
                                let table_desc = client.app
                                    .get_data_base_manager()
                                    .get_table(&String::from(name));
                                match table_desc {
                                    Some(table_desc) => client.json(&table_desc),
                                    None => {
                                        client.text(String::from("Table ".to_string() + name +
                                                                 " not found"))
                                    }
                                }
                            }
                            None => client.text("Parameter table name not found.".to_string()),
                        }
                    })
                });
            });
        }));
    });

    let mut app = Application::new(api);
    run_data_base_manager(&mut app);

    swagger::enable(&mut app,
                    swagger::Spec {
                        info: swagger::Info {
                            title: "Example API".to_string(),
                            description: Some("Simple API to demonstration".to_string()),
                            contact: Some(swagger::Contact {
                                name: "SKN".to_string(),
                                url: Some("http://panferov.me".to_string()),
                                ..std::default::Default::default()
                            }),
                            license: Some(swagger::License {
                                name: "MIT".to_string(),
                                url: "http://opensource.org/licenses/MIT".to_string(),
                            }),
                            ..std::default::Default::default()
                        },
                        host: Some("localhost:4300".to_string()),
                        ..std::default::Default::default()
                    });

    iron::Iron::new(app).http("localhost:4300").unwrap();
    // Iron::new(|request: &mut Request| {
    // Ok(match request.method {
    // method::Get => Response::with((status::NotImplemented, "Method get not supported yet")),
    // method::Put => {
    // let mut buffer = String::new();
    // request.body.read_to_string(&mut buffer);
    // println!("Getting string = {}", buffer);
    // Response::with((status::Ok, "Getting success"))
    // },
    // _ => Response::with((status::NotImplemented, "This method not implemented yet")),
    // })
    // }).http("localhost:4300").unwrap();
}
