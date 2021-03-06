#![feature(rustc_private)]
#![crate_name="nimdg"]

#[macro_use]
extern crate log;
// extern crate env_logger;
extern crate log4rs;

extern crate concurrent_hashmap;
extern crate bincode;
extern crate valico;
extern crate iron;
extern crate rustless;

use std::fmt::Display;

use valico::json_dsl;
use rustless::batteries::swagger;

use rustless::{Application, Api, Nesting, Versioning};
use rustless::framework::client::{Client, ClientResult};
use rustless::json::JsonValue;

pub mod data_base;

use self::data_base::app_extension::DataBaseExtension;
use self::data_base::meta::TableDescriptionView;
use self::data_base::transaction::LockMode;

use std::str::FromStr;

fn run_data_base_manager(app: &mut rustless::Application) {
    let data_base_manager = data_base::DataBaseManager::new();
    app.ext.insert::<data_base::app_extension::AppDataBase>(data_base_manager.unwrap());
}

// For show errors on client side
#[derive(Debug, Clone)]
enum ClientErrorType {
    GettingParamsError(Vec<String>),
    CommonError(String),
}

#[derive(Debug)]
struct ClientError {
    error_type: ClientErrorType,
    description: String,
}

impl ClientError {
    fn new(error_type: ClientErrorType) -> ClientError {
        ClientError {
            error_type: error_type.clone(),
            description: ClientError::get_description(&error_type),
        }
    }

    fn from_display(display: &Display) -> ClientError {
        let description = format!("{}", display);
        ClientError {
            error_type: ClientErrorType::CommonError(description.clone()),
            description: description.clone(),
        }
    }

    fn get_description(error_type: &ClientErrorType) -> String {
        match *error_type {
            ClientErrorType::GettingParamsError(ref param_names) => {
                param_names.iter()
                    .fold(String::from("Getting params error: "),
                          |acc, name| acc + name + ";")
            }
            ClientErrorType::CommonError(ref message) => message.clone(),
        }
    }
}

impl std::error::Error for ClientError {
    fn description(&self) -> &str {
        self.description.as_str()
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

macro_rules! client_error {
    ($message:expr) => (ClientError::new(ClientErrorType::CommonError( format!("Error: {}", $message) )))
}

fn handle_response<'a, F>(mut client: Client<'a>, handler: F) -> ClientResult<'a>
    where F: Fn(&mut Client<'a>) -> Result<rustless::json::JsonValue, ClientError>
{
    match handler(&mut client) {
        Ok(res) => client.json(&res),
        Err(error) => {
            client.internal_server_error();
            client.json(&JsonValue::String(error.description))
            // client.error(error)
        } //rustless::ErrorResponse{ error: Box::new(error), response: None })
    }
}

fn get_key_and_value(params: &rustless::json::JsonValue) -> Result<(&rustless::json::JsonValue, &rustless::json::JsonValue), String> {
    let data = try!(params.find("data").and_then(|data| data.as_object()).ok_or("Param data not found"));
    let key = try!(data.get("key").ok_or("Attribute key not found"));
    let value = try!(data.get("value").ok_or("Attribute value not found"));
    Ok((&key, &value))
}

fn get_parameter<'s, T>(name: &str, params: &'s JsonValue, mapping: &Fn(&'s JsonValue) -> Option<T>) -> Result<T, ClientError> {
    params.find(name)
        .and_then(|value| mapping(value))
        .ok_or(ClientError::new(ClientErrorType::GettingParamsError(vec![format!("{}:{}",
                                                                                 String::from(name),
                                                                                 params.find(name)
                                                                                     .map(|param| param.to_string())
                                                                                     .unwrap_or("not found".to_string()))])))
}

pub fn mount_api() {
    // env_logger::init().unwrap();
    log4rs::init_file("config/log4rs.yml", Default::default()).unwrap();
    info!("Hello, world!");

    let api = Api::build(|api| {
        api.version("v1", Versioning::Path);
        api.prefix("api");

        api.mount(swagger::create_api("api-docs"));

        api.mount(Api::build(|cache_api| {

            cache_api.get("info", |endpoint| {
                endpoint.handle(|client, _| {
                    let db_manager = client.app.get_data_base_manager();
                    db_manager.print_info();
                    client.json(&db_manager.get_tables_json_list())
                })
            });

            cache_api.namespace("tx/:mode", |tx_ns| {
                tx_ns.post("start", |endpoint| {
                    endpoint.params(|params| params.opt_typed("mode", json_dsl::string()));

                    endpoint.handle(|client, params| {
                        handle_response(client, |client| {
                            let mode = params.find("mode")
                                .and_then(|mode_json| mode_json.as_str())
                                .map(|mode_string| match mode_string {
                                    "optimistic" => Ok(LockMode::Optimistic),
                                    "pessimistic" => Ok(LockMode::Pessimistic),
                                    _ => {
                                        Err(ClientError::new(ClientErrorType::GettingParamsError(vec![format!("Unknown LockMode {}", mode_string)])))
                                    }
                                })
                                .unwrap_or(Ok(LockMode::Pessimistic));

                            let db_manager = client.app.get_data_base_manager();
                            match db_manager.tx_start(try!(mode)) {
                                Ok(tx_id) => {
                                    debug!("Response start tx {}", tx_id);
                                    Ok(JsonValue::U64(tx_id as u64))
                                }
                                Err(error) => Err(ClientError::new(ClientErrorType::CommonError(error.to_string()))),
                            }
                        })
                    })
                });

                tx_ns.delete("stop/:tx_id", |endpoint| {
                    endpoint.params(|params| params.req_typed("tx_id", json_dsl::u64()));

                    endpoint.handle(|client, params| {
                        handle_response(client, |client| {
                            let tx_id = try!(get_parameter("tx_id", params, &rustless::json::JsonValue::as_u64)) as u32;

                            let db_manager = client.app.get_data_base_manager();
                            match db_manager.tx_stop(&tx_id) {
                                Ok(()) => Ok(JsonValue::String(String::from("done"))),
                                Err(error) => Err(client_error!(error.to_string())),
                            }
                        })
                    })
                })
            });

            cache_api.post("put/:table_name", |endpoint| {
                endpoint.params(|params| {
                    params.req_typed("table_name", json_dsl::string());
                    params.req_typed("tx_id", json_dsl::u64());
                    params.req("data", |_| {})
                });
                endpoint.handle(|client, params| {
                    handle_response(client, |client| {
                        info!("put entity to table");
                        match get_key_and_value(params) {
                            Ok((key, value)) => {
                                let db_manager = client.app.get_data_base_manager();
                                let tx_id = try!(get_parameter("tx_id", params, &rustless::json::JsonValue::as_u64)) as u32;
                                let table_name = try!(get_parameter("table_name", params, &rustless::json::JsonValue::as_str));

                                db_manager.add_data(&tx_id, &String::from(table_name), &key, &value)
                                    .map(|_| JsonValue::String("Done".to_string()))
                                    .map_err(|error| client_error!(error.to_string()))
                            }
                            Err(message) => Err(client_error!(message)),
                        }
                    })
                })
            });

            cache_api.get("get/:table_name/:tx_id/:key", |endpoint| {
                endpoint.params(|params| {
                    params.req_typed("table_name", json_dsl::string());
                    params.req("key", |_| {}); //, json_dsl::object());
                    params.req_typed("tx_id", json_dsl::i64())
                });

                endpoint.handle(|client, params| {
                    handle_response(client, |client| {
                        info!("get entity from table {}", params);
                        let table_name = try!(get_parameter("table_name", params, &rustless::json::JsonValue::as_str));

                        let key = try!(params.find("key")
                            .and_then(|key| key.as_str())
                            .map(|key| {
                                rustless::json::JsonValue::from_str(key)
                                    .map_err(|error| ClientError::new(ClientErrorType::GettingParamsError(vec![format!("key:{}", error)])))
                            })
                            .unwrap_or(Err(ClientError::new(ClientErrorType::GettingParamsError(vec![format!("key")])))));

                        let tx_id = try!(get_parameter("tx_id", params, &rustless::json::JsonValue::as_u64)) as u32;

                        let db_manager = client.app.get_data_base_manager();
                        let value = db_manager.get_data(&tx_id, &String::from(table_name), &key);
                        match value {
                            Ok(value) => {
                                match value {
                                    Some(value) => Ok(value),
                                    None => {
                                        client.not_found();
                                        Ok(JsonValue::String(format!("Entity with key {} not found", key)))
                                    }
                                }
                            }
                            Err(message) => Err(client_error!(message.to_string())),
                        }
                    })
                })
            });

            cache_api.get("get/:table_name/:tx_id/:start/:count", |endpoint| {
                endpoint.params(|params| {
                    params.req_typed("table_name", json_dsl::string());
                    params.req_typed("tx_id", json_dsl::u64());
                    params.req_typed("start", json_dsl::u64());
                    params.req_typed("count", json_dsl::u64())
                });

                endpoint.handle(|client, params| {
                    handle_response(client, |client| {
                        debug!("Get list entities from table {}", params);
                        let table_name = try!(get_parameter("table_name", params, &rustless::json::JsonValue::as_str));
                        let tx_id = try!(get_parameter("tx_id", params, &rustless::json::JsonValue::as_u64));
                        let start = try!(get_parameter("start", params, &rustless::json::JsonValue::as_u64));
                        let count = try!(get_parameter("count", params, &rustless::json::JsonValue::as_u64));
                        let db_manager = client.app.get_data_base_manager();

                        let data_list = db_manager.get_list(tx_id as u32,
                                                            &String::from(table_name),
                                                            start as u32,
                                                            count as u32);
                        data_list.map(|data_list| rustless::json::JsonValue::Array(data_list))
                            .map_err(|error| client_error!(error))
                    })
                })
            });

            cache_api.namespace("meta", |meta_ns| {
                meta_ns.post("table", |endpoint| {
                    endpoint.desc("Update description");
                    endpoint.params(|params| {
                        params.req_typed("name", json_dsl::string());
                        params.req_typed("key", json_dsl::object());
                        params.req_typed("value", json_dsl::object())
                    });

                    endpoint.handle(|client, params| {
                        handle_response(client, |client| {
                            info!("Table update");
                            let table_desc = try!(TableDescriptionView::from_json(params).map_err(|error| ClientError::from_display(&error)));
                            match client.app.get_data_base_manager().add_table(table_desc) {
                                Ok(name) => Ok(JsonValue::String(format!("Table with name {} succefully added", name))),
                                Err(message) => Err(client_error!(message)),
                            }
                        })
                    })
                });

                meta_ns.get("tx/list", |endpoint| {
                    endpoint.handle(|client, _| {
                        handle_response(client,
                                        |client| Ok(client.app.get_data_base_manager().get_transactions_list()))
                    })
                });

                meta_ns.get("table/:name", |endpoint| {
                    endpoint.params(|params| params.req_typed("name", json_dsl::string()));

                    endpoint.handle(|client, params| {
                        handle_response(client, |client| {
                            let name = try!(get_parameter("name", params, &rustless::json::JsonValue::as_str));
                            info!("Table with name {}", name);
                            let table_desc = client.app
                                .get_data_base_manager()
                                .get_table_json(&String::from(name));
                            match table_desc {
                                Some(table_desc) => Ok(table_desc),
                                None => Err(client_error!(format!("Table {} not found", name))),
                            }
                        })
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


}
