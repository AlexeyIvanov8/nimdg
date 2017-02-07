
extern crate rustc_serialize;
extern crate concurrent_hashmap;
extern crate bincode;
extern crate valico;
extern crate hyper;
#[macro_use]
extern crate iron;
#[macro_use]
extern crate rustless;

use rustc_serialize::json;
use rustc_serialize::json::Json;
use std::collections::BTreeMap;
use std::boxed::Box;
use std::borrow::Borrow;
use std::ops::Deref;
use std::ops::DerefMut;
use std::hash::Hash;
use std::cmp::Eq;
use std::collections::HashMap;
use std::fmt::Display;

use valico::json_dsl;
use rustless::prelude::*;
use iron::status;
use iron::method;
use std::io::Read;
use rustless::json::ToJson;
use rustless::batteries::swagger;

use rustless::{
    Application, Api, Nesting, Versioning
};

use concurrent_hashmap::*;
use std::sync::Arc;

mod data_base;

use self::data_base::{DataBaseExtension, EntityDescriptionView, TableDescriptionView};

trait BaseStoredValue {}

// Base trait for value, that can be stored in cache
trait StoredValue<V: Clone> : BaseStoredValue {
	fn get(&self) -> V;
	fn set(&mut self, value: V);
}

// Predefined types
#[derive(Hash)]
struct StringValue(String);

#[derive(Hash)]
struct U32Value(u32);

impl BaseStoredValue for StringValue {}
impl StoredValue<String> for StringValue {
	fn get(&self) -> String {
		self.0.clone()
	}
	fn set(&mut self, value: String) {
		self.0 = value;
	}
}

impl BaseStoredValue for U32Value {}
impl StoredValue<u32> for U32Value {
	fn get(&self) -> u32 {
		self.0
	}
	fn set(&mut self, value: u32) {
		self.0 = value;
	}
}

// Cache value
#[derive(Hash)]
struct CacheValue {
	//values: Vec<Box<BaseStoredValue>>,
}

impl CacheValue {
	/*fn get<V: Clone, SV: StoredValue<V>>(&self, index:usize) -> V {
		let box_ref = self.values.get(index).unwrap();
		let reference = box_ref.deref();
		let raw = reference as *const BaseStoredValue;
		let sv_raw = raw as *const SV;
		let value = unsafe { (*sv_raw).get().clone() };
		value
	}*/
}

/*impl PartialEq for CacheValue {
    fn eq(&self, other: &CacheValue) -> bool {
        self.values == other.values
    }
}
impl Eq for CacheValue {}*/

/*impl Eq for CacheValue {
	fn eq(&self, other: CacheValue) -> bool {
		self.vec == other.vec
	}
}*/

struct Cache {
	map: Arc<BTreeMap<CacheValue, CacheValue>>,
}

struct Entity {

}

// reading views from rustless json
fn readEntityDescriptionView(json: &BTreeMap<String, rustless::json::JsonValue>) -> EntityDescriptionView {
	let fields_object = json.get("fields").unwrap().as_object().unwrap();
	let fields = fields_object.iter().map(|(k, v)| { (k.clone(), String::from(v.as_str().unwrap()) ) }).collect();
	//for (k, v) in fields { println!("Field {} = {}", k, v) };
	EntityDescriptionView { fields: fields }
}

fn readTableDescriptionView(json: &rustless::json::JsonValue) -> TableDescriptionView {
	let name = json.find("name").unwrap().as_str().unwrap();
	println!("Found cache desc with name = {}", name);
	let key = readEntityDescriptionView(json.find("key").unwrap().as_object().unwrap());
	let value = readEntityDescriptionView(json.find("value").unwrap().as_object().unwrap());
	TableDescriptionView { name: String::from(name), key: key, value: value }
}

fn run_data_base_manager(app: &mut rustless::Application) {
	let data_base_manager = data_base::DataBaseManager::new();
	app.ext.insert::<data_base::AppDataBase>(data_base_manager);
}

#[derive(RustcDecodable, RustcEncodable)]
pub struct TestStruct {
	data_int: u8,
	data_str: String,
	data_vector: Vec<u8>,
}

fn main() {

    println!("Hello, world!");
    let test = TestStruct {
    	data_int: 9,
    	data_str: "test r".to_string(),
    	data_vector: vec![4, 5, 2],
    };

	let encoded = json::encode(&test).unwrap();
	let decoded: TestStruct = json::decode(&encoded).unwrap();

	println!("Enc = {}, dec = {}", encoded, decoded.data_str);

	let json_string = "{
		\"foo\": \"test string\",
		\"data_int\": 9,
		\"data_str\": \"test r\",
		\"data_vector\": [4,5,2]
	}".to_string();
	let data = Json::from_str(&json_string).unwrap();
	println!("Data: {}", data);
	println!("Data is object = {}", data.is_object());

	let obj = data.as_object().unwrap();
	let foo = obj.get("foo").unwrap();

	println!("Array? {:?}", obj);
	println!("Str? {:?}", foo);

	for (key, value) in obj.iter() {
		println!("{}: {}", key, match *value {
            Json::U64(v) => format!("{} (u64)", v),
            Json::String(ref v) => format!("{} (string)", v),
            Json::Array(ref arr) => {
            	arr.iter()
            		.map(|elt| format!("{:?}", elt) )
            		.fold("Arr[".to_string(), |mut acc, i| { 
            			acc.push_str(i.as_str()); acc 
            		})
            },
            _ => format!("other")
        });
	}

	/*let mut cv = CacheValue { values: vec![
		Box::new(StringValue { value: "Test str".to_string() }), 
		Box::new(U32Value { value: 345 }) ]};
	//println!("Cache value = {:?}", cv.get2::<u32, U32Value>(1));
	println!("Cache value = {:?}", cv.get::<u32, U32Value>(1));
	println!("Cache value = {:?}", cv.get::<String, StringValue>(0));*/

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
				endpoint.handle(|client, params| {
					let db_manager = client.app.getDataBaseManager();
					db_manager.printInfo();
					//client.text("Some usefull info".to_string())
					client.json(&db_manager.getTablesList())
				})
			});

			cache_api.namespace("meta", |mats_ns| {
				cache_api.post("table", |endpoint| {
					println!("Table update");
					endpoint.desc("Update description");
					endpoint.params(|params| {
						params.req_typed("desc_id", json_dsl::u64());
						params.req_typed("id", json_dsl::u64());
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
						println!("Params = {:?}", &_params.to_json());
						let cache_desc = _params.find("data").unwrap();
						let tableDesc = readTableDescriptionView(cache_desc);
						for (k, v) in &tableDesc.key.fields { println!("  key.field {}:{}", k, v) };
						for (k, v) in &tableDesc.value.fields { println!("  value.field {}:{}", k, v) };
						client.app.getDataBaseManager().addTable(tableDesc);
						client.set_status(rustless::server::status::StatusCode::Ok);
						client.json(&_params.to_json())
					})
				});

				cache_api.get("table/:name", |endpoint| {
					endpoint.params(|params| {
						params.req_typed("name", json_dsl::string)
					})

					endpoint.handle(|client, _params| {
						let tableJson = _params
								.find("name")
								.map(as_str)
								.and_then(|name| { client.add.getDataBaseManager().getTable(name) });
						client.json(tableJson)
					})
				});
			});
		}));
	});

	let mut app = Application::new(api);
	run_data_base_manager(&mut app);

	swagger::enable(&mut app, swagger::Spec {
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
				url: "http://opensource.org/licenses/MIT".to_string()
			}),
			..std::default::Default::default()
		},
		host: Some("localhost:4300".to_string()),
		..std::default::Default::default()
	});

	iron::Iron::new(app).http("localhost:4300").unwrap();
	/*Iron::new(|request: &mut Request| {
		Ok(match request.method {
			method::Get => Response::with((status::NotImplemented, "Method get not supported yet")),
			method::Put => {
				let mut buffer = String::new();
				request.body.read_to_string(&mut buffer);
				println!("Getting string = {}", buffer);
				Response::with((status::Ok, "Getting success"))
			},
			_ => Response::with((status::NotImplemented, "This method not implemented yet")),
		})
	}).http("localhost:4300").unwrap();*/
}
