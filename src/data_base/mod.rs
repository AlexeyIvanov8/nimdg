extern crate iron;
extern crate concurrent_hashmap;
extern crate bincode;

use rustc_serialize::json;
use rustc_serialize::json::Json;
use rustless::json::ToJson;

use concurrent_hashmap::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::boxed::Box;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use bincode::rustc_serialize::{encode, decode};

use rustless::{self, Extensible};

// Type trait, that allow define user type
struct TypeDescription {
	name: String,
	reader: Box<Fn(&Json) -> Vec<u8>>,
	writer: Box<Fn(&Vec<u8>) -> Json>,
}

unsafe impl Send for TypeDescription {}
unsafe impl Sync for TypeDescription {}

impl ToJson for TypeDescription {
	fn to_json(&self) -> rustless::json::JsonValue {
		rustless::json::to_value(self.name.clone())
	}
}

// Universal description of some entity. For example: key or value
// For performance purposes each field is marked by number id
pub struct EntityDescription {
	count: AtomicUsize,
	fields: BTreeMap<String, Arc<Box<TypeDescription>>>,
	ids_map: BTreeMap<u16, String>,
	reverse_ids_map: BTreeMap<String, u16>,
}

impl ToJson for EntityDescription {
	fn to_json(&self) -> rustless::json::JsonValue {
		rustless::json::JsonValue::Object(self.fields.iter().map(|(k, v)| {
			(k.clone(), v.to_json())
		}).collect())
	}
}

struct Field {
	//typeId: u16,
	data: Vec<u8>,
}

struct Entity {
	fields: BTreeMap<u16, Field>,
}

impl EntityDescription {
	fn blank() -> EntityDescription {
		EntityDescription { count: AtomicUsize::new(0), 
				fields: BTreeMap::new(), ids_map:
				BTreeMap::new(), 
				reverse_ids_map: BTreeMap::new() 
		}
	}

	fn from_fields(fields: BTreeMap<String, Arc<Box<TypeDescription>>>) -> EntityDescription {
		let count = AtomicUsize::new(0);
		let u16_count = (count.fetch_add(1, Ordering::Relaxed) as u16;
		let mut ids_map = fields.iter()
				.map(|(k, v)| { (count.clone(), k.clone()) })
				.collect(); //BTreeMap::<u16, String>::new();
		let mut reverse_ids_map = ids_map.map(|(k, v)| { (v, k) }).collect();
		EntityDescription { count: count, fields: fields, ids_map: ids_map, reverse_ids_map: reverse_ids_map }
	}

	fn addField(&mut self, name: String, typeDesc: TypeDescription) {

	}
}

pub struct TableDescription {
	name: String,
	key: EntityDescription,
	value: EntityDescription,
}

impl ToJson for TableDescription {
	fn to_json(&self) -> rustless::json::JsonValue {
		let mut res = BTreeMap::<String, rustless::json::JsonValue>::new();
		res.insert(String::from("name"), rustless::json::to_value(self.name.clone()));
		res.insert(String::from("key"), self.key.to_json());
		res.insert(String::from("value"), self.value.to_json());
		rustless::json::JsonValue::Object(res)
	}
}

pub struct Table {
	description: TableDescription,
	data: ConcHashMap<Entity, Entity>,
}

impl Table {
	fn read_entity(&self, json: &rustless::json::JsonValue, description: &EntityDescription) -> Result<Entity, &'static str> {
		if json.is_object() {
			let json_object = json.as_object();
			json_object.and_then(|object| {
				// 1. select types for json fields
				let selected_values = object.iter().filter_map(|(name, value)| {
					let type_desc = description.fields.get(name);
					let field_id = description.reverse_ids_map.get(name);
					let field_desc = type_desc.and_then(|type_desc| { 
						field_id.map(|field_id| { (field_id, type_desc) })
					});
					field_desc.map(|(field_id, type_desc)| (name, (field_id, type_desc, value)) )
				}).collect();
				let selected_values_keys = selected_values.keys().iter().collect::<HashSet<String>>();
				
				// 2. check what all fields is typed and 
				let json_keys = object.keys().iter().collect::<HashSet<String>>();
				let unselected_json_keys = selected_values_keys.intersect(json_keys);

				// 3. all types selected
				let types_keys = description.fields.keys().iter().collect::<HashSet<String>>();
				let unselected_typed_keys = selected_values_keys.intersect(types_keys);

				if !unselected_json_keys.is_empty() || unselected_typed_keys.is_empty() {
					Err("Found unselected json values = [" + 
						unselected_json_keys.iter().fold(String::new(), |acc, &key| { acc + key }) + 
						"] and unused entity fields =[" +
						unselected_typed_keys.iter().fold(String::new(), |acc, &key| { acc + key }))
				}
				else {
					Ok( selected_values.map(|(name, (field_id, type_desc, value))| (field_id, type_desc.reader(value))) )
				}
			});
			description.fields.map(|(name, desc)| {
				
			})
		}
	}

	pub fn put(&self, key: &rustless::json::JsonValue, value: &rustless::json::JsonValue) {
		let key_entity = Table::read_entity(key, &self.description.key);
		let value_entity = Table::read_entity(value, &self.description.value);
		key_entity.and_then(|key_entity| { 
			value_entity.map(|value_entity| { 
				(key_entity, value_entity) 
			})
		}).map(|(k, v)| {
			self.data.insert(k, v)
		});
	}
}
// For getting from frontend
pub struct EntityDescriptionView {
	pub fields: BTreeMap<String, String>,
}

pub struct TableDescriptionView {
	pub name: String,
	pub key: EntityDescriptionView,
	pub value: EntityDescriptionView,
}

pub struct DataBaseManager {
    typeDescriptions: BTreeMap<String, Arc<Box<TypeDescription>>>,
    tableDescriptions: ConcHashMap<String, TableDescription>, //BTreeMap::<String, TableDescription>::new();
	tables: ConcHashMap<String, Table>,
}

fn read(ed: &EntityDescription, jsonString: String) -> HashMap<String, Vec<u8>> {
	let data = Json::from_str(&jsonString).unwrap();
	let object = data.as_object().unwrap();
	let mut res = HashMap::new();
	for (key, value) in object.iter() {
		match ed.fields.get(key) {
			Some(typeDesc) => res.insert(key.clone(), (typeDesc.reader)(value)),
			None => panic!("Type for key {} not found", key),
		};
	};
	res
}

fn write(ed: &EntityDescription, data: HashMap<String, Vec<u8>>) -> Json {
	let mut jsonObject = BTreeMap::<String, Json>::new();
	for (key, value) in data.iter() {
		match ed.fields.get(key) {
			Some(typeDesc) => jsonObject.insert(key.clone(), (typeDesc.writer)(value)),
			None => panic!("Type for key {} not found", key),
		};
	};
	Json::Object(jsonObject)
}

fn getUndefinedFields(entity_fields: &BTreeMap<String, Option<Arc<Box<TypeDescription>>> >) -> Vec<String> {
	entity_fields.iter().filter_map(|(k, v)| { match *v {
		Some(ref typeDesc) => None,
		None => Some(k.clone()),
	}}).collect::<Vec<String>>()
}

fn createEntityDescription(
		view: &EntityDescriptionView, 
		typeDescs: &BTreeMap<String, Arc<Box<TypeDescription>>>) -> Result<EntityDescription, String> {
	let mut entity_fields = view.fields.iter().map(|(k, v)| {
		(k.clone(), typeDescs.get(v).map(|typeDesc| { typeDesc.clone() }))
	}).collect();
	let undefined_fields: Vec<String> = getUndefinedFields(&entity_fields);
	if undefined_fields.iter().next().is_some() {
		Err(undefined_fields.iter().fold(String::new(), |base, field_name| { base + ", " + field_name.as_str() }))
	}
	else {
		let entity_fields = entity_fields.iter_mut().filter_map(move |(k, v)| { v.clone().map(|value| { (k.clone(), value )}) }).collect();
		Ok(EntityDescription::from_fields(entity_fields))
	}
}

fn createTableDescription(view: &TableDescriptionView, typeDescs: &BTreeMap<String, Arc<Box<TypeDescription>>>) -> TableDescription {
	TableDescription { 
		name: view.name.clone(),
		key: createEntityDescription(&view.key, typeDescs).unwrap(),
		value: createEntityDescription(&view.value, typeDescs).unwrap()
	}
}

impl DataBaseManager {
    pub fn new() -> DataBaseManager {
        let mut dbManager = DataBaseManager { 
            typeDescriptions: BTreeMap::new(),
            tableDescriptions: ConcHashMap::<String, TableDescription>::new() };

        let stringType = Arc::new(Box::new(TypeDescription {
            name: "StringType".to_string(),
            reader: Box::new(move |json| {
                match json.clone() {
                    Json::String(value) => encode(&value.clone(), bincode::SizeLimit::Infinite).unwrap(),
                    _ => panic!("Ожидался тип String"),
                }
            }),
            writer: Box::new(move |value: &Vec<u8>| {
                let string: String = decode(&value[..]).unwrap();
                Json::String(string)
            }),
        }));

        let u64Type = Arc::new(Box::new(TypeDescription {
            name: "U64Type".to_string(),
            reader: Box::new(move |json| {
                match json.clone() {
                    Json::U64(value) => encode(&value.clone(), bincode::SizeLimit::Infinite).unwrap(),
                    _ => panic!("Ожидался тип u64"),
                }
            }),
            writer: Box::new(move |value| {
                Json::U64(decode(&value[..]).unwrap())
            }),
        }));

        dbManager.typeDescriptions.insert("String".to_string(), stringType.clone());
        dbManager.typeDescriptions.insert("u64".to_string(), u64Type.clone());
        
        let mut ed = EntityDescription::blank();
        ed.fields.insert("id".to_string(), u64Type.clone());
        ed.fields.insert("code".to_string(), stringType.clone());
        ed.fields.insert("name".to_string(), stringType.clone());

        let testString = "{\"id\": 0, \"code\": \"Test code 5464565\", \"name\": \"John Doe\"}".to_string();
        let readed = read(&ed, testString);

        println!("### = {:?}", readed);

        let writed = write(&ed, readed);
        println!("$$$ = {}", writed.pretty());

        dbManager
    }

    pub fn printInfo(&self) -> () {
        println!("I'm a data base manager");
    }

    pub fn getTablesList(&self) -> rustless::json::JsonValue {
        let res = self.tableDescriptions.iter().map(|(k, v)| {
			(k.clone(), v.to_json())
		}).collect();
		rustless::json::JsonValue::Object(res)
    }

	pub fn getTable(&self, name: &String) -> Option<rustless::json::JsonValue> {
		self.tableDescriptions.find(name).map(|table| { table.get().to_json() })
	} 

    pub fn addTable(&self, tableDescription: TableDescriptionView) {
        let tableDesc = createTableDescription(&tableDescription, &self.typeDescriptions);
		self.tableDescriptions.insert(tableDesc.name.clone(), tableDesc);
    }

	pub fn addData(&self,
			table_name: String, 
			key: rustless::json::JsonValue,
			value: rustless::json::JsonValue) {
		self.tables.find(table_name).and_then(|table| {
			table.put(key, value);
		});
	}
}

pub struct AppDataBase;
impl iron::typemap::Key for AppDataBase {
    type Value = DataBaseManager;
}

pub trait DataBaseExtension: rustless::Extensible {
    fn getDataBaseManager(&self) -> &DataBaseManager;
}

impl DataBaseExtension for rustless::Application {
    fn getDataBaseManager(&self) -> &DataBaseManager {
        self.ext().get::<AppDataBase>().unwrap()
    }
}