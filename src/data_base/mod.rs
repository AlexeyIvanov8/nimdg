extern crate iron;
extern crate concurrent_hashmap;
extern crate bincode;

use rustc_serialize::json;
use rustc_serialize::json::Json;
use rustless::json::ToJson;

use concurrent_hashmap::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::boxed::Box;
use std::sync::atomic::AtomicUsize;
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
}

impl ToJson for EntityDescription {
	fn to_json(&self) -> rustless::json::JsonValue {
		rustless::json::JsonValue::Object(self.fields.iter().map(|(k, v)| {
			(k.clone(), v.to_json())
		}).collect())
	}
}

struct Field {
	typeId: u16,
	data: Vec<u8>,
}

struct Entity {
	fields: Vec<Field>,
}

impl EntityDescription {
	fn new() -> EntityDescription {
		EntityDescription { fields: BTreeMap::new(), ids_map: BTreeMap::new() }
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
		Ok(EntityDescription { fields: entity_fields})
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
        
        let mut ed = EntityDescription::new();
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
		self.tableDescriptions.get(name).map(to_json)
	} 

    pub fn addTable(&self, tableDescription: TableDescriptionView) {
        let tableDesc = createTableDescription(&tableDescription, &self.typeDescriptions);
		self.tableDescriptions.insert(tableDesc.name.clone(), tableDesc);
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