extern crate iron;
extern crate concurrent_hashmap;
extern crate bincode;
extern crate serde_json;

use rustc_serialize::json;
use rustc_serialize::json::Json;
use rustless::json::ToJson;

use concurrent_hashmap::*;
use std::collections::BTreeMap;
use std::hash::Hash;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::boxed::Box;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use bincode::rustc_serialize::{encode, decode};

use std::str::FromStr;

use rustless::{self, Extensible};

// Type trait, that allow define user type
struct TypeDescription {
	name: String,
	reader: Box<Fn(&rustless::json::JsonValue) -> Vec<u8>>,
	writer: Box<Fn(&Vec<u8>) -> rustless::json::JsonValue>,
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

#[derive(Eq)]
#[derive(Hash)]
struct Field {
	//typeId: u16,
	data: Vec<u8>,
}

impl PartialEq for Field {
    fn eq(&self, other: &Field) -> bool {
        self.data == other.data
    }
}

#[derive(Eq)]
#[derive(Hash)]
struct Entity {
	fields: BTreeMap<u16, Field>,
}

impl PartialEq for Entity {
	fn eq(&self, other: &Entity) -> bool {
		self.fields == other.fields
	}
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
		let mut ids_map = fields.iter()
				.map(|(k, v)| { (count.fetch_add(1, Ordering::Relaxed) as u16, k.clone()) })
				.collect::<BTreeMap<u16, String>>(); //BTreeMap::<u16, String>::new();
		let mut reverse_ids_map = ids_map.iter().map(|(k, v)| { (v.clone(), k.clone()) }).collect::<BTreeMap<String, u16>>();
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
	fn read_entity(json: &rustless::json::JsonValue, description: &EntityDescription) -> Result<Entity, String> {
		if json.is_object() {
			let json_object = json.as_object();
			let res = json_object.map(|object| {
				// 1. select types for json fields
				let selected_values = object.iter().filter_map(|(name, value)| {
					let type_desc = description.fields.get(name);
					let field_id = description.reverse_ids_map.get(name);
					let field_desc = type_desc.and_then(|type_desc| { 
						field_id.map(|field_id| { (field_id, type_desc) })
					});
					field_desc.map(|(field_id, type_desc)| (name.clone(), (field_id, type_desc.clone(), value)) )
				}).collect::<BTreeMap<String, (&u16, Arc<Box<TypeDescription>>, &rustless::json::JsonValue)>>();
				let selected_values_keys = selected_values.keys().map(|key| key.clone()).collect::<HashSet<String>>();
				
				// 2. check what all fields is typed and 
				let json_keys = object.keys().map(|key| key.clone()).collect::<HashSet<String>>();
				let unselected_json_keys = &selected_values_keys ^ &json_keys;

				// 3. all types selected
				let types_keys = description.fields.keys().map(|key| key.clone()).collect::<HashSet<String>>();
				let unselected_typed_keys = &selected_values_keys ^ &types_keys;

				if !unselected_json_keys.is_empty() || unselected_typed_keys.is_empty() {
					Err("Found unselected json values = [".to_string() + 
						unselected_json_keys.iter().fold(String::new(), |acc, ref key| { acc + key.as_str() }).as_str() + 
						"] and unused entity fields =[" +
						unselected_typed_keys.iter().fold(String::new(), |acc, ref key| { acc + key.as_str() }).as_str())
				}
				else {
					let fields = selected_values.iter().map(|(name, &(&field_id, ref type_desc, ref value))| 
							(field_id, Field { data: (type_desc.reader)(&value) }))
						.collect();
					Ok( Entity { fields: fields } )
				}
			});
			match res {
				Some(value) => value,
				None => Err("Json object not found".to_string()),
			}
			/*description.fields.map(|(name, desc)| {
				
			})*/
		} else {
			Err("Not object".to_string())
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
	let data = rustless::json::JsonValue::from_str(&jsonString).unwrap();
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

fn write(ed: &EntityDescription, data: HashMap<String, Vec<u8>>) -> rustless::json::JsonValue {
	let mut jsonObject = BTreeMap::<String, rustless::json::JsonValue>::new();
	for (key, value) in data.iter() {
		match ed.fields.get(key) {
			Some(typeDesc) => jsonObject.insert(key.clone(), (typeDesc.writer)(value)),
			None => panic!("Type for key {} not found", key),
		};
	};
	rustless::json::JsonValue::Object(jsonObject)
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
            tableDescriptions: ConcHashMap::<String, TableDescription>::new(),
			tables: ConcHashMap::<String, Table>::new() };

        let stringType = TypeDescription {
            name: "String".to_string(),
            reader: Box::new(move |json| {
                match json.clone() {
                    rustless::json::JsonValue::String(value) => encode(&value.clone(), bincode::SizeLimit::Infinite).unwrap(),
                    _ => panic!("Ожидался тип String"),
                }
            }),
            writer: Box::new(move |value: &Vec<u8>| {
                let string: String = decode(&value[..]).unwrap();
                rustless::json::JsonValue::String(string)
            }),
        };

        let u64Type = TypeDescription {
            name: "u64".to_string(),
            reader: Box::new(move |json| {
                match json.clone() {
                    rustless::json::JsonValue::U64(value) => encode(&value.clone(), bincode::SizeLimit::Infinite).unwrap(),
                    _ => panic!("Ожидался тип u64"),
                }
            }),
            writer: Box::new(move |value| {
                rustless::json::JsonValue::U64(decode(&value[..]).unwrap())
            }),
        };

		dbManager.add_type(u64Type);
		dbManager.add_type(stringType);

        //dbManager.typeDescriptions.insert(stringType.name.clone(), stringType.clone());
        //dbManager.typeDescriptions.insert(u64Type.name.clone(), u64Type.clone());
        
        /*let mut ed = EntityDescription::blank();
        ed.fields.insert("id".to_string(), u64Type.clone());
        ed.fields.insert("code".to_string(), stringType.clone());
        ed.fields.insert("name".to_string(), stringType.clone());

        let testString = "{\"id\": 0, \"code\": \"Test code 5464565\", \"name\": \"John Doe\"}".to_string();
        let readed = read(&ed, testString);

        println!("### = {:?}", readed);

        let writed = write(&ed, readed);
        println!("$$$ = {}", writed);*/

        dbManager
    }

	pub fn add_type(&self, type_desc: TypeDescription) -> Result<(), String> {
		if !self.typeDescriptions.contains_key(type_desc.name) {
			self.typeDescriptions.insert(type_desc.name.clone(), Arc::new(Box::new(type_desc)));
			Ok()
		} else {
			Err("Type with name ".to_string() + type_desc.name.clone().as_str() + " already defined.")
		}
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

	pub fn add_data(&self,
			table_name: &String, 
			key: &rustless::json::JsonValue,
			value: &rustless::json::JsonValue) -> Result<(), String> {
		match self.tables.find(table_name) {
			Some(table) => { 
				table.get().put(key, value);
				Ok(())
			},
			None => Err("Table with name ".to_string() + table_name.clone().as_str() + " not found.")
		}
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