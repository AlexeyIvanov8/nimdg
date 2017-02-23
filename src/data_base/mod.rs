extern crate iron;
extern crate concurrent_hashmap;
extern crate bincode;
extern crate serde_json;

use rustless::json::ToJson;

use concurrent_hashmap::*;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::boxed::Box;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std;
use std::fmt::{Debug, Display};

use bincode::rustc_serialize::{encode, decode};

use rustless::{self, Extensible};

// Type trait, that allow define user type
pub struct TypeDescription {
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

#[derive(Debug)]
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

#[derive(Debug)]
#[derive(Eq)]
#[derive(Hash)]
pub struct Entity {
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
		let ids_map = fields.iter()
				.map(|(k, _)| { (count.fetch_add(1, Ordering::Relaxed) as u16, k.clone()) })
				.collect::<BTreeMap<u16, String>>(); //BTreeMap::<u16, String>::new();
		let reverse_ids_map = ids_map.iter().map(|(k, v)| { (v.clone(), k.clone()) }).collect::<BTreeMap<String, u16>>();
		EntityDescription { count: count, fields: fields, ids_map: ids_map, reverse_ids_map: reverse_ids_map }
	}

	fn add_field(&mut self, name: String, type_desc: TypeDescription) {
		self.fields.insert(name.clone(), Arc::new(Box::new(type_desc)));
		let id = self.count.fetch_add(1, Ordering::Relaxed) as u16;
		self.ids_map.insert(id.clone(), name.clone());
		self.reverse_ids_map.insert(name.clone(), id.clone());
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

#[derive(Debug)]
pub enum IoEntityError {
	Read(String),
	Write(String),
}

#[derive(Debug)]
pub enum PersistenceError {
	IoEntity(IoEntityError),
	TableNotFound(String),
	EntityNotFound(Entity),
	Undefined(String),
}

impl Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Table {
	fn json_to_entity(json: &rustless::json::JsonValue, description: &EntityDescription) -> Result<Entity, IoEntityError> {
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

				if !unselected_json_keys.is_empty() || !unselected_typed_keys.is_empty() {
					Err(IoEntityError::Read(
						"Found unselected json values = [".to_string() + 
						unselected_json_keys.iter().fold(String::new(), |acc, ref key| { acc + key.as_str() }).as_str() + 
						"] and unused entity fields =[" +
						unselected_typed_keys.iter().fold(String::new(), |acc, ref key| { acc + key.as_str() }).as_str() + "]"))
				}
				else {
					let fields = selected_values.iter().map(|(_, &(&field_id, ref type_desc, ref value))| 
							(field_id, Field { data: (type_desc.reader)(&value) }))
						.collect();
					Ok( Entity { fields: fields } )
				}
			});
			match res {
				Some(value) => value,
				None => Err(IoEntityError::Read("Json object not found".to_string())),
			}
			/*description.fields.map(|(name, desc)| {
				
			})*/
		} else {
			Err(IoEntityError::Read("Not object".to_string()))
		}
	}

	fn entity_to_json(entity: &Entity, entity_description: &EntityDescription) -> Result<rustless::json::JsonValue, IoEntityError> {
		let json_object: BTreeMap<String, rustless::json::JsonValue> = entity.fields.iter().filter_map(|(type_id, value)| {
			let field_name = entity_description.ids_map.get(type_id);
			let type_desc = field_name.and_then(|field_name| 
				entity_description.fields.get(field_name).map(|type_desc| (field_name, type_desc)) );
			type_desc.map(|(name, type_desc)| { (name.clone(), (type_desc.writer)(&value.data)) })
		}).collect::<BTreeMap<String, rustless::json::JsonValue>>();

		if json_object.len() != entity.fields.len() {
			let unset = entity.fields.iter()
				.filter(|&(&type_id, _)| 
					entity_description.ids_map
						.get(&type_id)
						.and_then(|name| entity_description.fields.get(name) )
						.is_none())
				.map(|(type_id, _)| type_id.to_string())
				.fold(String::new(), |acc, type_id| acc + ", " + type_id.as_str());
			Err(IoEntityError::Write("Not found field descriptions for some fields ".to_string() + unset.as_str()))
		} else {
			Ok(rustless::json::JsonValue::Object(json_object))
		}
	}

	pub fn put(&self, key: &rustless::json::JsonValue, value: &rustless::json::JsonValue) -> Result<(), PersistenceError> {
		let key_entity = try!(Table::json_to_entity(key, &self.description.key).map_err(|err| PersistenceError::IoEntity(err)));
		let value_entity = try!(Table::json_to_entity(value, &self.description.value).map_err(|err| PersistenceError::IoEntity(err)));
		self.data.insert(key_entity, value_entity);//.ok_or(PersistenceError::Undefined("Cannot write. I don't know what.".to_string()))
		Ok(())
	}

	pub fn get(&self, key: &rustless::json::JsonValue) -> Result<Option<rustless::json::JsonValue>, PersistenceError> {
		let key_entity = try!(Table::json_to_entity(key, &self.description.key).map_err(|err| PersistenceError::IoEntity(err)));
		match self.data.find(&key_entity) {
			Some(data) => Table::entity_to_json(data.get(), &self.description.value)
				.map(|json_entity| Some(json_entity))
				.map_err(|err| PersistenceError::IoEntity(err)),
			None => Ok(None)
		}
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
    type_descriptions: BTreeMap<String, Arc<Box<TypeDescription>>>,
    table_descriptions: ConcHashMap<String, TableDescription>, //BTreeMap::<String, TableDescription>::new();
	tables: ConcHashMap<String, Table>,
}

/*fn read(ed: &EntityDescription, json_string: String) -> HashMap<String, Vec<u8>> {
	let data = rustless::json::JsonValue::from_str(&json_string).unwrap();
	let object = data.as_object().unwrap();
	let mut res = HashMap::new();
	for (key, value) in object.iter() {
		match ed.fields.get(key) {
			Some(type_desc) => res.insert(key.clone(), (type_desc.reader)(value)),
			None => panic!("Type for key {} not found", key),
		};
	};
	res
}

fn write(ed: &EntityDescription, data: HashMap<String, Vec<u8>>) -> rustless::json::JsonValue {
	let mut json_object = BTreeMap::<String, rustless::json::JsonValue>::new();
	for (key, value) in data.iter() {
		match ed.fields.get(key) {
			Some(type_desc) => json_object.insert(key.clone(), (type_desc.writer)(value)),
			None => panic!("Type for key {} not found", key),
		};
	};
	rustless::json::JsonValue::Object(json_object)
}*/

fn get_undefined_fields(entity_fields: &BTreeMap<String, Option<Arc<Box<TypeDescription>>> >) -> Vec<String> {
	entity_fields.iter().filter_map(|(k, v)| { match *v {
		Some(_) => None,
		None => Some(k.clone()),
	}}).collect::<Vec<String>>()
}

fn create_entity_description(
		view: &EntityDescriptionView, 
		type_descs: &BTreeMap<String, Arc<Box<TypeDescription>>>) -> Result<EntityDescription, String> {
	let mut entity_fields = view.fields.iter().map(|(k, v)| {
		(k.clone(), type_descs.get(v).map(|type_desc| { type_desc.clone() }))
	}).collect();
	let undefined_fields: Vec<String> = get_undefined_fields(&entity_fields);
	if undefined_fields.iter().next().is_some() {
		Err(undefined_fields.iter().fold(String::new(), |base, field_name| { base + ", " + field_name.as_str() }))
	}
	else {
		let entity_fields = entity_fields.iter_mut().filter_map(move |(k, v)| { v.clone().map(|value| { (k.clone(), value )}) }).collect();
		Ok(EntityDescription::from_fields(entity_fields))
	}
}

fn create_table_description(view: &TableDescriptionView, type_descs: &BTreeMap<String, Arc<Box<TypeDescription>>>) -> TableDescription {
	TableDescription { 
		name: view.name.clone(),
		key: create_entity_description(&view.key, type_descs).unwrap(),
		value: create_entity_description(&view.value, type_descs).unwrap()
	}
}

impl DataBaseManager {
    pub fn new() -> Result<DataBaseManager, String> {
        let mut db_manager = DataBaseManager { 
            type_descriptions: BTreeMap::new(),
            table_descriptions: ConcHashMap::<String, TableDescription>::new(),
			tables: ConcHashMap::<String, Table>::new() };

        let string_type = TypeDescription {
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

        let u64_type = TypeDescription {
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

		try!(db_manager.add_type(u64_type));
		try!(db_manager.add_type(string_type));

        Ok(db_manager)
    }

	pub fn add_type(&mut self, type_desc: TypeDescription) -> Result<(), String> {
		if !self.type_descriptions.contains_key(&type_desc.name) {
			self.type_descriptions.insert(type_desc.name.clone(), Arc::new(Box::new(type_desc)));
			Ok(())
		} else {
			Err("Type with name ".to_string() + type_desc.name.clone().as_str() + " already defined.")
		}
	}

    pub fn print_info(&self) -> () {
        println!("I'm a data base manager");
    }

    pub fn get_tables_list(&self) -> rustless::json::JsonValue {
        let res = self.table_descriptions.iter().map(|(k, v)| {
			(k.clone(), v.to_json())
		}).collect();
		rustless::json::JsonValue::Object(res)
    }

	pub fn get_table(&self, name: &String) -> Option<rustless::json::JsonValue> {
		//self.table_descriptions.find(name).map(|table| { table.get().to_json() })
	self.tables.find(name).map(|table| { table.get().description.to_json() })
	} 

	/** Add new table by he view description
	 * return - table name or error description is adding fail */
    pub fn add_table(&self, table_description: TableDescriptionView) -> Result<String, String> {
		if !self.table_descriptions.find(&table_description.name).is_some() {
        	let table_desc = create_table_description(&table_description, &self.type_descriptions);
			self.tables.insert(table_desc.name.clone(), Table { description: table_desc, data: ConcHashMap::<Entity, Entity>::new() });
			//self.table_descriptions.insert(table_desc.name.clone(), table_desc);
			Ok(table_description.name.clone())
		} else {
			Err("Table with name ".to_string() + table_description.name.as_str() + " already exists.")
		}
    }

	pub fn add_data(&self,
			table_name: &String, 
			key: &rustless::json::JsonValue,
			value: &rustless::json::JsonValue) -> Result<(), PersistenceError> {
		match self.tables.find(table_name) {
			Some(table) => table.get().put(key, value),
			None => Err(PersistenceError::TableNotFound(table_name.clone()))
		}
	}

	pub fn get_data(&self,
			table_name: &String,
			key: &rustless::json::JsonValue) -> Result<Option<rustless::json::JsonValue>, PersistenceError> {
		let table = try!(self.tables.find(table_name).ok_or(PersistenceError::TableNotFound(table_name.clone())));
		table.get().get(key)
	}
}

pub struct AppDataBase;
impl iron::typemap::Key for AppDataBase {
    type Value = DataBaseManager;
}

pub trait DataBaseExtension: rustless::Extensible {
    fn get_data_base_manager(&self) -> &DataBaseManager;
}

impl DataBaseExtension for rustless::Application {
    fn get_data_base_manager(&self) -> &DataBaseManager {
        self.ext().get::<AppDataBase>().unwrap()
    }
}