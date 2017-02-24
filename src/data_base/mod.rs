extern crate iron;
extern crate concurrent_hashmap;
extern crate bincode;
extern crate serde_json;

use std;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::boxed::Box;
use std::fmt::{Debug, Display};

use concurrent_hashmap::*;

use bincode::rustc_serialize::{encode, decode};

use rustless::{self};
use rustless::json::ToJson;

pub mod app_extension;
pub mod meta;

use data_base::meta::{TypeDescription, EntityDescription, TableDescription, TableDescriptionView};

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
				entity_description.fields
					.get(field_name)
					.map(|type_desc| (field_name, type_desc)) 
			);
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

pub struct DataBaseManager {
    type_descriptions: BTreeMap<String, Arc<Box<TypeDescription>>>,
    table_descriptions: ConcHashMap<String, TableDescription>, //BTreeMap::<String, TableDescription>::new();
	tables: ConcHashMap<String, Table>,
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
        	let table_desc = try!(TableDescription::from_view(&table_description, &self.type_descriptions));
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