extern crate iron;
extern crate concurrent_hashmap;
extern crate bincode;
extern crate serde_json;

use std;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::boxed::Box;
use std::fmt::{Debug, Display};
use std::sync::{Mutex, MutexGuard, Condvar};

use concurrent_hashmap::*;

use bincode::rustc_serialize::{encode, decode};

use rustless::{self};
use rustless::json::ToJson;

pub mod app_extension;
pub mod meta;
pub mod transaction;

use data_base::meta::{TypeDescription, EntityDescription, TableDescription, TableDescriptionView};
use data_base::transaction::{TransactionManager, Lock, LockType, LockedValue};

// Top struct for interaction with tables
pub struct DataBaseManager {
    type_descriptions: BTreeMap<String, Arc<Box<TypeDescription>>>,
    table_descriptions: ConcHashMap<String, TableDescription>, //BTreeMap::<String, TableDescription>::new();
	tables: ConcHashMap<String, Arc<Table>>,
	tx_manager: Arc<TransactionManager>
}

// Field of entity
#[derive(Debug, Eq, Clone)]
struct Field {
	data: Vec<u8>,
}

// Entity, that can be stored as key or value in table
#[derive(Debug, Eq, Clone)]
pub struct Entity {
	fields: BTreeMap<u16, Field>,
	lock: Lock
}

// Persistence for concrete entity structure
pub struct Table {
	description: TableDescription,
	data: ConcHashMap<Entity, Arc<Mutex<Entity>>>,
	tx_manager: Arc<TransactionManager>
}

// Errors
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
	UndefinedTransaction(u32),
	TransactionAlreadyStarted(u32),
	WrongTransaction(u32, u32) // real tx_id, expected tx_id
}

impl Display for IoEntityError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

// Field impl
impl PartialEq for Field {
    fn eq(&self, other: &Field) -> bool {
        self.data == other.data
    }
}

// Entity impl
impl PartialEq for Entity {
	fn eq(&self, other: &Entity) -> bool {
		self.fields == other.fields
	}
}

impl Hash for Field {
	fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl Hash for Entity {
	fn hash<H: Hasher>(&self, state: &mut H) {
        self.fields.hash(state);
    }
}

// Table impl
impl Table {
	fn json_to_entity(json: &rustless::json::JsonValue, description: &EntityDescription) -> Result<Entity, IoEntityError> {
		if json.is_object() {
			let json_object = try!(json.as_object().ok_or(IoEntityError::Read("Json object not found".to_string())));
			// 1. select types for json fields
			let selected_values = json_object.iter().filter_map(|(name, value)| {
				let type_desc = description.get_field(name);
				let field_id = description.get_field_id(name);

				if let (Some(type_desc), Some(field_id)) =
						(type_desc, field_id) {
					Some((name.clone(), (field_id, type_desc.clone(), value)))
				} else {
					None
				}
			}).collect::<BTreeMap<
					String, 
					(&u16, Arc<Box<TypeDescription>>, &rustless::json::JsonValue)
				>>();
			let selected_values_keys = selected_values.keys().map(|key| key.clone()).collect::<HashSet<String>>();
				
			// 2. check what all fields is typed and 
			let json_keys = json_object.keys().map(|key| key.clone()).collect::<HashSet<String>>();
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
				let fields: Result<BTreeMap<u16, Field>, _> = selected_values.iter()
					.map(|(_, &(&field_id, ref type_desc, ref value))| 
						((type_desc.reader)(&value)).map(|value| { 
							(field_id, Field { data: value }) 
						}))
					.collect();
				fields.map(|fields| Entity { fields: fields, lock: Lock::new() })
				//Ok( Entity { fields: fields } )
			}
		} else {
			Err(IoEntityError::Read("Not object".to_string()))
		}
	}

	fn entity_to_json(entity: &Entity, entity_description: &EntityDescription) -> Result<rustless::json::JsonValue, IoEntityError> {
		let json_object: BTreeMap<String, rustless::json::JsonValue> = try!(entity.fields.iter().filter_map(|(type_id, value)| {
			let field_name = entity_description.ids_map.get(type_id);
			let type_desc = field_name.and_then(|field_name| 
				entity_description.fields
					.get(field_name)
					.map(|type_desc| (field_name, type_desc)) );
			type_desc.map(|(name, type_desc)| { ((type_desc.writer)(&value.data)).map(|data| (name.clone(), data)) })
		}).collect());//::<BTreeMap<String, rustless::json::JsonValue>>();

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
		self.data.insert(key_entity, Arc::new(Mutex::new(value_entity)));
		Ok(())
	}

	pub fn raw_put(&self, key: Entity, value: Entity) -> Option<Arc<Mutex<Entity>>> {
		self.data.insert(key, Arc::new(Mutex::new(value)))
	}

	fn get(&self, key_entity: &Entity) -> Result<Option<rustless::json::JsonValue>, PersistenceError> {
		match self.data.find(&key_entity) {
			Some(data) => {
				//data.get().lock();
				Table::entity_to_json(&data.get().lock().unwrap().clone(), &self.description.value)
				.map(|json_entity| Some(json_entity))
				.map_err(|err| PersistenceError::IoEntity(err))
			},
			None => Ok(None)
		}
	}

	fn get_copy_of_locked_value(locked_value: Arc<Mutex<Entity>>) -> Entity {
		let locked_value_unwrap = locked_value.lock().unwrap();
		locked_value_unwrap.clone()
	}

	fn get_lock_for_get(&self, tx_id: &u32, key_entity: &Entity) -> Result<Option<Entity>, PersistenceError> {
		let transaction = try!(self.tx_manager.get_tx(tx_id));
		let locked_transaction = transaction.lock().unwrap();
		let value_accessor = locked_transaction.get_locked_value(self.description.name.clone(), key_entity);
		match value_accessor {
			Some(locked_value) => {
				debug!("Lock for key = {} already taken", Table::entity_to_json(key_entity, &self.description.key).unwrap());
				Ok(Some(locked_value.value.clone()))
			},
			None => {
				debug!("Entity with key = {} not locked yet", Table::entity_to_json(key_entity, &self.description.key).unwrap());
				match self.data.find_mut(key_entity) {
					Some(mut accessor) => {
						match Table::get_lock_type(accessor.get().clone()) {
							LockType::Read => {
								let locked_value = TransactionManager::lock_value(tx_id, self, &locked_transaction, key_entity, Some(accessor.get().clone()));
								Ok(locked_value)
							},
							LockType::Write => Ok(Some(accessor.get().lock().unwrap().clone()))
						}
					},
					None => Ok(None) //Err(PersistenceError::EntityNotFound(key_entity.clone()))
				}
			}
		} 
	}

	fn get_lock_type(entity: Arc<Mutex<Entity>>) -> LockType {
		entity.lock().unwrap().lock.lock_type.clone()
	}

	fn get_lock_for_put(&self, tx_id: &u32, key_entity: &Entity, inserted_value: Arc<Mutex<Entity>>) -> Result<Option<Entity>, PersistenceError> {
		let transaction = try!(self.tx_manager.get_tx(tx_id));
		// Try get lock on key
		let locked_transaction = transaction.lock().unwrap();
		let locked_value = locked_transaction.get_locked_value(self.description.name.clone(), key_entity);
		//debug!("Contains test = {}", locked_transaction.locked_keys.find(key_entity).is_some());
		match locked_value {
			Some(value) => {
				debug!("In tx {} found value {} by key {}", tx_id,
					Table::entity_to_json(&value.value, &self.description.value).unwrap(),
					Table::entity_to_json(key_entity, &self.description.key).unwrap());
				Ok(Some(value.value.clone()))
			},
			None => {
				debug!("Entity with key = {} not locked yet", Table::entity_to_json(key_entity, &self.description.key).unwrap());
				match self.data.find_mut(key_entity) {
					Some(mut accessor) => {
						TransactionManager::lock_value(tx_id, self, &locked_transaction, key_entity, Some(accessor.get().clone()));
						//let res = TransactionManager::lock_value(tx_id, self, &locked_transaction, key_entity, value_entity);
						Ok(None)
					},
					None => {
						debug!("Tx not contains key yet. Add key {}", Table::entity_to_json(key_entity, &self.description.key).unwrap());
						let mut new_key_entity = key_entity.clone();
						new_key_entity.lock.tx_id = tx_id.clone();
						locked_transaction.add_entity(self, key_entity.clone(), None, inserted_value.lock().unwrap().clone());
						//TransactionManager::lock_value(tx_id, self, &locked_transaction, &new_key_entity, Some(inserted_value));
						debug!("Return ok for get_lock_for_put");
						Ok(None)
					}
				}
			}
		}
	}

	pub fn tx_get(&self, tx_id: &u32, key: &rustless::json::JsonValue) -> Result<Option<rustless::json::JsonValue>, PersistenceError> {
		let key_entity = try!(Table::json_to_entity(key, &self.description.key).map_err(|err| PersistenceError::IoEntity(err)));
		let locked_value: Option<Entity> = try!(self.get_lock_for_get(tx_id, &key_entity));
		match locked_value {
			Some(value) => {
				debug!("In current tx found value = {} by key = {}", 
					Table::entity_to_json(&value, &self.description.value).unwrap(),
					Table::entity_to_json(&key_entity, &self.description.key).unwrap());
				Table::entity_to_json(&value, &self.description.value)
					.map(|r| Some(r))
					.map_err(|err| PersistenceError::IoEntity(err))
			},
			None => {
				debug!("In current tx not found value for key = {}", 
					Table::entity_to_json(&key_entity, &self.description.key).unwrap());
				self.get(&key_entity)
			}
		}
	}

	fn get_lock_value(value: Arc<Mutex<Entity>>) -> bool {
		value.lock().unwrap().lock.is_locked()
	}

	pub fn tx_put(&self, tx_id: &u32, key: &rustless::json::JsonValue, value: &rustless::json::JsonValue) -> Result<(), PersistenceError> {
		debug!("Tx put started");
		let key_entity: Entity = try!(Table::json_to_entity(key, &self.description.key).map_err(|err| PersistenceError::IoEntity(err)));
		let value_entity = try!(Table::json_to_entity(value, &self.description.value).map_err(|err| PersistenceError::IoEntity(err)));
		let inserted_value = Arc::new(Mutex::new(value_entity));
		let lock_result = try!(self.get_lock_for_put(tx_id, &key_entity, inserted_value.clone()));

		debug!("Upsert value = {} with key = {}", Table::entity_to_json(&inserted_value.lock().unwrap(), &self.description.value).unwrap(),
			 Table::entity_to_json(&key_entity, &self.description.key).unwrap());

		//self.data.upsert(key_entity, inserted_value.clone(), &|value| *value = inserted_value.clone());
		for (k, v) in self.data.iter() {
			debug!("    Current data: {} -> {}", Table::entity_to_json(k, &self.description.key).unwrap(),
				Table::entity_to_json(&v.lock().unwrap(), &self.description.value).unwrap());
		}
		Ok(())
	}
}

impl DataBaseManager {
    pub fn new() -> Result<DataBaseManager, String> {
        let mut db_manager = DataBaseManager { 
            type_descriptions: BTreeMap::new(),
            table_descriptions: ConcHashMap::<String, TableDescription>::new(),
			tables: ConcHashMap::<String, Arc<Table>>::new(),
			tx_manager: Arc::new(TransactionManager::new()) };

        let string_type = TypeDescription {
            name: "String".to_string(),
            reader: Box::new(move |json| {
                match json.clone() {
                    rustless::json::JsonValue::String(value) => 
						encode(&value.clone(), bincode::SizeLimit::Infinite).map_err(|err| IoEntityError::Read(err.to_string())),
                    _ => Err(IoEntityError::Read(String::from("Expected type String")))
                }
            }),
            writer: Box::new(|value: &Vec<u8>| {
                let string: String = try!(decode(&value[..]).map_err(|err| IoEntityError::Write(err.to_string())));
                Ok(rustless::json::JsonValue::String(string))
            })
        };

        let u64_type = TypeDescription {
            name: "u64".to_string(),
            reader: Box::new(move |json| {
                match json.clone() {
                    rustless::json::JsonValue::U64(value) =>
						encode(&value.clone(), bincode::SizeLimit::Infinite)
							.map_err(|err| IoEntityError::Read(err.to_string())),
                    _ => Err(IoEntityError::Read(String::from("Expected type u64")))
                }
            }),
            writer: Box::new(|ref value| {
				let u64_value = try!(decode(&value[..]).map_err(|err| IoEntityError::Write(err.to_string())));
                Ok(rustless::json::JsonValue::U64(u64_value))
            })
        };

		let i64_type = TypeDescription {
			name: "i64".to_string(),
			reader: Box::new(|ref json| {
				match *json {
					&rustless::json::JsonValue::I64(value) => 
						encode(&value, bincode::SizeLimit::Infinite).map_err(|err| IoEntityError::Read(err.to_string())),
					_ => Err(IoEntityError::Read(String::from("Expected type i64")))
				}
			}),
			writer: Box::new(|ref value| {
				let i64_value = try!(decode(&value[..]).map_err(|err| IoEntityError::Write(err.to_string())));
				Ok(rustless::json::JsonValue::I64(i64_value))
			})
		};

		try!(db_manager.add_type(u64_type));
		try!(db_manager.add_type(string_type));
		try!(db_manager.add_type(i64_type));

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

    pub fn get_tables_json_list(&self) -> rustless::json::JsonValue {
        let res = self.table_descriptions.iter().map(|(k, v)| {
			(k.clone(), v.to_json())
		}).collect();
		rustless::json::JsonValue::Object(res)
    }

	pub fn get_table_json(&self, name: &String) -> Option<rustless::json::JsonValue> {
		//self.table_descriptions.find(name).map(|table| { table.get().to_json() })
		self.tables.find(name).map(|table| { table.get().description.to_json() })
	} 

	pub fn get_table(&self, name: &String) -> Option<Arc<Table>> {
		self.tables.find(name).map(|accessor| accessor.get().clone())
	}

	/** Add new table by he view description
	 * return - table name or error description is adding fail */
    pub fn add_table(&self, table_description: TableDescriptionView) -> Result<String, String> {
		if !self.table_descriptions.find(&table_description.name).is_some() {
        	let table_desc = try!(TableDescription::from_view(&table_description, &self.type_descriptions));
			self.tables.insert(
				table_desc.name.clone(), 
				Arc::new(Table { description: table_desc, data: ConcHashMap::<Entity, Arc<Mutex<Entity>>>::new(), tx_manager: self.tx_manager.clone() }));
			//self.table_descriptions.insert(table_desc.name.clone(), table_desc);
			Ok(table_description.name.clone())
		} else {
			Err("Table with name ".to_string() + table_description.name.as_str() + " already exists.")
		}
    }

	pub fn add_data(&self,
			tx_id: &u32,
			table_name: &String, 
			key: &rustless::json::JsonValue,
			value: &rustless::json::JsonValue) -> Result<(), PersistenceError> {
		match self.tables.find(table_name) {
			Some(table) => table.get().tx_put(tx_id, key, value),
			None => Err(PersistenceError::TableNotFound(table_name.clone()))
		}
	}

	pub fn get_data(&self,
			tx_id: &u32,
			table_name: &String,
			key: &rustless::json::JsonValue) -> Result<Option<rustless::json::JsonValue>, PersistenceError> {
		let table = try!(self.tables.find(table_name).ok_or(PersistenceError::TableNotFound(table_name.clone())));
		table.get().tx_get(tx_id, key)
	}
	
	pub fn tx_start(&self) -> Result<u32, PersistenceError> {
		self.tx_manager.start()
	}

	pub fn tx_stop(&self, tx_id: &u32) -> Result<(), PersistenceError> {
		self.tx_manager.stop(self, tx_id)
	}

	pub fn tx_rollback(&self, tx_id: &u32) -> Result<(), PersistenceError> {
		self.tx_manager.rollback(self, tx_id)
	}
}