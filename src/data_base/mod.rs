extern crate iron;
extern crate concurrent_hashmap;
extern crate bincode;
extern crate serde_json;

use std;
use std::hash::{Hash, Hasher, SipHasher};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::boxed::Box;
use std::fmt::{Debug, Display};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, Condvar, LockResult};
use std::ops::Deref;

use concurrent_hashmap::*;

use bincode::rustc_serialize::{encode, decode};

use rustless::{self};
use rustless::json::ToJson;

pub mod app_extension;
pub mod meta;

use data_base::meta::{TypeDescription, EntityDescription, TableDescription, TableDescriptionView};

// Top struct for interaction with tables
pub struct DataBaseManager {
    type_descriptions: BTreeMap<String, Arc<Box<TypeDescription>>>,
    table_descriptions: ConcHashMap<String, TableDescription>, //BTreeMap::<String, TableDescription>::new();
	tables: ConcHashMap<String, Table>,
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

#[derive(Debug, Clone)]
struct Lock {
	lock_type: LockType,
	tx_id: u32,
	condition: Arc<(Mutex<bool>, Condvar)>
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
enum LockType {
	Read,
	Write
}

// Persistence for concrete entity structure
pub struct Table {
	description: TableDescription,
	data: ConcHashMap<Entity, Arc<Mutex<Entity>>>,
	tx_manager: Arc<TransactionManager>
}

// Struct for store data of transaction
struct Transaction {
	id: u32,
	on: bool, // true - transaction is executed
	locked_keys: Box<ConcHashMap<Entity, Arc<Mutex<Entity>>>> // keys and refs to values of locked entities
}

// Transactions data driver
struct TransactionManager {
	counter: Arc<Mutex<u32>>, // beacause need check overflow and get new value - AtomicUsize is not relevant
	transactions: ConcHashMap<u32, Arc<Mutex<Transaction>>>
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
	UndefinedTransaction,
	TransactionAlreadyStarted(u32),
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

impl Lock {
	fn new() -> Lock {
		Lock { lock_type: LockType::Read, tx_id: 0, condition: Arc::new((Mutex::new(false), Condvar::new())) }
	}
}

impl PartialEq for Lock {
	fn eq(&self, other: &Lock) -> bool {
		self.tx_id == other.tx_id && self.lock_type == other.lock_type
	}
}

impl Eq for Lock {}

impl Hash for Lock {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tx_id.hash(state);
        self.lock_type.hash(state);
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

fn my_hash<T>(obj: T) -> u64
    where T: Hash
{
    let mut hasher = SipHasher::new();
    obj.hash(&mut hasher);
    hasher.finish()
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

	fn lock_value(tx_id: &u32, key_desc: &EntityDescription, mut locked_transaction: &Transaction, key_entity: &Entity, value_entity: Arc<Mutex<Entity>>) -> Entity {
		let mut temp = value_entity.clone();
		let mut mut_value_entity: MutexGuard<Entity> = temp.lock().unwrap();
		debug!("Lock for key {} is taken; lock id on key = {}, prev tx_id = {}", 
				Table::entity_to_json(key_entity, key_desc).unwrap(), mut_value_entity.lock.tx_id, mut_value_entity.lock.tx_id);
		if mut_value_entity.lock.tx_id != *tx_id {
			let ref mut lock_mut = mut_value_entity.lock;
			let &(ref lock_var, ref condvar) = &*lock_mut.condition;
			let mut locked = lock_var.lock().unwrap();
			while *locked {
				locked = condvar.wait(locked).unwrap();
			}
			*locked = true;
			lock_mut.tx_id = tx_id.clone();
			locked_transaction.add_entity(key_entity.clone(), value_entity.clone());
			debug!("Lock for key {} is set, tx updated", Table::entity_to_json(key_entity, key_desc).unwrap());
		}
		mut_value_entity.clone()
	}

	fn get_lock_for_get(&self, tx_id: &u32, key_entity: &Entity) -> Result<Entity, PersistenceError> {
		let transaction = try!(self.tx_manager.get_tx(tx_id));
		let mut locked_transaction = transaction.lock().unwrap();
		if !locked_transaction.locked_keys.find(key_entity).is_some() {
			debug!("Entity with key = {} not locked yet", Table::entity_to_json(key_entity, &self.description.key).unwrap());
			match self.data.find_mut(key_entity) {
				Some(mut accessor) => {
					let value_entity: Entity = Table::lock_value(tx_id, &self.description.key, &locked_transaction, key_entity, *accessor.get());
					Ok(key_entity.clone())
				},
				None => Err(PersistenceError::EntityNotFound(key_entity.clone()))
			}
		} else {
			debug!("Lock for key = {} already taken", Table::entity_to_json(key_entity, &self.description.key).unwrap());
			Ok(key_entity.clone())
		}
	}

	fn get_lock_for_put(&self, tx_id: &u32, key_entity: &Entity, value_entity: Arc<Mutex<Entity>>) -> Result<Entity, PersistenceError> {
		let transaction = try!(self.tx_manager.get_tx(tx_id));
		// Try get lock on key
		let mut locked_transaction = transaction.lock().unwrap();
		debug!("Contains test = {}", locked_transaction.locked_keys.find(key_entity).is_some());
		if !locked_transaction.locked_keys.find(key_entity).is_some() {
			debug!("Entity with key = {} not locked yet", Table::entity_to_json(key_entity, &self.description.key).unwrap());
			match self.data.find_mut(key_entity) {
				Some(mut accessor) => {
					let value_entity: Entity = Table::lock_value(tx_id, &self.description.key, &locked_transaction, key_entity, *accessor.get());
					Ok(value_entity)
				},
				None => {
					debug!("Tx not contains key yet. Add key {}", Table::entity_to_json(key_entity, &self.description.key).unwrap());
					let mut new_key_entity = key_entity.clone();
					new_key_entity.lock.tx_id = tx_id.clone();
					let res = Table::lock_value(tx_id, &self.description.key, &locked_transaction, &new_key_entity, value_entity);
					//locked_transaction.add_key(new_key_entity.clone(), );
					Ok(res)
				}
			}
		} else {
			debug!("Lock for key = {} already taken", Table::entity_to_json(key_entity, &self.description.key).unwrap());
			match self.data.find(key_entity) {
				Some(mut accessor) => Ok(accessor.get().lock().unwrap().clone()),
				None => Err(PersistenceError::EntityNotFound(key_entity.clone()))
			}
		}
	}

	/* Try get lock for key_entity. Return copy of locked key with set tx_id. */
	/*fn get_lock(&self, tx_id: &u32, key_entity: &Entity, value_entity: Arc<Mutex<Entity>>) -> Result<Entity, PersistenceError> {
		debug!("get_lock started");
		let transaction = try!(self.tx_manager.get_tx(tx_id));
		debug!("Tx id = {} for locking found", tx_id);
		// Try get lock on key
		let mut locked_transaction = transaction.lock().unwrap();
		for (k, v) in locked_transaction.locked_keys.iter() {
			debug!("    tx[{}] locked key {}, k==key_entity = {}, k.hash = {}, key_entity.hash = {}", tx_id, 
				Table::entity_to_json(&k, &self.description.key).unwrap(),
				key_entity.eq(k), my_hash(k), my_hash(key_entity));
		}
		debug!("Contains test = {}", locked_transaction.locked_keys.find(key_entity).is_some());
		if !locked_transaction.locked_keys.find(key_entity).is_some() {
			debug!("Entity with key = {} not locked yet", Table::entity_to_json(key_entity, &self.description.key).unwrap());
			match self.data.find_mut(key_entity) {
				Some(mut accessor) => {
					let value_entity: Entity = Table::lock_value(tx_id, &self.description.key, &locked_transaction, key_entity, *accessor.get());
					Ok(value_entity)
				},
				None => {
					debug!("Tx not contains key yet. Add key {}", Table::entity_to_json(key_entity, &self.description.key).unwrap());
					let mut new_key_entity = key_entity.clone();
					new_key_entity.lock.tx_id = tx_id.clone();
					let res = Table::lock_value(tx_id, &self.description.key, &locked_transaction, &new_key_entity, value_entity);
					//locked_transaction.add_key(new_key_entity.clone(), );
					Ok(res)
				}
			}
		} else {
			debug!("Lock for key = {} already taken", Table::entity_to_json(key_entity, &self.description.key).unwrap());
			Ok(key_entity.clone())
		}
	}*/

	pub fn tx_get(&self, tx_id: &u32, key: &rustless::json::JsonValue) -> Result<Option<rustless::json::JsonValue>, PersistenceError> {
		let key_entity = try!(Table::json_to_entity(key, &self.description.key).map_err(|err| PersistenceError::IoEntity(err)));
		try!(self.get_lock_for_get(tx_id, &key_entity));
		self.get(&key_entity)
	}

	fn set_lock(entity: &Entity) {
		//let entity = entity_mutext.get_mut();
		let &(ref lock_var, ref condvar) = &*entity.lock.condition;
		*lock_var.lock().unwrap() = true;
	}

	pub fn tx_put(&self, tx_id: &u32, key: &rustless::json::JsonValue, value: &rustless::json::JsonValue) -> Result<(), PersistenceError> {
		debug!("Tx put started");
		let mut key_entity: Entity = try!(Table::json_to_entity(key, &self.description.key).map_err(|err| PersistenceError::IoEntity(err)));
		let mut value_entity = try!(Table::json_to_entity(value, &self.description.value).map_err(|err| PersistenceError::IoEntity(err)));
		Table::set_lock(&value_entity);
		//value_entity.lock.tx_id = tx_id.clone();
		let new_value_entity = try!(self.get_lock_for_put(tx_id, &key_entity, value_entity));
		/*match self.data.insert(key_entity.clone(), inserted_value.clone()) {
			Some(prev) => { try!(self.get_lock(tx_id, &key_entity)); },
			None => {}
		};*/
		debug!("Upsert value = {} with key = {}", Table::entity_to_json(&new_value_entity, &self.description.value).unwrap(),
			 Table::entity_to_json(&key_entity, &self.description.key).unwrap());
		let inserted_value = Arc::new(Mutex::new(new_value_entity));
		self.data.upsert(key_entity, inserted_value.clone(), &|value| *value = inserted_value.clone());
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
			tables: ConcHashMap::<String, Table>::new(),
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
						encode(&value.clone(), bincode::SizeLimit::Infinite).map_err(|err| IoEntityError::Read(err.to_string())),
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
			self.tables.insert(
				table_desc.name.clone(), 
				Table { description: table_desc, data: ConcHashMap::<Entity, Arc<Mutex<Entity>>>::new(), tx_manager: self.tx_manager.clone() });
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
		self.tx_manager.stop(tx_id)
	}
}

impl TransactionManager {
	fn new() -> TransactionManager {
		TransactionManager { counter: Arc::new(Mutex::new(1)), transactions: ConcHashMap::<u32, Arc<Mutex<Transaction>>>::new() }
	}

	fn get_tx_id(&self) -> u32 {
		let counter = self.counter.clone();
		let mut counter_mut = counter.lock().unwrap();
		if counter_mut.eq(&u32::max_value()) {
				*counter_mut = 1;
		};
		let res = counter_mut.clone();
		*counter_mut = *counter_mut + 1;
		res
	}
	
	fn get_tx(&self, tx_id: &u32) -> Result<Arc<Mutex<Transaction>>, PersistenceError> {
		match self.transactions.find(&tx_id) {
			Some(transaction) => {
				debug!("Found tx with id = {}", tx_id);
				Ok(transaction.get().clone())
			},
			None => {
				debug!("Tx with id = {} not found", tx_id);
				Err(PersistenceError::UndefinedTransaction)
			}
		}
	}

	fn start(&self) -> Result<u32, PersistenceError> {
		let id = self.get_tx_id();
		let transaction = Arc::new(Mutex::new(Transaction { id: id, on: true, locked_keys: Box::new(HashSet::<Entity>::new()) }));
		match self.transactions.insert(id, transaction) {
			Some(value) => {
				error!("Tx with id = {} already started", id);
				Err(PersistenceError::TransactionAlreadyStarted(id))
			},
			None => {
				debug!("Tx with id = {} started", id);
				Ok(id)
			}
		}
	}

	fn stop(&self, id: &u32) -> Result<(), PersistenceError> {
		match self.transactions.remove(&id) {
			Some(transaction) => {
				let mut locked_transaction = transaction.lock().unwrap();
				for locked_key in locked_transaction.locked_keys.iter_mut() {

					let ref mut lock_mut = locked_key.lock;
					let &(ref lock_var, ref condvar) = &*lock_mut.condition; //&*mut_value_entity.lock.condition;
					let mut locked = lock_var.lock().unwrap();
					*locked = false;
					lock_mut.tx_id = 0;
				};
				locked_transaction.locked_keys.clear();
				debug!("Tx with id = {} stopped", id);
				Ok(())
			},
			None => Err(PersistenceError::UndefinedTransaction)
		}
	}
}

impl Transaction {
	fn add_entity(&mut self, key: Entity, value: Arc<Mutex<Entity>>) -> bool {
		self.locked_keys.as_mut().insert(key, value)
	}

	fn remove_key(&mut self, key: Entity) -> bool {
		self.locked_keys.as_mut().remove(&key)
	}
}