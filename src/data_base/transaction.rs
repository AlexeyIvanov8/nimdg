
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
use std::fmt;
use std::fmt::{Debug, Display};
use std::sync::{Mutex, MutexGuard, Condvar};

use concurrent_hashmap::*;

use bincode::rustc_serialize::{encode, decode};

use rustless::{self};
use rustless::json::ToJson;

use data_base::{DataBaseManager, Entity, PersistenceError, Table};
use data_base::meta::EntityDescription;

const DEFAULT_TX_ID: u32 = 0;

#[derive(Debug, Clone)]
pub struct Lock {
	pub lock_type: LockType,
	pub tx_id: u32,
	condition: Arc<(Mutex<bool>, Condvar)>
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub enum LockType {
	Read,
	Write
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct LockedKey {
	table_name: String,
	key: Entity
}

#[derive(Clone)]
pub struct LockedValue {
	reference: Option<Arc<Mutex<Entity>>>, // reference to entity in table, if value is new, then none
	pub value: Entity // actual value in tx
}

// Struct for store data of transaction
pub struct Transaction {
	id: u32,
	on: bool, // true - transaction is executed
	locked_keys: Arc<ConcHashMap<LockedKey, LockedValue>> // keys and refs to values of locked entities
}

// Transactions data driver
pub struct TransactionManager {
	counter: Arc<Mutex<u32>>, // beacause need check overflow and get new value - AtomicUsize is not relevant
	transactions: ConcHashMap<u32, Arc<Mutex<Transaction>>>
}

impl Lock {
	pub fn new() -> Lock {
		Lock { lock_type: LockType::Write, tx_id: 0, condition: Arc::new((Mutex::new(false), Condvar::new())) }
	}

	pub fn is_locked(&self) -> bool {
		let &(ref lock_var, _) = &*self.condition;
		let locked = lock_var.lock().unwrap();
		*locked
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

impl LockedValue {
	fn update_reference(&self) {
		match self.reference {
			Some(ref reference) => {
				let mut locked_reference = reference.lock().unwrap();
				let ref mut deref = *locked_reference;
				deref.fields = self.value.fields.clone();
			},
			None => {}
		}
	}
}

impl fmt::Debug for LockedValue {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{{reference = {:?}, value = {:?} }}", self.reference, self.value)
	}
}

impl PartialEq for LockedValue {
	fn eq(&self, other: &LockedValue) -> bool {
		self.value == other.value
	}
}

impl Eq for LockedValue {}

impl Hash for LockedValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl TransactionManager {
	pub fn new() -> TransactionManager {
		TransactionManager { 
			counter: Arc::new(Mutex::new(1)),
			transactions: ConcHashMap::<u32, Arc<Mutex<Transaction>>>::new()
		}
	}

	pub fn get_tx_id(&self) -> u32 {
		let counter = self.counter.clone();
		let mut counter_mut = counter.lock().unwrap();
		if counter_mut.eq(&u32::max_value()) {
				*counter_mut = 1;
		};
		let res = counter_mut.clone();
		*counter_mut = *counter_mut + 1;
		res
	}
	
	pub fn get_tx(&self, tx_id: &u32) -> Result<Arc<Mutex<Transaction>>, PersistenceError> {
		match self.transactions.find(&tx_id) {
			Some(transaction) => {
				debug!("Found tx with id = {}", tx_id);
				Ok(transaction.get().clone())
			},
			None => {
				debug!("Tx with id = {} not found", tx_id);
				Err(PersistenceError::UndefinedTransaction(tx_id.clone()))
			}
		}
	}

	pub fn start(&self) -> Result<u32, PersistenceError> {
		let id = self.get_tx_id();
		let transaction = Arc::new(Mutex::new(Transaction { 
			id: id,
			on: true, 
			locked_keys: Arc::new(ConcHashMap::<LockedKey, LockedValue>::new()) 
		}));
		match self.transactions.insert(id, transaction) {
			Some(_) => {
				error!("Tx with id = {} already started", id);
				Err(PersistenceError::TransactionAlreadyStarted(id))
			},
			None => {
				debug!("Tx with id = {} started", id);
				Ok(id)
			}
		}
	}
	
	pub fn stop(&self, data_base_manager: &DataBaseManager, id: &u32) -> Result<(), PersistenceError> {
		debug!("Begin stop tx {}", id);
		match self.transactions.remove(&id) {
			Some(transaction) => {
				let locked_transaction = transaction.lock().unwrap();
				debug!("Lock tx for stop {}", locked_transaction.id);
				for (locked_key, locked_value) in locked_transaction.locked_keys.iter() {
					locked_value.update_reference();
					try!(TransactionManager::unlock_value(locked_transaction.id.clone(), data_base_manager, locked_key, locked_value));
				};
				locked_transaction.locked_keys.clear();
				debug!("Tx with id = {} stopped", id);
				Ok(())
			},
			None => Err(PersistenceError::UndefinedTransaction(id.clone()))
		}
	}

    fn unlock_value(tx_id: u32, data_base_manager: &DataBaseManager, locked_key: &LockedKey, locked_value: &LockedValue) -> Result<(), PersistenceError> {
		match locked_value.reference {
			Some(ref value_entity) => {
				let mut mut_value_entity: MutexGuard<Entity> = value_entity.lock().unwrap();
				debug!("Unlock key for tx {}", tx_id);
				if mut_value_entity.lock.tx_id != tx_id {
					trace!("Current tx = {}, value tx = {}", tx_id, mut_value_entity.lock.tx_id);
					Err(PersistenceError::WrongTransaction(mut_value_entity.lock.tx_id.clone(), tx_id.clone()))
				} else {
					let ref mut mut_lock: Lock = mut_value_entity.lock;
					let &(ref lock_var, _) = &*mut_lock.condition;
					let mut locked = lock_var.lock().unwrap();
					*locked = false;
					mut_lock.tx_id = DEFAULT_TX_ID;
					Ok(())
				}
			},
			None => {
				let table: Arc<Table> = data_base_manager.get_table(&locked_key.table_name).unwrap();
				table.raw_put(locked_key.key.clone(), locked_value.value.clone());
				Ok(())
			}
		}
	}

	pub fn lock_value(
			tx_id: &u32, 
			table: &Table, 
			locked_transaction: &Transaction, 
			key_entity: &Entity, 
			value_entity_opt: Option<Arc<Mutex<Entity>>>) -> Option<Entity> {
		value_entity_opt.map(|value_entity| {
			let temp = value_entity.clone();
			let mut mut_value_entity: MutexGuard<Entity> = temp.lock().unwrap();
			let copy_value = mut_value_entity.clone();
			debug!("Lock for key {} is taken; lock id on key = {}, prev tx_id = {}", 
					Table::entity_to_json(key_entity, &table.description.key).unwrap(), mut_value_entity.lock.tx_id, mut_value_entity.lock.tx_id);
			if mut_value_entity.lock.tx_id != *tx_id {
				let ref mut lock_mut = mut_value_entity.lock;
				let &(ref lock_var, ref condvar) = &*lock_mut.condition;
				let mut locked = lock_var.lock().unwrap();
				debug!("Current locked = {}", *locked);
				while *locked {
					debug!("While locked = {}", *locked);
					locked = condvar.wait(locked).unwrap();
				}
				debug!("Lock taken = {}", *locked);
				*locked = true;
				debug!("Lock taken2 = {}", *locked);
				lock_mut.tx_id = tx_id.clone();
				locked_transaction.add_entity(table, key_entity.clone(), Some(value_entity.clone()), copy_value);
				debug!("Lock for key {} is set, tx updated", Table::entity_to_json(key_entity, &table.description.key).unwrap());
			}
			debug!("Value locked");
			mut_value_entity.clone()
		})
	}
}

impl Transaction {
	pub fn add_entity(&self, table: &Table, key: Entity, value: Option<Arc<Mutex<Entity>>>, copy_value: Entity) -> bool {
        //let copy_value = value.as_ref().lock().unwrap().clone();
		self.locked_keys.insert(
			LockedKey{ table_name: table.description.name.clone(), key: key }, 
			LockedValue { reference: value, value: copy_value }).is_none()
	}

	fn remove_key(&self, key: LockedKey) -> bool {
		self.locked_keys.remove(&key).is_some()
	}

	pub fn get_locked_value(&self, table_name: String, key: &Entity) -> Option<&LockedValue> {
		self.locked_keys.find(&LockedKey { table_name: table_name, key: key.clone() }).map(|accessor| accessor.get())
	}
}