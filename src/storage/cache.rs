extern crate time;

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use std::marker::PhantomData;
use std::cmp::Eq;
use std::hash::Hash;
use std::option::Option;
use std::sync::{Arc, Mutex};
use self::time::SteadyTime;
use std::thread;
use std::cell::Cell;

use storage::transaction::{TransactionManager, Transaction, TransactionMode};

pub struct CacheConfiguration<K: Eq + Hash, V> {
	pub name: String,
	phantom_key: PhantomData<K>,
	phantom_value: PhantomData<V>,
}

impl <'a, K: 'a + Eq + Hash, V: 'a> CacheConfiguration<K, V> {
	pub fn new(name: String) -> CacheConfiguration<K, V> {
		CacheConfiguration { name: name, phantom_key: PhantomData, phantom_value: PhantomData }
	}	
}

trait Locked<V> {
	fn try_read(&mut self, transaction: &Transaction, config: LockConfig) -> Option<&V>;
	fn free_lock(&mut self);
}

struct LockConfig {
	read: bool,
	write: bool,
}

struct Lock {
	set: bool,
	transaction_id: u64,
	config: LockConfig,
}

struct CacheValue<V> {
	value: V,
	lock: Lock,
	time: SteadyTime,
}

impl <V> Locked<V> for CacheValue<V> {
	fn try_read(&mut self, transaction: &Transaction, config: LockConfig) -> Option<&V> {
		if !self.lock.set || self.lock.transaction_id == transaction.id { 
			self.lock.set = true;
			self.lock.transaction_id = transaction.id;
			self.lock.config = config;
			Some(&self.value)
		} else {
			None
		}
	}

	fn free_lock(&mut self) {
		self.lock.set = false
	}
}

impl <V> CacheValue<V> {
	fn new(value: V) -> CacheValue<V> {
		CacheValue { 
			value: value, 
			lock: Lock { 
				set: false, 
				config: 
					LockConfig { 
						read: false, 
						write: true 
					}, 
				transaction_id: 0
			},
			time: SteadyTime::now()
		}
	}
}

pub struct Cache<K: Eq + Hash, V> {
	config: CacheConfiguration<K, V>,
	map: HashMap<K, VecDeque<CacheValue<V>>>, // last values(by time) place in head of queue
	transaction_values: HashMap<u64, HashSet<K>>, // set of values, that was modified in tx with id = key
	transaction_manager: Arc<TransactionManager>,
}

impl <K: Eq + Hash, V> Cache<K, V> {
	pub fn put(&mut self, key: K, value: V, transaction: &Transaction) {
		if !self.map.contains_key(&key) {
			let mut values = VecDeque::new();
			values.push_front(CacheValue::new(value));
			self.map.insert(key, values); 
		} else {
			self.map.get_mut(&key).unwrap().push_front(CacheValue::new(value));
		}
	}

	pub fn get(&mut self, key: K, transaction: &Transaction) -> Option<&V> {
		// TODO: iterate values while not find first with time < tx.statr_time. If not then return None.
		self.map.get_mut(&key).map(|values| {
				let value_opt = values.front_mut();
				match value_opt {
					Some(value) => value.try_read(transaction, LockConfig { read: true, write: true }),
					None => None
				}
			}).unwrap_or(None)
	}

	fn rollback_value(map: &mut HashMap<K, VecDeque<CacheValue<V>>>, transaction: &Transaction, key: &K) {
		match map.get_mut(key) {
			Some(values) => {
				let mut end = match values.front() {
					Some(value) => value.time.lt(&transaction.start_time),
					None => true,
				};
				while !end {
					end = match values.pop_front() {
						Some(value) => { 
							value.time.lt(&transaction.start_time) 
						},
						None => true,
					}
				}
			},
			None => {},
		};
	}

	pub fn rollback(&mut self, transaction: &Transaction) {
		let values = self.transaction_values.get(&transaction.id);
		match values {
			Some(modified_values) => {
				for key in modified_values.iter() {
					Cache::rollback_value(&mut self.map, transaction, key);
				}
			},
			None => {},
		};
	}
}

pub struct CacheManager {
	pub transaction_manager: Arc<TransactionManager>,
}

pub struct TestS {
	name: String,
}

impl CacheManager {
	pub fn new() -> CacheManager {
		CacheManager { transaction_manager: Arc::new(TransactionManager::new()) }
	}

	pub fn create_cache<'a, K: Eq + Hash, V>(&'a self, config: CacheConfiguration<K, V>) -> Cache<K, V> {
		Cache { config: config, map: HashMap::new(), transaction_values: HashMap::new(), transaction_manager: self.transaction_manager.clone() }
	}
}