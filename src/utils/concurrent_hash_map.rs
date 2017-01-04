// kill me, please
use std::collections::HashMap;
use std::sync::Mutex;
use std::cmp::Eq;
use std::hash::Hash;
use std::borrow::Borrow;

pub struct ConcurrentHashMap<K: Hash + Eq, V> {
	map: Mutex<HashMap<K, V>>,
}

impl <K: Hash + Eq, V> ConcurrentHashMap<K, V> {
	/*pub fn new() -> ConcurrentHashMap<K, V> {
		ConcurrentHashMap { map: Mutex::new(HashMap::new()) }
	}

	pub fn insert(&self, key: K, value: V) -> Option<V> {
		self.map.lock().unwrap().insert(key, value)
	}
	
	pub fn get<'a, Q: ?Sized>(&'a self, key: &Q) -> Option<&V> where K: Borrow<Q> + Hash + Eq + Send + Sync, Q: Hash + Eq + Sync {
		match self.map.try_lock() {
			Ok(map) => map.get(key),
			Err(err) => panic!("Error {}", err),
		}
	}

	pub fn contains_key(&self, key: &K) -> bool {
		self.map.lock().unwrap().contains_key(key)
	}

	pub fn remove(&self, key: &K) -> Option<V> {
		self.map.lock().unwrap().remove(key)
	}*/
}