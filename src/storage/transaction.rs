extern crate time;
extern crate concurrent_hashmap;

use self::concurrent_hashmap::*;
use self::time::SteadyTime;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::collections::HashSet;
use std::sync::Arc;
//use utils::concurrent_hash_map::ConcurrentHashMap;

#[derive(Clone)]
pub struct Transaction {
	pub id: u64,
	pub start_time: SteadyTime,
	mode: TransactionMode,
	modifed_caches: Arc<HashSet<String>>,
}

#[derive(Clone)]
pub enum TransactionMode {
	ReadUncommited,
	ReadCommited,
	RepeatebleRead,
	Serialized,
}

pub struct TransactionManager {
	counter: AtomicUsize,
	map: ConcHashMap<u64, Transaction>,
}

impl TransactionManager {
	pub fn new() -> TransactionManager {
		TransactionManager { counter: AtomicUsize::new(0), map: ConcHashMap::<u64, Transaction>::new() }
	}

	fn next_index(&self) -> u64 {
		self.counter.fetch_add(1, Ordering::SeqCst) as u64
	}

	pub fn start(&self, mode: TransactionMode) -> Transaction {
		let count = self.next_index();
		let transaction = Transaction { id: count, start_time: SteadyTime::now(), mode: mode, modifed_caches: Arc::new(HashSet::new()) };
		self.map.insert(count, transaction.clone());
		println!("Tranasaction {} was opened", transaction.id);
		transaction
	}

	pub fn commit(&self, transaction: &Transaction) {
		if self.map.find(&transaction.id).is_none() {
			panic!("Transaction with id = {} not exists and can't be closed", &transaction.id);
		}
		else {
			let closed_transaction = self.map.remove(&transaction.id);
			println!("Transaction {} was closed", &closed_transaction.unwrap().id);
		}
	}

	pub fn rollback(&self, transaction: &Transaction) {
		if self.map.find(&transaction.id).is_none() {
			panic!("Transaction with id = {} not exists and can't be closed", &transaction.id);
		}
		else {
			println!("Transaction must be rolled. Not implemented yet. {}", &transaction.id);
		}
	}
}