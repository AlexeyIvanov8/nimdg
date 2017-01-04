mod storage;
mod utils;

use storage::cache::{CacheManager, CacheConfiguration};
use storage::transaction::{TransactionMode};
use std::collections::HashMap;

fn main() {
    println!("Hello, world!");
    println!("Test = {}", std::mem::size_of::<char>());

    let mut test = HashMap::<u32, String>::new();
    test.insert(32, String::from("val342"));
    test.insert(1, String::from("test_val6"));
    
    let mut cache_manager = CacheManager::new();
    let cache1 = cache_manager.create_cache(CacheConfiguration::<u32, String>::new(String::from("cache_one")));
    let cache2 = cache_manager.create_cache(CacheConfiguration::<u64, String>::new(String::from("cache_two")));
    
    //let cache1 = cache_manager.test::<u32, String>(CacheConfiguration::<u32, String>::new(String::from("cache_two")));//String::from("rr"));
    //let cache2 = cache_manager.test::<u32, String>(CacheConfiguration::<u32, String>::new(String::from("cache_two")));//String::from("vrvdv"));
    let tx_ext = cache_manager.transaction_manager.start(TransactionMode::ReadUncommited);
    let tx = cache_manager.transaction_manager.start(TransactionMode::ReadUncommited);

    let mut simple_cache = cache_manager.create_cache(CacheConfiguration::new(String::from("simple_cache")));
    let key = 4;
    simple_cache.put(key, "test", &tx);
    match simple_cache.get(key, &tx) {
    	Some(value) => println!("{} value = {}", key, value),
    	None => println!("Value for key {} not found", key),
    };

    match simple_cache.get(key, &tx_ext) {
    	Some(value) => println!("Tx2 get = {}", value),
    	None => println!("Tx2 get = None"),
    };
    

    cache_manager.transaction_manager.commit(&tx);

    match simple_cache.get(key, &tx_ext) {
    	Some(value) => println!("Tx2 get = {}", value),
    	None => println!("Tx2 get = None"),
    };
    
    cache_manager.transaction_manager.commit(&tx_ext);
}