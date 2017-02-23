
use std::boxed::Box;

use rustless::json::ToJson;

// Type trait, that allow define user type
pub struct TypeDescription {
	name: String,
	reader: Box<Fn(&rustless::json::JsonValue) -> Vec<u8>>,
	writer: Box<Fn(&Vec<u8>) -> rustless::json::JsonValue>,
}

// Universal description of some entity. For example: key or value
// For performance purposes each field is marked by number id
pub struct EntityDescription {
	count: AtomicUsize,
	fields: BTreeMap<String, Arc<Box<TypeDescription>>>,
	ids_map: BTreeMap<u16, String>,
	reverse_ids_map: BTreeMap<String, u16>,
}

// Description of table, that is key-value cache 
pub struct TableDescription {
	name: String,
	key: EntityDescription,
	value: EntityDescription,
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

// TypeDescription impl
unsafe impl Send for TypeDescription {}
unsafe impl Sync for TypeDescription {}

impl ToJson for TypeDescription {
	fn to_json(&self) -> rustless::json::JsonValue {
		rustless::json::to_value(self.name.clone())
	}
}

// EntityDescription impl
impl ToJson for EntityDescription {
	fn to_json(&self) -> rustless::json::JsonValue {
		rustless::json::JsonValue::Object(self.fields.iter().map(|(k, v)| {
			(k.clone(), v.to_json())
		}).collect())
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

// TableDescription impl
impl ToJson for TableDescription {
	fn to_json(&self) -> rustless::json::JsonValue {
		let mut res = BTreeMap::<String, rustless::json::JsonValue>::new();
		res.insert(String::from("name"), rustless::json::to_value(self.name.clone()));
		res.insert(String::from("key"), self.key.to_json());
		res.insert(String::from("value"), self.value.to_json());
		rustless::json::JsonValue::Object(res)
	}
}