
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::boxed::Box;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustless::json::ToJson;
use rustless::{self};

use data_base::IoEntityError;

// Type trait, that allow define user type
pub struct TypeDescription {
	pub name: String,
	pub reader: Box<Fn(&rustless::json::JsonValue) -> Result<Vec<u8>>, IoEntityError::Read>>,
	pub writer: Box<Fn(&Vec<u8>) -> Result<rustless::json::JsonValue, IoEntityError::Write>>,
}

// Universal description of some entity. For example: key or value
// For performance purposes each field is marked by number id
pub struct EntityDescription {
	count: AtomicUsize,
	pub fields: BTreeMap<String, Arc<Box<TypeDescription>>>,
	pub ids_map: BTreeMap<u16, String>,
	pub reverse_ids_map: BTreeMap<String, u16>,
}

// Description of table, that is key-value cache 
pub struct TableDescription {
	pub name: String,
	pub key: EntityDescription,
	pub value: EntityDescription,
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
    fn from_view(
            view: &EntityDescriptionView, 
		    type_descs: &BTreeMap<String, Arc<Box<TypeDescription>>>) -> Result<EntityDescription, String> {
        let mut entity_fields = view.fields.iter().map(|(k, v)| {
            (k.clone(), type_descs.get(v).map(|type_desc| { type_desc.clone() }))
        }).collect();
        let undefined_fields: Vec<String> = EntityDescription::get_undefined_fields(&entity_fields);
        if undefined_fields.iter().next().is_some() {
            Err(undefined_fields.iter().fold(String::new(), |base, field_name| { base + ", " + field_name.as_str() }))
        }
        else {
            let entity_fields = entity_fields.iter_mut().filter_map(move |(k, v)| { v.clone().map(|value| { (k.clone(), value )}) }).collect();
            Ok(EntityDescription::from_fields(entity_fields))
        }
    }

    fn get_undefined_fields(entity_fields: &BTreeMap<String, Option<Arc<Box<TypeDescription>>> >) -> Vec<String> {
        entity_fields.iter().filter_map(|(k, v)| { match *v {
            Some(_) => None,
            None => Some(k.clone()),
        }}).collect::<Vec<String>>()
    }

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

impl TableDescription {
    pub fn from_view(view: &TableDescriptionView, type_descs: &BTreeMap<String, Arc<Box<TypeDescription>>>) -> Result<TableDescription, String> {
        let key_desc = try!(EntityDescription::from_view(&view.key, type_descs));
        let value_desc = try!(EntityDescription::from_view(&view.value, type_descs));
        Ok(TableDescription { 
            name: view.name.clone(),
            key: key_desc,
            value: value_desc
        })
    }
}