use std::collections::HashMap;

use crate::{Attribute, Entity, Value};

#[derive(Clone, Copy, serde::Serialize)]
pub enum Type {
    Int,
    Ref,
    Float,
    Str,
}

#[derive(serde::Serialize)]
pub enum Cardinality {
    One,
    Many,
}

pub struct AttributeDetails {
    pub type_: Type,
    pub cardinality: Cardinality,
    pub id: Entity,
}

pub struct Schema {
    pub details: HashMap<Attribute, AttributeDetails>,
    pub attributes: HashMap<Entity, Attribute>,
}

impl Schema {
    pub fn new() -> Self {
        Schema {
            details: HashMap::new(),
            attributes: HashMap::new(),
        }
    }

    pub fn get_id(&self, a: &Attribute) -> Option<Entity> {
        self.details.get(a).map(|d| d.id)
    }

    pub fn valid_type(&self, a: &Attribute, v: &Value) -> bool {
        if let Some(details) = self.details.get(a) {
            match (details.type_, v) {
                (Type::Int, Value::Int(_)) => true,
                (Type::Ref, Value::Int(_)) => true,
                (Type::Float, Value::Float(_)) => true,
                (Type::Str, Value::Str(_)) => true,
                _ => false,
            }
        } else {
            false
        }
    }
}
