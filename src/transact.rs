use std::collections::HashMap;

use crate::{store::Store, Attribute, Result};

#[derive(Debug, Clone)]
pub enum Entity {
    Entity(crate::Entity),
    TempRef(String),
}

impl Entity {
    pub fn to_entity(self) -> Result<crate::Entity> {
        match self {
            Entity::Entity(e) => Ok(e),
            Entity::TempRef(tempref) => Err(format!(
                "Internal error: Unresolved Entity::Tempref: `{tempref}`"
            )),
        }
    }
}

#[derive(Debug)]
pub enum Value {
    Value(crate::Value),
    TempRef(String),
}

impl Value {
    pub fn to_value(self) -> Result<crate::Value> {
        match self {
            Value::Value(v) => Ok(v),
            Value::TempRef(tempref) => Err(format!(
                "Internal error: Unresolved Value::Tempref: `{tempref}`"
            )),
        }
    }
}

#[derive(Debug)]
pub enum Transact {
    WithEntity { e: Entity, sub: Box<Transact> },
    AddValue { a: Attribute, v: Value },
    AddComponent { a: Attribute, sub: Box<Transact> },
    RetractValue { a: Attribute, v: Value },
    RetractAttribute { a: Attribute },
    Retract,
    List(Vec<Transact>),
}

impl Transact {
    pub fn compile(self, store: &mut Store) -> Result<Vec<Update>> {
        let without_refs = self.resolve_temprefs(store);

        match without_refs {
            Transact::WithEntity { e, sub } => sub.compile_rec(store, e),
            Transact::List(subs) => {
                let nested = subs
                    .into_iter()
                    .map(|sub| sub.compile(store))
                    .collect::<Result<Vec<_>>>()?;
                Ok(nested.into_iter().flatten().collect())
            }
            _ => Err(format!(
                "Expected `WithEntity` or `List`, got {without_refs:?}"
            )),
        }
    }

    fn compile_rec(self, store: &mut Store, e: Entity) -> Result<Vec<Update>> {
        match self {
            Transact::WithEntity { e, sub } => sub.compile_rec(store, e),
            Transact::AddValue { a, v } => Ok(vec![Update {
                add: true,
                e: e.to_entity()?,
                a,
                v: v.to_value()?,
            }]),
            Transact::AddComponent { a, sub } => {
                let new_e = store.next_entity_id();
                let mut result = vec![Update {
                    add: true,
                    e: e.to_entity()?,
                    a,
                    v: crate::Value::Ref(new_e.0),
                }];
                result.append(&mut sub.compile_rec(store, Entity::Entity(new_e))?);
                Ok(result)
            }
            Transact::RetractValue { a, v } => Ok(vec![Update {
                add: false,
                e: e.to_entity()?,
                a,
                v: v.to_value()?,
            }]),
            Transact::RetractAttribute { a } => {
                todo!()
            }
            Transact::Retract => {
                todo!()
            }
            Transact::List(subs) => {
                let nested = subs
                    .into_iter()
                    .map(|sub| sub.compile_rec(store, e.clone()))
                    .collect::<Result<Vec<_>>>()?;
                Ok(nested.into_iter().flatten().collect())
            }
        }
    }

    fn resolve_temprefs(self, store: &mut Store) -> Self {
        let mut mapping: HashMap<String, crate::Entity> = HashMap::new();
        self.resolve_temprefs_rec(store, &mut mapping)
    }

    fn resolve_temprefs_rec(
        self,
        store: &mut Store,
        mapping: &mut HashMap<String, crate::Entity>,
    ) -> Self {
        let mut get_id = |tempref| {
            mapping
                .entry(tempref)
                .or_insert_with(|| store.next_entity_id())
                .clone()
        };

        match self {
            Transact::WithEntity { e, sub } => Transact::WithEntity {
                e: match e {
                    Entity::TempRef(tempref) => Entity::Entity(get_id(tempref)),
                    Entity::Entity(e) => Entity::Entity(e),
                },
                sub: Box::new(sub.resolve_temprefs_rec(store, mapping)),
            },
            Transact::AddValue { a, v } => Transact::AddValue {
                a,
                v: match v {
                    Value::TempRef(tempref) => Value::Value(crate::Value::Ref(get_id(tempref).0)),
                    _ => v,
                },
            },
            Transact::AddComponent { a, sub } => Transact::AddComponent {
                a,
                sub: Box::new(sub.resolve_temprefs_rec(store, mapping)),
            },
            Transact::RetractValue { a, v } => Transact::RetractValue { a, v },
            Transact::RetractAttribute { a } => Transact::RetractAttribute { a },
            Transact::Retract => Transact::Retract,
            Transact::List(subs) => Transact::List(
                subs.into_iter()
                    .map(|sub| sub.resolve_temprefs_rec(store, mapping))
                    .collect(),
            ),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Update {
    add: bool,
    e: crate::Entity,
    a: Attribute,
    v: crate::Value,
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;

    use crate::{store::Store, Attribute};

    use super::{Entity, Transact, Update, Value};

    #[test]
    fn compile() {
        let tx = Transact::WithEntity {
            e: Entity::TempRef("a".to_string()),
            sub: Box::new(Transact::List(vec![
                Transact::AddValue {
                    a: Attribute("age".to_string()),
                    v: Value::Value(crate::Value::Int(40)),
                },
                Transact::AddComponent {
                    a: Attribute("friend".to_string()),
                    sub: Box::new(Transact::AddValue {
                        a: Attribute("boss".to_string()),
                        v: Value::TempRef("a".to_string()),
                    }),
                },
            ])),
        };

        let mut store = Store::new();

        assert_eq!(
            tx.compile(&mut store).unwrap(),
            vec![
                Update {
                    add: true,
                    e: crate::Entity(11,),
                    a: Attribute("age".to_string(),),
                    v: crate::Value::Int(40,),
                },
                Update {
                    add: true,
                    e: crate::Entity(11,),
                    a: Attribute("friend".to_string(),),
                    v: crate::Value::Ref(12,),
                },
                Update {
                    add: true,
                    e: crate::Entity(12,),
                    a: Attribute("boss".to_string(),),
                    v: crate::Value::Ref(11,),
                },
            ]
        );
    }
}
