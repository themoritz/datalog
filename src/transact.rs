use std::collections::HashMap;

use crate::{
    store::{Cardinality, Store},
    Attribute, Result,
};

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
            Transact::AddValue { a, v } => {
                let e = e.to_entity()?;
                let a = store.get_attribute_id(&a)?;
                let v = v.to_value()?;
                store.check_attribute_type(a, &v)?;

                let mut result = vec![];
                if let Cardinality::One = store.get_attribute_cardinality(a)? {
                    result.extend(store.iter_entity_attribute(e, a).map(|eav| Update {
                        add: false,
                        e: eav.e,
                        a: eav.a,
                        v: eav.v,
                    }))
                }

                result.push(Update { add: true, e, a, v });

                Ok(result)
            }
            Transact::AddComponent { a, sub } => {
                let e = e.to_entity()?;
                let a = store.get_attribute_id(&a)?;
                // Don't want to create id here yet, just checking type:
                store.check_attribute_type(a, &crate::Value::Ref(0))?;

                let mut result = vec![];

                if let Cardinality::One = store.get_attribute_cardinality(a)? {
                    for eav in store.iter_entity_attribute(e, a).collect::<Vec<_>>() {
                        let comp_e = match &eav.v {
                            crate::Value::Ref(e) => Ok(crate::Entity(*e)),
                            _ => Err(format!("Expected Ref value; found {}", eav.v)),
                        }?;
                        result.append(
                            &mut Transact::Retract.compile_rec(store, Entity::Entity(comp_e))?,
                        );
                        result.push(Update {
                            add: false,
                            e: eav.e,
                            a: eav.a,
                            v: eav.v,
                        });
                    }
                }

                let new_e = store.next_entity_id();
                result.push(Update {
                    add: true,
                    e,
                    a,
                    v: crate::Value::Ref(new_e.0),
                });
                result.append(&mut sub.compile_rec(store, Entity::Entity(new_e))?);
                Ok(result)
            }
            Transact::RetractValue { a, v } => {
                let a = store.get_attribute_id(&a)?;
                Ok(vec![Update {
                    add: false,
                    e: e.to_entity()?,
                    a,
                    v: v.to_value()?,
                }])
            }
            Transact::RetractAttribute { a } => {
                let a = store.get_attribute_id(&a)?;
                Ok(store
                    .iter_entity_attribute(e.to_entity()?, a)
                    .map(|eav| Update {
                        add: false,
                        e: eav.e,
                        a: eav.a,
                        v: eav.v,
                    })
                    .collect())
            }
            Transact::Retract => {
                // TODO: Retract components
                // TODO: Retract links to entity (need vae index for this)
                Ok(store
                    .iter_entity(e.to_entity()?)
                    .map(|eav| Update {
                        add: false,
                        e: eav.e,
                        a: eav.a,
                        v: eav.v,
                    })
                    .collect())
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
    a: crate::Entity,
    v: crate::Value,
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;

    use crate::{movies::DATA, Attribute};

    use super::{Entity, Transact, Update, Value};

    #[test]
    fn compile_add_component() {
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
                        a: Attribute("name".to_string()),
                        v: Value::Value(crate::Value::Str("Piet".to_string())),
                    }),
                },
            ])),
        };

        let mut store = DATA.clone();

        assert_eq!(
            tx.compile(&mut store).unwrap(),
            vec![
                Update {
                    add: true,
                    e: crate::Entity(201),
                    a: crate::Entity(12),
                    v: crate::Value::Int(40),
                },
                Update {
                    add: true,
                    e: crate::Entity(201),
                    a: crate::Entity(13),
                    v: crate::Value::Ref(202),
                },
                Update {
                    add: true,
                    e: crate::Entity(202),
                    a: crate::Entity(11),
                    v: crate::Value::Str("Piet".to_string()),
                },
            ]
        );
    }

    #[test]
    fn compile_retract() {
        let tx = Transact::WithEntity {
            e: Entity::Entity(crate::Entity(100)),
            sub: Box::new(Transact::Retract),
        };

        let mut store = DATA.clone();

        assert_eq!(
            tx.compile(&mut store).unwrap(),
            vec![
                Update {
                    add: false,
                    e: crate::Entity(100),
                    a: crate::Entity(11),
                    v: crate::Value::Str("Moritz".to_string()),
                },
                Update {
                    add: false,
                    e: crate::Entity(100),
                    a: crate::Entity(12),
                    v: crate::Value::Int(39),
                },
            ]
        );
    }

    #[test]
    fn compile_upsert() {
        let tx = Transact::WithEntity {
            e: Entity::Entity(crate::Entity(100)),
            sub: Box::new(Transact::AddValue {
                a: Attribute("age".to_string()),
                v: Value::Value(crate::Value::Int(40)),
            }),
        };

        let mut store = DATA.clone();

        assert_eq!(
            tx.compile(&mut store).unwrap(),
            vec![
                Update {
                    add: false,
                    e: crate::Entity(100),
                    a: crate::Entity(12),
                    v: crate::Value::Int(39),
                },
                Update {
                    add: true,
                    e: crate::Entity(100),
                    a: crate::Entity(12),
                    v: crate::Value::Int(40),
                },
            ]
        );
    }
}
