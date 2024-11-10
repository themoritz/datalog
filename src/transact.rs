use std::collections::HashMap;

use crate::{
    store::{Cardinality, Store},
    Attribute, Result,
};

struct Tmp<'a>(&'a str);

#[derive(Debug, Clone)]
pub enum Entity {
    Entity(crate::Entity),
    TempRef(String),
}

impl Entity {
    fn resolve_tempref(
        self,
        mapping: &mut HashMap<String, crate::Entity>,
        store: &mut Store,
    ) -> Self {
        match self {
            Entity::Entity(e) => Entity::Entity(e),
            Entity::TempRef(tempref) => Entity::Entity(
                mapping
                    .entry(tempref)
                    .or_insert_with(|| store.next_entity_id())
                    .clone(),
            ),
        }
    }

    pub fn to_entity(self) -> Result<crate::Entity> {
        match self {
            Entity::Entity(e) => Ok(e),
            Entity::TempRef(tempref) => Err(format!(
                "Internal error: Unresolved Entity::Tempref: `{tempref}`"
            )),
        }
    }
}

impl From<u64> for Entity {
    fn from(value: u64) -> Self {
        Entity::Entity(crate::Entity(value))
    }
}

impl<'a> From<Tmp<'a>> for Entity {
    fn from(value: Tmp) -> Self {
        Entity::TempRef(value.0.to_string())
    }
}

#[derive(Debug)]
pub enum Value {
    Value(crate::Value),
    TempRef(String),
}

impl<T> From<T> for Value
where
    crate::Value: From<T>,
{
    fn from(value: T) -> Self {
        Value::Value(crate::Value::from(value))
    }
}

impl<'a> From<Tmp<'a>> for Value {
    fn from(value: Tmp) -> Self {
        Value::TempRef(value.0.to_string())
    }
}

impl Value {
    fn resolve_tempref(
        self,
        mapping: &mut HashMap<String, crate::Entity>,
        store: &mut Store,
    ) -> Self {
        match self {
            Value::Value(v) => Value::Value(v),
            Value::TempRef(tempref) => mapping
                .entry(tempref)
                .or_insert_with(|| store.next_entity_id())
                .clone()
                .into(),
        }
    }

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
    Add { e: Entity, add: Add },
    RetractValue { e: Entity, a: Attribute, v: Value },
    RetractAttribute { e: Entity, a: Attribute },
    Retract { e: Entity },
    List(Vec<Transact>),
}

#[derive(Debug)]
pub enum Add {
    Value { a: Attribute, v: Value },
    Component { a: Attribute, sub: Box<Add> },
    List(Vec<Add>),
}

impl Transact {
    pub fn and(self, other: Self) -> Self {
        Transact::List(vec![self, other])
    }

    fn compile(self, store: &mut Store) -> Result<Vec<Update>> {
        let resolved = self.resolve_temprefs(store);

        match resolved {
            Transact::Add { e, add } => add.compile(store, e),
            Transact::RetractValue { e, a, v } => {
                let a = store.get_attribute_id(&a)?;
                Ok(vec![Update {
                    add: false,
                    e: e.to_entity()?,
                    a,
                    v: v.to_value()?,
                }])
            }
            Transact::RetractAttribute { e, a } => {
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
            Transact::Retract { e } => {
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
                    .map(|sub| sub.compile(store))
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
        match self {
            Transact::Add { e, add } => Transact::Add {
                e: e.resolve_tempref(mapping, store),
                add: add.resolve_temprefs(store, mapping),
            },
            Transact::RetractValue { e, a, v } => Transact::RetractValue {
                e: e.resolve_tempref(mapping, store),
                a,
                v: v.resolve_tempref(mapping, store),
            },
            Transact::RetractAttribute { e, a } => Transact::RetractAttribute {
                e: e.resolve_tempref(mapping, store),
                a,
            },
            Transact::Retract { e } => Transact::Retract {
                e: e.resolve_tempref(mapping, store),
            },
            Transact::List(subs) => Transact::List(
                subs.into_iter()
                    .map(|sub| sub.resolve_temprefs_rec(store, mapping))
                    .collect(),
            ),
        }
    }
}

impl Add {
    fn compile(self, store: &mut Store, e: Entity) -> Result<Vec<Update>> {
        match self {
            Add::Value { a, v } => {
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
            Add::Component { a, sub } => {
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
                            &mut Transact::Retract {
                                e: Entity::Entity(comp_e),
                            }
                            .compile(store)?,
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
                result.append(&mut sub.compile(store, Entity::Entity(new_e))?);
                Ok(result)
            }
            Add::List(subs) => {
                let nested = subs
                    .into_iter()
                    .map(|sub| sub.compile(store, e.clone()))
                    .collect::<Result<Vec<_>>>()?;
                Ok(nested.into_iter().flatten().collect())
            }
        }
    }

    fn resolve_temprefs(
        self,
        store: &mut Store,
        mapping: &mut HashMap<String, crate::Entity>,
    ) -> Self {
        match self {
            Add::Value { a, v } => Add::Value {
                a,
                v: v.resolve_tempref(mapping, store),
            },
            Add::Component { a, sub } => Add::Component {
                a,
                sub: Box::new(sub.resolve_temprefs(store, mapping)),
            },
            Add::List(subs) => Add::List(
                subs.into_iter()
                    .map(|sub| sub.resolve_temprefs(store, mapping))
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

    use crate::{movies::DATA, transact::Add};

    use super::{Tmp, Transact, Update};

    #[test]
    fn compile_add_component() {
        let tx = Transact::List(vec![Transact::Add {
            e: Tmp("a").into(),
            add: Add::List(vec![
                Add::Value {
                    a: "age".into(),
                    v: 40.into(),
                },
                Add::Component {
                    a: "friend".into(),
                    sub: Box::new(Add::Value {
                        a: "name".into(),
                        v: "Piet".into(),
                    }),
                },
            ]),
        }]);

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
        let tx = Transact::Retract { e: 100.into() };

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
        let tx = Transact::Add {
            e: 100.into(),
            add: Add::Value {
                a: "age".into(),
                v: 40.into(),
            },
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
