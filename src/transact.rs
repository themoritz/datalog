use std::collections::HashMap;

use crate::{
    store::{Cardinality, Store},
    Attribute, Result,
};

pub struct Tmp<'a>(pub &'a str);

pub type IdMapping = HashMap<String, crate::Entity>;

#[derive(Debug, Clone, PartialEq)]
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

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub enum Transact {
    Add { e: Entity, add: Add },
    RetractValue { e: Entity, a: Attribute, v: Value },
    RetractAttribute { e: Entity, a: Attribute },
    Retract { e: Entity },
    List(Vec<Transact>),
}

#[derive(Debug, PartialEq)]
pub enum Add {
    Value { a: Attribute, v: Value },
    Component { a: Attribute, sub: Box<Add> },
    List(Vec<Add>),
}

impl Transact {
    pub fn and(self, other: Self) -> Self {
        Transact::List(vec![self, other])
    }

    pub fn compile(self, store: &mut Store) -> Result<(Vec<Update>, IdMapping)> {
        let (resolved, mapping) = self.resolve_temprefs(store);
        Ok((resolved.compile_rec(store)?, mapping))
    }

    fn compile_rec(self, store: &mut Store) -> Result<Vec<Update>> {
        match self {
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
                    .map(|sub| sub.compile_rec(store))
                    .collect::<Result<Vec<_>>>()?;
                Ok(nested.into_iter().flatten().collect())
            }
        }
    }

    fn resolve_temprefs(self, store: &mut Store) -> (Self, IdMapping) {
        let mut mapping: IdMapping = HashMap::new();
        (self.resolve_temprefs_rec(store, &mut mapping), mapping)
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
                            .compile_rec(store)?,
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
    pub add: bool,
    pub e: crate::Entity,
    pub a: crate::Entity,
    pub v: crate::Value,
}

#[macro_export]
macro_rules! retract {
    ($e:expr) => {
        $crate::transact::Transact::Retract {
            e: $crate::transact::Entity::from($e),
        }
    };

    ($e:expr, $a:expr) => {
        $crate::transact::Transact::RetractAttribute {
            e: $crate::transact::Entity::from($e),
            a: $crate::Attribute::from($a),
        }
    };

    ($e:expr, $a:tt: $v:expr) => {
        $crate::transact::Transact::RetractValue {
            e: $crate::transact::Entity::from($e),
            a: $crate::Attribute::from($a),
            v: $crate::transact::Value::from($v),
        }
    };
}

#[macro_export]
macro_rules! add {
    // Entry with default `x` tempref
    ({$($tt:tt)*}) => {
        $crate::transact::Transact::Add { e: $crate::transact::Entity::TempRef("x".to_string()), add: add!(@multi [] $($tt)*) }
    };

    // Entry value
    ($e:expr, $a:tt: $v:expr) => {
        $crate::transact::Transact::Add { e: $crate::transact::Entity::from($e), add: add!(@value ($a) ($v)) }
    };

    // Entry component
    ($e:expr, {$($tt:tt)*}) => {
        $crate::transact::Transact::Add { e: $crate::transact::Entity::from($e), add: add!(@multi [] $($tt)*) }
    };

    // VALUE

    (@value ($a:tt) ($v:expr)) => {
        $crate::transact::Add::Value { a: $crate::Attribute::from($a), v: $crate::transact::Value::from($v) }
    };

    // COMPONENT

    (@component ($a:tt) ($sub:expr)) => {
        $crate::transact::Add::Component { a: $crate::Attribute::from($a), sub: Box::new($sub) }
    };

    // MULTIPLE ATTRIBUTES

    // Done
    (@multi [$($elems:expr),*]) => {
        $crate::transact::Add::List(vec![$($elems,)*])
    };

    // Component
    (@multi [$($elems:expr),*] $a:tt: {$($tt:tt)*} $(, $($rest:tt)*)?) => {
        add!(@multi [$($elems,)* add!(@component ($a) (add!(@multi [] $($tt)*)))] $($($rest)*)?)
    };

    // List
    (@multi [$($elems:expr),*] $a:tt: [$($tt:tt)*] $(, $($rest:tt)*)?) => {
        add!(@multi [$($elems,)* add!(@list ($a) [] [] $($tt)*)] $($($rest)*)?)
    };

    // Value
    (@multi [$($elems:expr),*] $a:tt: $v:expr $(, $($rest:tt)*)?) => {
        add!(@multi [$($elems,)* add!(@value ($a) ($v))] $($($rest)*)?)
    };

    // LIST

    // Done
    (@list ($a:tt) [$($values:expr),*] [$($components:expr),*]) => {{
        let mut result: Vec<$crate::transact::Add> = vec![];
        for v in vec![$($values,)*].into_iter() {
            result.push($crate::transact::Add::Value { a: $crate::Attribute::from($a), v });
        }
        for c in vec![$($components,)*].into_iter() {
            result.push($crate::transact::Add::Component { a: $crate::Attribute::from($a), sub: Box::new(c) });
        }
        $crate::transact::Add::List(result)
    }};

    // Component
    (@list ($a:tt) [$($values:expr),*] [$($components:expr),*] {$($tt:tt)*} $(, $($rest:tt)*)?) => {
        add!(@list ($a) [$($values,)*] [$($components),* add!(@multi [] $($tt)*)] $($($rest)*)?)
    };

    // Value
    (@list ($a:tt) [$($values:expr),*] [$($components:expr),*] $v:expr $(, $($rest:tt)*)?) => {
        add!(@list ($a) [$($values,)* $crate::transact::Value::from($v)] [$($components),*] $($($rest)*)?)
    };
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;

    use crate::{movies::DATA, Attribute};

    use super::{Add, Entity, Tmp, Transact, Update, Value};

    #[test]
    fn macro_add() {
        let tx = add!(Tmp("a"), {
           "name": "Moritz",
           "age": 39
        });

        assert_eq!(
            tx,
            Transact::Add {
                e: Entity::TempRef("a".to_string()),
                add: Add::List(vec![
                    Add::Value {
                        a: Attribute("name".to_string(),),
                        v: Value::Value(crate::Value::Str("Moritz".to_string(),),),
                    },
                    Add::Value {
                        a: Attribute("age".to_string(),),
                        v: Value::Value(crate::Value::Int(39,),),
                    },
                ],),
            }
        );
    }

    #[test]
    fn macro_add_component() {
        let tx = add!(1, {
            "friend": [{
                "age": 39
            }]
        });

        assert_eq!(
            tx,
            Transact::Add {
                e: Entity::Entity(crate::Entity(1,)),
                add: Add::List(vec![Add::List(vec![Add::Component {
                    a: crate::Attribute("friend".to_string()),
                    sub: Box::new(Add::List(vec![Add::Value {
                        a: crate::Attribute("age".to_string()),
                        v: Value::Value(crate::Value::Int(39)),
                    }]))
                }])])
            }
        );
    }

    #[test]
    fn macro_add_x() {
        let tx = add!({
            "name": "Moritz"
        });

        let mut store = DATA.clone();

        let (_, mapping) = tx.compile(&mut store).unwrap();
        assert_eq!(mapping["x"], crate::Entity(201));
    }

    #[test]
    fn macro_retract() {
        let tx = retract!(Tmp("a"));
        assert_eq!(
            tx,
            Transact::Retract {
                e: Entity::TempRef("a".to_string())
            }
        );

        let tx = retract!(14, "name");
        assert_eq!(
            tx,
            Transact::RetractAttribute {
                e: Entity::Entity(crate::Entity(14)),
                a: Attribute("name".to_string())
            }
        );
    }

    #[test]
    fn compile_add_component() {
        let tx = add!(Tmp("a"), {
            "age": 40,
            "friend": {
                "name": "Piet"
            }
        });

        let mut store = DATA.clone();

        assert_eq!(
            tx.compile(&mut store).unwrap().0,
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
        let tx = retract!(100);

        let mut store = DATA.clone();

        assert_eq!(
            tx.compile(&mut store).unwrap().0,
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
        let tx = add!(100, "age": 40);

        let mut store = DATA.clone();

        assert_eq!(
            tx.compile(&mut store).unwrap().0,
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
