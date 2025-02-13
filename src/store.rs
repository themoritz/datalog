use std::collections::{BTreeSet, HashSet};

use crate::{
    persist::Backend,
    query::{Entry, Pattern, Query, Where},
    transact::{IdMapping, Transact},
    Attribute, Datom, Entity, Result, Value,
};

#[derive(Clone, Debug, Copy, serde::Serialize)]
pub enum Type {
    Bool,
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

#[derive(Default, PartialEq, Debug, Clone)]
pub struct Builtins {
    ident: Entity,
    type_: Entity,
    card: Entity,
    doc: Entity,
    one: Entity,
    many: Entity,
    ref_: Entity,
    string: Entity,
    int: Entity,
    float: Entity,
    bool: Entity,
}

impl Builtins {
    fn type_entity(&self, type_: Type) -> Entity {
        match type_ {
            Type::Bool => self.bool,
            Type::Ref => self.ref_,
            Type::Int => self.int,
            Type::Float => self.float,
            Type::Str => self.string,
        }
    }

    fn card_entity(&self, card: Cardinality) -> Entity {
        match card {
            Cardinality::One => self.one,
            Cardinality::Many => self.many,
        }
    }

    fn build<S: Store>(s: &mut S) -> Self {
        let ident = Entity(0);
        let type_ = Entity(1);
        let card = Entity(2);
        let doc = Entity(3);

        let one = Entity(4);
        let many = Entity(5);

        s.insert_raw(one, ident, "db.cardinality/one");
        s.insert_raw(many, ident, "db.cardinality/many");

        let ref_ = s.next_entity_id();
        s.insert_raw(ref_, ident, "db.type/ref");
        let string = s.next_entity_id();
        s.insert_raw(string, ident, "db.type/string");
        let int = s.next_entity_id();
        s.insert_raw(int, ident, "db.type/int");
        let float = s.next_entity_id();
        s.insert_raw(float, ident, "db.type/float");
        let bool = s.next_entity_id();
        s.insert_raw(bool, ident, "db.type/bool");

        s.insert_raw(ident, ident, "db/ident");
        s.insert_raw(ident, type_, string);
        s.insert_raw(ident, card, one);
        s.insert_raw(ident, doc, "The identifier of an attribute");

        s.insert_raw(type_, ident, "db/type");
        s.insert_raw(type_, type_, ref_);
        s.insert_raw(type_, card, one);
        s.insert_raw(type_, doc, "Type of an attribute");

        s.insert_raw(card, ident, "db/cardinality");
        s.insert_raw(card, type_, ref_);
        s.insert_raw(card, card, one);
        s.insert_raw(card, doc, "Cardinality of an attribute");

        s.insert_raw(doc, ident, "db/doc");
        s.insert_raw(doc, type_, string);
        s.insert_raw(doc, card, one);
        s.insert_raw(doc, doc, "Documentation string for an attribute");

        Builtins {
            ident,
            type_,
            card,
            doc,
            one,
            many,
            ref_,
            string,
            int,
            float,
            bool,
        }
    }
}

pub trait Store {
    fn naked() -> Self;
    // fn new() -> Self;
    fn builtins(&self) -> &Builtins;
    fn set_builtins(&mut self, builtins: Builtins);
    fn set_next_id(&mut self, next_id: Entity);
    fn next_entity_id(&mut self) -> Entity;
    fn insert_raw(&mut self, e: Entity, a: Entity, v: impl Clone + Into<Value>);
    fn retract_raw(&mut self, e: Entity, a: Entity, v: Value);
    fn iter(&self) -> impl Iterator<Item = EAV> + '_;
    fn iter_entity(&self, e: Entity) -> impl Iterator<Item = EAV> + '_;
    fn iter_entity_attribute(&self, e: Entity, a: Entity) -> impl Iterator<Item = EAV> + '_;
    fn iter_attribute_value(&self, a: Entity, v: Value) -> impl Iterator<Item = EAV> + '_;

    /// PROVIDED:

    fn new() -> Self
    where
        Self: Sized,
    {
        let mut s = Self::naked();

        let builtins = Builtins::build(&mut s);
        s.set_builtins(builtins);

        s
    }

    fn add_attribute(
        &mut self,
        name: &str,
        type_: Type,
        cardinality: Cardinality,
        doc: &str,
    ) -> Result<()> {
        if let Ok(_) = self.get_attribute_id(&Attribute(name.to_string())) {
            return Err("Attribute already defined".to_string());
        }

        let e = self.next_entity_id();

        self.insert_raw(e, self.builtins().ident, name);
        self.insert_raw(e, self.builtins().type_, self.builtins().type_entity(type_));
        self.insert_raw(
            e,
            self.builtins().card,
            self.builtins().card_entity(cardinality),
        );
        self.insert_raw(e, self.builtins().doc, doc);

        Ok(())
    }

    fn insert(&mut self, datom: Datom) -> Result<()> {
        let a = self.get_attribute_id(&datom.a)?;
        self.check_attribute_type(a, &datom.v)?;
        self.insert_raw(datom.e, a, datom.v);
        Ok(())
    }

    fn retract(&mut self, datom: Datom) -> Result<()> {
        let a = self.get_attribute_id(&datom.a)?;

        self.retract_raw(datom.e, a, datom.v);

        Ok(())
    }

    fn get_entity_by_ident(&self, ident: &str) -> Option<Entity> {
        self.iter_attribute_value(self.builtins().ident, Value::Str(ident.to_string()))
            .next()
            .map(|eav| eav.e)
    }

    fn get_attribute_id(&self, a: &Attribute) -> Result<Entity> {
        match self.get_entity_by_ident(&a.0) {
            Some(e) => Ok(e),
            None => Err(format!("Could not find id for attribute `{}`", a.0)),
        }
    }

    fn get_attribute(&self, e: Entity) -> Result<Attribute> {
        match self.iter_entity_attribute(e, self.builtins().ident).next() {
            None => Err(format!("Could not get attribute name for entity {}", e.0)),
            Some(eav) => match eav.v {
                Value::Str(a) => Ok(Attribute(a)),
                _ => Err(format!(
                    "Attribute name value should be string, found {}. DB corrupt?",
                    eav.v
                )),
            },
        }
    }

    fn get_attribute_type(&self, e: Entity) -> Result<Type> {
        match self.iter_entity_attribute(e, self.builtins().type_).next() {
            None => {
                let a = self.get_attribute(e)?;
                Err(format!("Could not get type for attribute {a}. DB corrupt?"))
            }
            Some(eav) => match eav.v {
                Value::Ref(i) => {
                    if i == self.builtins().float.0 {
                        Ok(Type::Float)
                    } else if i == self.builtins().int.0 {
                        Ok(Type::Int)
                    } else if i == self.builtins().string.0 {
                        Ok(Type::Str)
                    } else if i == self.builtins().ref_.0 {
                        Ok(Type::Ref)
                    } else if i == self.builtins().bool.0 {
                        Ok(Type::Bool)
                    } else {
                        Err(format!(
                            "Type calue doesn't match type builtin. DB corrupt?"
                        ))
                    }
                }
                _ => Err(format!(
                    "Type value expected as ref, found {}. DB corrupt?",
                    eav.v
                )),
            },
        }
    }

    #[must_use]
    fn check_attribute_type(&self, a: Entity, v: &Value) -> Result<()> {
        let expected = self.get_attribute_type(a)?;
        match (expected, v) {
            (Type::Bool, Value::Bool(_)) => Ok(()),
            (Type::Float, Value::Float(_)) => Ok(()),
            (Type::Int, Value::Int(_)) => Ok(()),
            (Type::Ref, Value::Ref(_)) => Ok(()),
            (Type::Str, Value::Str(_)) => Ok(()),
            _ => {
                let a = self.get_attribute(a)?;
                Err(format!(
                    "Invalid type for attribute `{a}`: Expected {expected:?}, got {v:?}"
                ))
            }
        }
    }

    fn get_attribute_cardinality(&self, e: Entity) -> Result<Cardinality> {
        match self.iter_entity_attribute(e, self.builtins().card).next() {
            None => {
                let a = self.get_attribute(e)?;
                Err(format!(
                    "Could not determine cardinality of attribute `{a}`. DB corrupt?"
                ))
            }
            Some(eav) => match eav.v {
                Value::Ref(i) => {
                    if i == self.builtins().one.0 {
                        Ok(Cardinality::One)
                    } else if i == self.builtins().many.0 {
                        Ok(Cardinality::Many)
                    } else {
                        Err(format!(
                            "Cardinality value doesn't match cardinality builtin. DB corrupt?"
                        ))
                    }
                }
                _ => Err(format!(
                    "Cardinality value expected as ref, found {}. DB corrupt?",
                    eav.v
                )),
            },
        }
    }

    fn resolve_pattern(&self, p: &Pattern<Attribute>) -> Result<Pattern<Entity>> {
        let a = match &p.a {
            Entry::Var(v) => Ok(Entry::Var(v.clone())),
            Entry::Lit(a) => self.get_attribute_id(&a).map(Entry::Lit),
        }?;

        Ok(Pattern {
            e: p.e.clone(),
            a,
            v: p.v.clone(),
        })
    }

    fn resolve_where(&self, whr: &Where<Attribute>) -> Result<Where<Entity>> {
        match whr {
            Where::Pattern(p) => Ok(Where::Pattern(self.resolve_pattern(&p)?)),
            Where::And(l, r) => Ok(Where::and(self.resolve_where(l)?, self.resolve_where(r)?)),
            Where::Or(l, r) => Ok(Where::or(self.resolve_where(l)?, self.resolve_where(r)?)),
        }
    }

    fn resolve_result(&self, table: HashSet<Vec<Value>>) -> HashSet<Vec<Value>> {
        table
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|val| {
                        if let Value::Ref(i) = val {
                            if let Ok(Attribute(a)) = self.get_attribute(Entity(i)) {
                                Value::Str(a.clone())
                            } else {
                                val
                            }
                        } else {
                            val
                        }
                    })
                    .collect()
            })
            .collect()
    }

    fn save<B: Backend>(&self, mut backend: B) -> Result<B> {
        backend.save(self.iter())
    }

    fn load<B: Backend>(mut backend: B) -> Result<Self>
    where
        Self: Sized,
    {
        let mut s = Self::naked();

        let mut largest_id = 0;
        let mut ident = Entity(0);
        for eav in backend.load()? {
            largest_id = largest_id.max(eav.e.0);
            if eav.e == eav.a && eav.v == Value::Str("db/ident".to_string()) {
                ident = eav.e;
            }
            s.insert_raw(eav.e, eav.a, eav.v);
        }

        // Need this to recover builtin entities
        s.set_builtins(Builtins {
            ident,
            ..Default::default()
        });

        s.set_next_id(Entity(largest_id + 1));

        macro_rules! recover_builtin {
            ($ident:literal) => {
                match s.get_entity_by_ident($ident) {
                    Some(e) => e,
                    None => {
                        return Err(format!(
                            "Could not find built-in entity `{}`. DB corrupt?",
                            $ident
                        ))
                    }
                }
            };
        }

        s.set_builtins(Builtins {
            ident,
            type_: recover_builtin!("db/type"),
            card: recover_builtin!("db/cardinality"),
            doc: recover_builtin!("db/doc"),
            one: recover_builtin!("db.cardinality/one"),
            many: recover_builtin!("db.cardinality/many"),
            ref_: recover_builtin!("db.type/ref"),
            string: recover_builtin!("db.type/string"),
            int: recover_builtin!("db.type/int"),
            float: recover_builtin!("db.type/float"),
            bool: recover_builtin!("db.type/bool"),
        });

        Ok(s)
    }

    fn transact(&mut self, tx: Transact) -> Result<IdMapping>
    where
        Self: Sized,
    {
        let (updates, mapping) = tx.compile(self)?;
        for u in updates {
            if u.add {
                self.insert_raw(u.e, u.a, u.v);
            } else {
                self.retract_raw(u.e, u.a, u.v);
            }
        }
        Ok(mapping)
    }

    /// Returns tempref with id `x`. Can be created with `add` macro.
    fn new_entity(&mut self, tx: Transact) -> Result<Entity>
    where
        Self: Sized,
    {
        let mapping = self.transact(tx)?;
        Ok(mapping[&"x".to_string()])
    }

    fn query(&self, q: Query) -> Result<HashSet<Vec<Value>>>
    where
        Self: Sized,
    {
        q.qeval(self)
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct MemStore {
    eav: BTreeSet<EAV>,
    ave: BTreeSet<AVE>,
    builtins: Builtins,
    next_id: u64,
    next_tx: u64,
}

impl Store for MemStore {
    fn naked() -> Self {
        MemStore {
            eav: BTreeSet::new(),
            ave: BTreeSet::new(),
            builtins: Builtins::default(),
            next_id: 0,
            next_tx: 1,
        }
    }

    fn builtins(&self) -> &Builtins {
        &self.builtins
    }

    fn set_builtins(&mut self, builtins: Builtins) {
        self.builtins = builtins;
    }

    fn set_next_id(&mut self, next_id: Entity) {
        self.next_id = next_id.0;
    }

    fn next_entity_id(&mut self) -> Entity {
        let result = Entity(self.next_id);
        self.next_id += 1;
        result
    }

    fn insert_raw(&mut self, e: Entity, a: Entity, v: impl Clone + Into<Value>) {
        self.eav.insert(EAV {
            e,
            a,
            v: v.clone().into(),
        });
        self.ave.insert(AVE { a, v: v.into(), e });
        self.next_id = self.next_id.max(e.0 + 1);
    }

    fn retract_raw(&mut self, e: Entity, a: Entity, v: Value) {
        self.eav.remove(&EAV { e, a, v: v.clone() });
        self.ave.remove(&AVE { a, v, e });
    }

    fn iter(&self) -> impl Iterator<Item = EAV> + '_ {
        self.eav.clone().into_iter()
    }

    fn iter_entity(&self, e: Entity) -> impl Iterator<Item = EAV> + '_ {
        let min = EAV {
            e,
            a: Entity::min(),
            v: Value::min(),
        };
        let max = EAV {
            e: e.next(),
            a: Entity::min(),
            v: Value::min(),
        };
        self.eav.range(min..max).map(|eav| eav.clone().into())
    }

    fn iter_entity_attribute(&self, e: Entity, a: Entity) -> impl Iterator<Item = EAV> + '_ {
        let min = EAV {
            e,
            a: a.clone(),
            v: Value::min(),
        };
        let max = EAV {
            e,
            a: a.next(),
            v: Value::min(),
        };
        self.eav.range(min..max).map(|eav| eav.clone().into())
    }

    fn iter_attribute_value(&self, a: Entity, v: Value) -> impl Iterator<Item = EAV> + '_ {
        let min = AVE {
            a: a.clone(),
            v: v.clone(),
            e: Entity::min(),
        };
        let max = AVE {
            a: a.clone(),
            v: v.next(),
            e: Entity::min(),
        };
        self.ave.range(min..max).map(|ave| ave.clone().into())
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct EAV {
    pub e: Entity,
    pub a: Entity,
    pub v: Value,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
struct AVE {
    a: Entity,
    v: Value,
    e: Entity,
}

impl From<AVE> for EAV {
    fn from(value: AVE) -> Self {
        EAV {
            e: value.e,
            a: value.a,
            v: value.v,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{datom, store::Store};

    use super::{Cardinality, MemStore, Type};

    #[test]
    fn insert_type_check() {
        let mut s = MemStore::new();
        s.add_attribute("foo", Type::Int, Cardinality::One, "")
            .unwrap();

        s.insert(datom!(100, "foo" = 4)).unwrap();

        assert!(s.insert(datom!(200, "foo" = "Bar")).is_err());
    }
}
