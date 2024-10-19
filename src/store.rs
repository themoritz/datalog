use std::collections::{BTreeSet, HashSet};

use crate::{
    persist::Backend,
    query::{Entry, Pattern, Where},
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

#[derive(Default, PartialEq, Debug)]
struct Builtins {
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
}

#[derive(PartialEq, Debug)]
pub struct Store {
    eav: BTreeSet<EAV>,
    ave: BTreeSet<AVE>,
    builtins: Builtins,
    next_id: u64,
    next_tx: u64,
}

impl Store {
    pub fn new() -> Self {
        let mut s = Store {
            eav: BTreeSet::new(),
            ave: BTreeSet::new(),
            builtins: Builtins::default(),
            next_id: 0,
            next_tx: 1,
        };

        let ident = s.next_entity_id();
        let type_ = s.next_entity_id();
        let card = s.next_entity_id();
        let doc = s.next_entity_id();

        let one = s.next_entity_id();
        let many = s.next_entity_id();

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

        s.builtins = Builtins {
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
        };

        s
    }

    pub fn next_entity_id(&mut self) -> Entity {
        let result = Entity(self.next_id);
        self.next_id += 1;
        result
    }

    pub fn add_attribute(
        &mut self,
        name: &str,
        type_: Type,
        cardinality: Cardinality,
        doc: &str,
    ) -> Result<()> {
        if let Some(_) = self.get_attribute_id(Attribute(name.to_string())) {
            return Err("Attribute already defined".to_string());
        }

        let e = self.next_entity_id();

        self.insert_raw(e, self.builtins.ident, name);
        self.insert_raw(e, self.builtins.type_, self.builtins.type_entity(type_));
        self.insert_raw(
            e,
            self.builtins.card,
            self.builtins.card_entity(cardinality),
        );
        self.insert_raw(e, self.builtins.doc, doc);

        Ok(())
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

    pub fn insert(&mut self, datom: Datom) -> Result<()> {
        let a = match self.get_attribute_id(datom.a.clone()) {
            Some(id) => Ok(id),
            None => Err(format!("Could not find id for attribute `{}`", datom.a.0)),
        }?;

        if let Some(expected) = self.get_attribute_type(a) {
            match (expected, &datom.v) {
                (Type::Bool, Value::Bool(_)) => {}
                (Type::Float, Value::Float(_)) => {}
                (Type::Int, Value::Int(_)) => {}
                (Type::Ref, Value::Ref(_)) => {}
                (Type::Str, Value::Str(_)) => {}
                _ => {
                    return Err(format!(
                        "Invalid type for attribute `{}`: Expected {:?}, got {:?}",
                        datom.a.0, expected, datom.v
                    ))
                }
            }
        } else {
            return Err(format!(
                "Could not determine required type for attribute `{}`. DB corrupt?",
                datom.a.0
            ));
        }

        self.insert_raw(datom.e, a, datom.v);

        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = &EAV> + '_ {
        self.eav.iter()
    }

    pub fn iter_entity(&self, e: Entity) -> impl Iterator<Item = EAV> + '_ {
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

    pub fn iter_entity_attribute(&self, e: Entity, a: Entity) -> impl Iterator<Item = EAV> + '_ {
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

    pub fn iter_attribute_value(&self, a: Entity, v: Value) -> impl Iterator<Item = EAV> + '_ {
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

    fn get_entity_by_ident(&self, ident: &str) -> Option<Entity> {
        self.iter_attribute_value(self.builtins.ident, Value::Str(ident.to_string()))
            .next()
            .map(|eav| eav.e)
    }

    fn get_attribute_id(&self, a: Attribute) -> Option<Entity> {
        self.get_entity_by_ident(&a.0)
    }

    fn get_attribute(&self, e: Entity) -> Option<Attribute> {
        self.iter_entity_attribute(e, self.builtins.ident)
            .next()
            .and_then(|eav| match eav.v {
                Value::Str(a) => Some(Attribute(a)),
                _ => None,
            })
    }

    fn get_attribute_type(&self, e: Entity) -> Option<Type> {
        self.iter_entity_attribute(e, self.builtins.type_)
            .next()
            .and_then(|eav| match eav.v {
                Value::Ref(i) => {
                    if i == self.builtins.float.0 {
                        Some(Type::Float)
                    } else if i == self.builtins.int.0 {
                        Some(Type::Int)
                    } else if i == self.builtins.string.0 {
                        Some(Type::Str)
                    } else if i == self.builtins.ref_.0 {
                        Some(Type::Ref)
                    } else if i == self.builtins.bool.0 {
                        Some(Type::Bool)
                    } else {
                        None
                    }
                }
                _ => None,
            })
    }

    fn resolve_pattern(&self, p: &Pattern<Attribute>) -> Result<Pattern<Entity>> {
        let a = match &p.a {
            Entry::Var(v) => Ok(Entry::Var(v.clone())),
            Entry::Lit(a) => {
                if let Some(id) = self.get_attribute_id(a.clone()) {
                    Ok(Entry::Lit(id))
                } else {
                    Err("Id not found".to_string())
                }
            }
        }?;

        Ok(Pattern {
            e: p.e.clone(),
            a,
            v: p.v.clone(),
        })
    }

    pub fn resolve_where(&self, whr: &Where<Attribute>) -> Result<Where<Entity>> {
        match whr {
            Where::Pattern(p) => Ok(Where::Pattern(self.resolve_pattern(&p)?)),
            Where::And(l, r) => Ok(Where::and(self.resolve_where(l)?, self.resolve_where(r)?)),
            Where::Or(l, r) => Ok(Where::or(self.resolve_where(l)?, self.resolve_where(r)?)),
        }
    }

    pub fn resolve_result(&self, table: HashSet<Vec<Value>>) -> HashSet<Vec<Value>> {
        table
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|val| {
                        if let Value::Ref(i) = val {
                            if let Some(Attribute(a)) = self.get_attribute(Entity(i)) {
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

    pub fn save<B: Backend>(&self, mut backend: B) -> Result<B> {
        backend.save(self.iter())
    }

    pub fn load<B: Backend>(mut backend: B) -> Result<Self> {
        let mut s = Store {
            eav: BTreeSet::new(),
            ave: BTreeSet::new(),
            builtins: Builtins::default(),
            next_id: 0,
            next_tx: 1,
        };

        let mut ident = Entity(0);
        for eav in backend.load()? {
            if eav.e == eav.a && eav.v == Value::Str("db/ident".to_string()) {
                ident = eav.e;
            }
            s.insert_raw(eav.e, eav.a, eav.v);
        }

        // Need this to recover builtin entities
        s.builtins.ident = ident;

        if let Some(last) = s.eav.last() {
            s.next_id = last.e.0 + 1;
        }

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

        s.builtins = Builtins {
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
        };

        Ok(s)
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
    use crate::datom;

    use super::{Cardinality, Store, Type};

    #[test]
    fn insert_type_check() {
        let mut s = Store::new();
        s.add_attribute("foo", Type::Int, Cardinality::One, "")
            .unwrap();

        s.insert(datom!(100, :foo 4)).unwrap();

        assert!(s.insert(datom!(200, :foo "Bar")).is_err());
    }
}
