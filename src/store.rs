use std::collections::{BTreeSet, HashSet};

use crate::{
    query::{Entry, Pattern, Where},
    Attribute, Datom, Entity, Result, Value,
};

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

#[derive(Default)]
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
    }

    pub fn insert(&mut self, datom: Datom) -> Result<()> {
        let a = match self.get_attribute_id(datom.a.clone()) {
            Some(id) => Ok(id),
            None => Err(format!("Could not find id for attribute `{}`", datom.a.0)),
        }?;

        // TODO: Validate type
        // if !self.schema.valid_type(&datom.a, &datom.v) {
        //     return Err(format!(
        //         "Invalid type for attribute `{}`: {:?}",
        //         datom.a.0, datom.v
        //     ));
        // }

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

    fn get_attribute_id(&self, a: Attribute) -> Option<Entity> {
        self.iter_attribute_value(self.builtins.ident, Value::Str(a.0))
            .next()
            .map(|eav| eav.e)
    }

    fn get_attribute(&self, e: Entity) -> Option<Attribute> {
        self.iter_entity_attribute(e, self.builtins.ident)
            .next()
            .and_then(|eav| match eav.v {
                Value::Str(a) => Some(Attribute(a)),
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
                        if let Value::Int(i) = val {
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

    pub fn store_json(&self) -> serde_json::Value {
        let mut entries = vec![];

        for eav in self.eav.iter() {
            let v = match eav.v {
                Value::Int(i) => serde_json::json!(i),
                Value::Float(f) => serde_json::json!(f),
                Value::Str(ref s) => serde_json::json!(s),
            };
            entries.push(serde_json::json!([eav.e.0, eav.a.0, v]));
        }

        serde_json::Value::Array(entries)
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
    use crate::movies::DATA;

    #[test]
    fn persist_json() {
        let actual = DATA.store_json();
        let expected = serde_json::json!([
            [0, 0, "db/ident"],
            [0, 1, 7],
            [0, 2, 4],
            [0, 3, "The identifier of an attribute"],
            [1, 0, "db/type"],
            [1, 1, 6],
            [1, 2, 4],
            [1, 3, "Type of an attribute"],
            [2, 0, "db/cardinality"],
            [2, 1, 6],
            [2, 2, 4],
            [2, 3, "Cardinality of an attribute"],
            [3, 0, "db/doc"],
            [3, 1, 7],
            [3, 2, 4],
            [3, 3, "Documentation string for an attribute"],
            [4, 0, "db.cardinality/one"],
            [5, 0, "db.cardinality/many"],
            [6, 0, "db.type/ref"],
            [7, 0, "db.type/string"],
            [8, 0, "db.type/int"],
            [9, 0, "db.type/float"],
            [10, 0, "db.type/bool"],
            [11, 0, "name"],
            [11, 1, 7],
            [11, 2, 4],
            [11, 3, "The name"],
            [12, 0, "age"],
            [12, 1, 8],
            [12, 2, 4],
            [12, 3, ""],
            [100, 11, "Moritz"],
            [100, 12, 39],
            [150, 11, "Moritz"],
            [150, 12, 30],
            [200, 11, "Piet"],
            [200, 12, 39],
        ]);
        assert_eq!(actual, expected);
    }
}
