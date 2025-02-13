use crate::{
    store::{Builtins, Store},
    Entity,
};

struct PgStore {
    rt: tokio::runtime::Runtime,
    pool: sqlx::PgPool,
    builtins: Builtins,
    next_id: u64,
}

impl Store for PgStore {
    fn naked() -> Self {
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            let def = "postgres://localhost/postgres".to_string();
            println!("DATABASE_URL not set, using default `{def}`");
            def
        });
        let rt = tokio::runtime::Runtime::new().unwrap();
        let pool = rt.block_on(sqlx::PgPool::connect(&db_url)).unwrap();

        PgStore {
            rt,
            pool,
            builtins: Default::default(),
            next_id: 0,
        }
    }

    fn builtins(&self) -> &crate::store::Builtins {
        &self.builtins
    }

    fn set_builtins(&mut self, builtins: crate::store::Builtins) {
        self.builtins = builtins;
    }

    fn set_next_id(&mut self, next_id: crate::Entity) {
        self.next_id = next_id.0;
    }

    fn next_entity_id(&mut self) -> crate::Entity {
        let result = Entity(self.next_id);
        self.next_id += 1;
        result
    }

    fn insert_raw(
        &mut self,
        e: crate::Entity,
        a: crate::Entity,
        v: impl Clone + Into<crate::Value>,
    ) {
        let q = sqlx::query!(
            r#"INSERT INTO triples (user_id, e, a, v) VALUES ($1, $2, $3, $4)"#,
            1,
            e.0 as i64,
            a.0 as i64,
            serde_json::to_value(v.into()).unwrap(),
        );
        self.rt.block_on(q.execute(&self.pool)).unwrap();
    }

    fn retract_raw(&mut self, e: crate::Entity, a: crate::Entity, v: crate::Value) {
        let q = sqlx::query!(
            r#"DELETE FROM triples WHERE e = $1 AND a = $2 AND v = $3"#,
            e.0 as i64,
            a.0 as i64,
            serde_json::to_value(v).unwrap(),
        );
        self.rt.block_on(q.execute(&self.pool)).unwrap();
    }

    fn iter(&self) -> impl Iterator<Item = crate::store::EAV> + '_ {
        let q = sqlx::query!(r#"SELECT e, a, v FROM triples"#,);
        let rows = self.rt.block_on(q.fetch_all(&self.pool)).unwrap();
        rows.into_iter().map(|row| crate::store::EAV {
            e: Entity(row.e as u64),
            a: Entity(row.a as u64),
            v: serde_json::from_value(row.v).unwrap(),
        })
    }

    fn iter_entity(&self, e: crate::Entity) -> impl Iterator<Item = crate::store::EAV> + '_ {
        let q = sqlx::query!(r#"SELECT e, a, v FROM triples where e = $1"#, e.0 as i64,);
        let rows = self.rt.block_on(q.fetch_all(&self.pool)).unwrap();
        rows.into_iter().map(|row| crate::store::EAV {
            e: Entity(row.e as u64),
            a: Entity(row.a as u64),
            v: serde_json::from_value(row.v).unwrap(),
        })
    }

    fn iter_entity_attribute(
        &self,
        e: crate::Entity,
        a: crate::Entity,
    ) -> impl Iterator<Item = crate::store::EAV> + '_ {
        let q = sqlx::query!(
            r#"SELECT e, a, v FROM triples where e = $1 AND a = $2"#,
            e.0 as i64,
            a.0 as i64,
        );
        let rows = self.rt.block_on(q.fetch_all(&self.pool)).unwrap();
        rows.into_iter().map(|row| crate::store::EAV {
            e: Entity(row.e as u64),
            a: Entity(row.a as u64),
            v: serde_json::from_value(row.v).unwrap(),
        })
    }

    fn iter_attribute_value(
        &self,
        a: crate::Entity,
        v: crate::Value,
    ) -> impl Iterator<Item = crate::store::EAV> + '_ {
        let q = sqlx::query!(
            r#"SELECT e, a, v FROM triples where a = $1 and v = $2"#,
            a.0 as i64,
            serde_json::to_value(v).unwrap(),
        );
        let rows = self.rt.block_on(q.fetch_all(&self.pool)).unwrap();
        rows.into_iter().map(|row| crate::store::EAV {
            e: Entity(row.e as u64),
            a: Entity(row.a as u64),
            v: serde_json::from_value(row.v).unwrap(),
        })
    }
}
