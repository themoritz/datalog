use std::{collections::HashSet, hint::black_box};

use criterion::{criterion_group, criterion_main, Criterion};

use datalog::{
    movies::STORE,
    query,
    query::{Entry, Pattern, Query, Var, Where},
    where_, Attribute, Value,
};

fn run_query(name: &str) -> HashSet<Vec<Value>> {
    let q = query! {
        find: [?director, ?movie],
        where: [
            [?a, :person/name name]
            [?m, :movie/cast ?a]
            [?m, :movie/title ?movie]
            [?m, :movie/director ?d]
            [?d, :person/name ?director]
        ]
    };

    q.qeval(&STORE).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("arnold", |b| {
        b.iter(|| run_query(black_box("Arnold Schwarzenegger")))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
