use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use datalog::{movies::STORE, query, where_, Attribute, Entry, Pattern, Query, Value, Var, Where};

fn run_query(name: &str) -> Vec<Vec<Value>> {
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
