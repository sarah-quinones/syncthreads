use diol::prelude::*;
use rayon::prelude::*;
use std::thread::Builder;
use syncthreads::{AllocHint, BarrierInit, SpinLimit};

fn sequential(bencher: Bencher, PlotArg(n): PlotArg) {
    let x = &mut *vec![1.0; n];

    bencher.bench(|| {
        x.fill(1.0);
        for i in 0..Ord::min(n, Ord::max(16, n / 64)) {
            let head = x[i];

            x[i + 1..].iter_mut().for_each(|x| {
                *x += head;
            });
        }
    })
}

fn rayon_iter(bencher: Bencher, PlotArg(n): PlotArg) {
    let x = &mut *vec![1.0; n];

    bencher.bench(|| {
        x.fill(1.0);
        for i in 0..Ord::min(n, Ord::max(16, n / 64)) {
            let head = x[i];

            x[i + 1..].par_iter_mut().for_each(|x| {
                *x += head;
            });
        }
    })
}

fn rayon_iter_chunks(bencher: Bencher, PlotArg(n): PlotArg) {
    let nthreads = rayon::current_num_threads();
    let x = &mut *vec![1.0; n];

    bencher.bench(|| {
        x.fill(1.0);
        for i in 0..Ord::min(n, Ord::max(16, n / 64)) {
            let head = x[i];
            let len = x[i + 1..].len();

            if len > 0 {
                x[i + 1..]
                    .par_chunks_mut(len.div_ceil(nthreads))
                    .for_each(|x| {
                        for x in x {
                            *x += head;
                        }
                    });
            }
        }
    })
}

fn barrier_rayon(bencher: Bencher, PlotArg(n): PlotArg) {
    let nthreads = rayon::current_num_threads();
    let limit = SpinLimit::best(nthreads);

    let x = &mut *vec![1.0; n];

    bencher.bench(|| {
        x.fill(1.0);
        let init = BarrierInit::new(&mut *x, nthreads, AllocHint::default(), limit);

        rayon::in_place_scope(|s| {
            for _ in 0..nthreads {
                s.spawn(|_| {
                    let mut barrier = init.barrier_ref();

                    for i in 0..Ord::min(n, Ord::max(16, n / 64)) {
                        let Ok((head, mine)) = syncthreads::sync!(barrier, |x| {
                            let (head, x) = x[i..].split_at_mut(1);
                            (head[0], syncthreads::iter::partition_mut(x, nthreads))
                        }) else {
                            break;
                        };

                        for x in mine.iter_mut() {
                            *x += head;
                        }
                    }
                });
            }
        });
    })
}

fn barrier_pool(bencher: Bencher, PlotArg(n): PlotArg) {
    let nthreads = rayon::current_num_threads();
    let limit = SpinLimit::best(nthreads);
    let x = &mut *vec![1.0; n];

    let mut pool = syncthreads::pool::ThreadPool::new(nthreads, |_| Builder::new()).unwrap();

    let mut alloc = AllocHint::default();

    bencher.bench(|| {
        x.fill(1.0);
        let init = BarrierInit::new(&mut *x, nthreads, core::mem::take(&mut alloc), limit);

        pool.all().broadcast(|_| {
            let mut barrier = init.barrier_ref();

            for i in 0..Ord::min(n, Ord::max(16, n / 64)) {
                let Ok((head, mine)) = syncthreads::sync!(barrier, |x| {
                    let (head, x) = x[i..].split_at_mut(1);
                    (head[0], syncthreads::iter::partition_mut(x, nthreads))
                }) else {
                    break;
                };

                for x in mine.iter_mut() {
                    *x += head;
                }
            }
        });

        alloc = init.into_inner().1;
    })
}

fn barrier_pool2(bencher: Bencher, PlotArg(n): PlotArg) {
    let nthreads = rayon::current_num_threads();
    let limit = SpinLimit::best(nthreads);
    let x = &mut *vec![1.0; n];

    let mut pool = syncthreads::pool::ThreadPool::new(nthreads, |_| Builder::new()).unwrap();

    bencher.bench(|| {
        x.fill(1.0);

        for i in 0..Ord::min(n, Ord::max(16, n / 64)) {
            let init = BarrierInit::new(&mut *x, nthreads, Default::default(), limit);
            pool.all().broadcast(|_| {
                let mut barrier = init.barrier_ref();

                let Ok((head, mine)) = syncthreads::sync!(barrier, |x| {
                    let (head, x) = x[i..].split_at_mut(1);
                    (head[0], syncthreads::iter::partition_mut(x, nthreads))
                }) else {
                    return;
                };

                for x in mine.iter_mut() {
                    *x += head;
                }
            });
        }
    })
}

fn barrier_pool_fork(bencher: Bencher, PlotArg(n): PlotArg) {
    let nthreads = 2 * rayon::current_num_threads();
    let limit = SpinLimit::best(nthreads);
    let mut x = &mut *vec![1.0; n];
    let mut y = &mut *vec![1.0; n];

    let mut pool = syncthreads::pool::ThreadPool::new(nthreads, |_| Builder::new()).unwrap();

    bencher.bench(|| {
        pool.all().fork2(
            &[rayon::current_num_threads(), rayon::current_num_threads()],
            |group| {
                let nthreads = group.num_threads();
                x.fill(1.0);
                let init = BarrierInit::new(&mut x, nthreads, Default::default(), limit);

                group.broadcast(|_| {
                    let mut barrier = init.barrier_ref();

                    for i in 0..Ord::min(n, Ord::max(16, n / 64)) {
                        if i % 2 == 0 {
                            continue;
                        }
                        let Ok((head, mine)) = syncthreads::sync!(barrier, |x| {
                            let (head, x) = x[i..].split_at_mut(1);
                            (head[0], syncthreads::iter::partition_mut(x, nthreads))
                        }) else {
                            break;
                        };

                        for x in mine.iter_mut() {
                            *x += head;
                        }
                    }
                });
            },
            |group| {
                let nthreads = group.num_threads();
                y.fill(1.0);
                let init = BarrierInit::new(&mut y, nthreads, Default::default(), limit);

                group.broadcast(|_| {
                    let mut barrier = init.barrier_ref();

                    for i in 0..Ord::min(n, Ord::max(16, n / 64)) {
                        if i % 2 == 1 {
                            continue;
                        }
                        let Ok((head, mine)) = syncthreads::sync!(barrier, |x| {
                            let (head, x) = x[i..].split_at_mut(1);
                            (head[0], syncthreads::iter::partition_mut(x, nthreads))
                        }) else {
                            break;
                        };

                        for x in mine.iter_mut() {
                            *x += head;
                        }
                    }
                });
            },
        );
    })
}

fn main() -> std::io::Result<()> {
    // rayon::ThreadPoolBuilder::new()
    //     .num_threads(6)
    //     .build_global()
    //     .unwrap();
    dbg!(rayon::current_num_threads());
    dbg!(SpinLimit::best(rayon::current_num_threads()));

    let mut bench = Bench::new(BenchConfig::from_args()?);
    bench.register_many(
        list![
            sequential,
            barrier_pool,
            barrier_pool2,
            barrier_rayon,
            rayon_iter,
            rayon_iter_chunks,
        ],
        [10, 100, 1000, 10_000, 100_000, 400_000].map(PlotArg),
    );
    bench.register_many(
        list![barrier_pool_fork],
        [10, 100, 1000, 10_000, 100_000, 400_000].map(PlotArg),
    );
    bench.run()?;

    Ok(())
}
