use diol::prelude::*;
use rayon::prelude::*;
use std::{cell::UnsafeCell, thread::Builder};
use syncthreads::{iter, pool, pool2, AllocHint, BarrierInit};

#[derive(Copy, Clone)]
struct AssumeMtSafe<T>(T);
unsafe impl<T> Sync for AssumeMtSafe<T> {}
unsafe impl<T> Send for AssumeMtSafe<T> {}

fn sequential(bencher: Bencher, PlotArg(n): PlotArg) {
    let x = &mut *vec![1.0; n * n];

    bencher.bench(|| {
        x.fill(1.0);
        for j in 0..n {
            let head = x[j * n];

            x[n * (j + 1)..].iter_mut().for_each(|x| {
                *x += head;
            });
        }
    })
}

fn rayon_iter_chunks(bencher: Bencher, PlotArg(n): PlotArg) {
    let nthreads = rayon::current_num_threads();
    let x = &mut *vec![1.0; n * n];

    bencher.bench(|| {
        x.fill(1.0);
        for j in 0..n {
            let head = x[j * n];
            let len = x[n * (j + 1)..].len();

            if len > 0 {
                x[n * (j + 1)..]
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

fn barrier_pool(bencher: Bencher, PlotArg(n): PlotArg) {
    let nthreads = rayon::current_num_threads();
    let x = &mut *vec![1.0; n * n];

    let mut pool = pool::ThreadPool::new(nthreads, |_| Builder::new()).unwrap();
    let pool = pool.all();

    let mut alloc = AllocHint::default();

    bencher.bench(|| {
        pool.with_pool(nthreads, |pool| {
            x.fill(1.0);
            let init = BarrierInit::new(
                &mut *x,
                nthreads,
                core::mem::take(&mut alloc),
                Default::default(),
            );

            pool.broadcast(|_| {
                let mut barrier = init.barrier_ref();

                for j in 0..n {
                    let Ok((head, mine)) = syncthreads::sync!(barrier, |x| {
                        let head = x[j * n];
                        (
                            head,
                            syncthreads::iter::partition_mut(&mut x[n * (j + 1)..], nthreads),
                        )
                    }) else {
                        break;
                    };

                    for x in mine.iter_mut() {
                        *x += head;
                    }
                }
            });
            alloc = init.into_inner().1;
        });
    });
}

fn barrier_pool2(bencher: Bencher, PlotArg(n): PlotArg) {
    let nthreads = rayon::current_num_threads();
    let x = &mut *vec![1.0; n * n];

    let pool = pool2::ThreadPool::new(nthreads, |_| Builder::new()).unwrap();

    bencher.bench(|| {
        x.fill(1.0);
        let x = &*UnsafeCell::from_mut(x);
        let x = AssumeMtSafe(unsafe { &*(x as *const _ as *const [UnsafeCell<f64>]) });

        for j in 0..n {
            pool.collect(nthreads, |tid| unsafe {
                let x = { x }.0;
                let head = *x[j * n].get();
                let x = &x[n * (j + 1)..];
                let mine = iter::partition(x, nthreads).nth(tid).unwrap();
                let mine = &mut *UnsafeCell::raw_get(mine as *const _ as *const UnsafeCell<[f64]>);

                for x in mine.iter_mut() {
                    *x += head;
                }
            });
        }
    });
}
fn main() -> std::io::Result<()> {
    dbg!(rayon::current_num_threads());

    let mut bench = Bench::new(BenchConfig::from_args()?);
    bench.register_many(
        list![barrier_pool, barrier_pool2, rayon_iter_chunks, sequential],
        [40, 48, 60, 80, 100, 200, 400, 1000, 1500].map(PlotArg),
    );
    bench.run()?;

    Ok(())
}
