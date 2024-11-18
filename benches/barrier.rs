use diol::prelude::*;
use rayon::prelude::*;
use std::thread::Builder;
use syncthreads::{AllocHint, BarrierInit, SpinLimit, YieldLimit};

fn n_iters(n: usize) -> usize {
    ((n as f64).sqrt()) as usize
}

fn sequential(bencher: Bencher, PlotArg(n): PlotArg) {
    let x = &mut *vec![1.0; n];

    bencher.bench(|| {
        x.fill(1.0);
        for i in 0..n_iters(n) {
            let head = x[i];

            x[i + 1..].iter_mut().for_each(|x| {
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
        for i in 0..n_iters(n) {
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

fn barrier_pool(bencher: Bencher, PlotArg(n): PlotArg) {
    let nthreads = rayon::current_num_threads();
    let limit = syncthreads::autotune(nthreads);
    // let limit = (SpinLimit(10), YieldLimit(20));
    let x = &mut *vec![1.0; n];

    let mut pool = syncthreads::pool::ThreadPool::new(nthreads, |_| Builder::new()).unwrap();

    let mut alloc = AllocHint::default();

    bencher.bench(|| {
        x.fill(1.0);
        let init = BarrierInit::new(&mut *x, nthreads, core::mem::take(&mut alloc), limit);

        pool.all().broadcast(|_| {
            let mut barrier = init.barrier_ref();

            for i in 0..n_iters(n) {
                // syncthreads::sync::COUNTER.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

                let Ok((head, mine)) = syncthreads::sync!(barrier, |x| {
                    // syncthreads::sync::COUNTER.store(0, std::sync::atomic::Ordering::Release);
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

fn main() -> std::io::Result<()> {
    dbg!(rayon::current_num_threads());

    let mut bench = Bench::new(BenchConfig::from_args()?);
    bench.register_many(
        list![
            barrier_pool,
            rayon_iter_chunks,
            sequential,
            // barrier_pool2,
            // barrier_rayon,
        ],
        [200 * 200, 400 * 400, 1000 * 1000, 1500 * 1500].map(PlotArg),
    );
    // bench.register_many(
    //     list![barrier_pool_fork],
    //     [10, 100, 1000, 10_000, 100_000, 400_000].map(PlotArg),
    // );
    bench.run()?;

    Ok(())
}
