#![allow(non_snake_case)]

use aligned_vec::avec;
use diol::prelude::*;
use rayon::prelude::*;

#[repr(transparent)]
struct UnsafeCell<T: ?Sized>(std::cell::UnsafeCell<T>);
unsafe impl<T: ?Sized + Sync> Sync for UnsafeCell<T> {}

fn seq(bencher: Bencher, (m, n): (usize, usize)) {
	let A = &mut *avec![0.0; m * n];
	let B = &mut *avec![0.0; m * n];

	bencher.bench(|| {
		A.fill(0.0);
		B.fill(0.0);
		for _ in 0..n {
			for col in A.chunks_exact_mut(m) {
				for e in col {
					*e += 1.0;
				}
			}
		}
		for _ in 0..n {
			for col in B.chunks_exact_mut(m) {
				for e in col {
					*e += 1.0;
				}
			}
		}

		for e in &*A {
			assert_eq!(*e, n as f64);
		}
	});
}

fn par_rayon(bencher: Bencher, (m, n): (usize, usize)) {
	let A = &mut *avec![0.0; m * n];
	let B = &mut *avec![0.0; m * n];

	bencher.bench(|| {
		A.fill(0.0);
		B.fill(0.0);

		[&mut *A, &mut *B].into_par_iter().for_each(|A| {
			for _ in 0..n {
				A.par_chunks_exact_mut(m).for_each(|col| {
					for e in &mut *col {
						*e += 1.0;
					}
				})
			}
		});

		for e in &*A {
			assert_eq!(*e, n as f64);
		}
	});
}

fn par_scope_coarse(bencher: Bencher, (m, n): (usize, usize)) {
	let A = &mut *avec![0.0; m * n];
	let B = &mut *avec![0.0; m * n];
	let n_jobs = rayon::current_num_threads();

	bencher.bench(|| {
		A.fill(0.0);
		B.fill(0.0);

		[&mut *A, &mut *B].into_par_iter().for_each(|A| {
			syncthreads::with_lock(n_jobs, || {
				for _ in 0..n {
					syncthreads::for_each(n_jobs / 2, A.par_chunks_exact_mut(m * n / n_jobs), |cols| {
						for col in cols.chunks_exact_mut(m) {
							for e in col {
								*e += 1.0;
							}
						}
					});
				}
			});
		});

		for e in &*A {
			assert_eq!(*e, n as f64);
		}
	});
}

fn par_scope_fine(bencher: Bencher, (m, n): (usize, usize)) {
	let A = &mut *avec![0.0; m * n];
	let B = &mut *avec![0.0; m * n];
	let n_jobs = rayon::current_num_threads() * 8;

	bencher.bench(|| {
		A.fill(0.0);
		B.fill(0.0);

		[&mut *A, &mut *B].into_par_iter().for_each(|A| {
			syncthreads::with_lock(n_jobs, || {
				for _ in 0..n {
					syncthreads::for_each(n_jobs / 2, A.par_chunks_exact_mut(m * n / n_jobs), |cols| {
						for col in cols.chunks_exact_mut(m) {
							for e in col {
								*e += 1.0;
							}
						}
					});
				}
			});
		});

		for e in &*A {
			assert_eq!(*e, n as f64);
		}
	});
}

fn par_scope_fine_recursive(bencher: Bencher, (m, n): (usize, usize)) {
	let A = &mut *avec![0.0; m * n];
	let B = &mut *avec![0.0; m * n];
	let n_jobs = rayon::current_num_threads() * 8;

	bencher.bench(|| {
		A.fill(0.0);
		B.fill(0.0);

		syncthreads::with_lock(n_jobs, || {
			syncthreads::for_each(2, [&mut *A, &mut *B].into_par_iter(), |A| {
				for _ in 0..n {
					syncthreads::for_each(n_jobs / 2, A.par_chunks_exact_mut(m * n / n_jobs), |cols| {
						for col in cols.chunks_exact_mut(m) {
							for e in col {
								*e += 1.0;
							}
						}
					});
				}
			});
		});

		for e in &*A {
			assert_eq!(*e, n as f64);
		}
	});
}

fn par_sync_free(bencher: Bencher, (m, n): (usize, usize)) {
	let A = &mut *avec![0.0; m * n];
	let B = &mut *avec![0.0; m * n];

	bencher.bench(|| unsafe {
		A.fill(0.0);
		B.fill(0.0);
		{
			let A = &*(A as *mut [f64] as *const [UnsafeCell<f64>]);
			let B = &*(B as *mut [f64] as *const [UnsafeCell<f64>]);

			rayon::broadcast(|cx| {
				let mat = if cx.index() % 2 == 0 { A } else { B };
				let tid = cx.index() / 2;
				let n_threads = cx.num_threads() / 2;

				for _ in 0..n {
					for col in mat.chunks_exact(m * n / n_threads).skip(tid).take(1) {
						let col = std::cell::UnsafeCell::raw_get(col as *const [UnsafeCell<f64>] as *const std::cell::UnsafeCell<[f64]>);
						for col in (&mut *col).chunks_exact_mut(m) {
							for e in col {
								*e += 1.0;
							}
						}
					}
				}
			});
		}
		for e in &*A {
			assert_eq!(*e, n as f64);
		}
	});
}

fn main() -> std::io::Result<()> {
	dbg!(rayon::current_num_threads(), num_cpus::get_physical());

	let n = 256usize.next_multiple_of(rayon::current_num_threads() * 8);

	let mut bench = Bench::new(BenchConfig::from_args()?);
	bench.register_many(
		list![seq, par_scope_fine_recursive, par_scope_fine, par_scope_coarse, par_rayon, par_sync_free],
		[(256, n), (512, 2 * n), (1024, 2 * n), (2048, 2 * n), (4096, 2 * n), (8192, 2 * n)],
	);
	bench.run()?;

	Ok(())
}
