use aligned_vec::avec;
use diol::prelude::*;
use rayon::prelude::*;

#[repr(transparent)]
struct UnsafeCell<T: ?Sized>(std::cell::UnsafeCell<T>);
unsafe impl<T: ?Sized + Sync> Sync for UnsafeCell<T> {}

fn seq(bencher: Bencher, (m, n): (usize, usize)) {
	let mat = &mut *avec![0.0; m * n];

	bencher.bench(|| {
		mat.fill(0.0);
		for _ in 0..n {
			for col in mat.chunks_exact_mut(m) {
				for e in col {
					*e += 1.0;
				}
			}
		}
		for e in &*mat {
			assert_eq!(*e, n as f64);
		}
	});
}

fn par_rayon(bencher: Bencher, (m, n): (usize, usize)) {
	let mat = &mut *avec![0.0; m * n];

	bencher.bench(|| {
		mat.fill(0.0);
		for _ in 0..n {
			mat.par_chunks_exact_mut(m).for_each(|col| {
				for e in &mut *col {
					*e += 1.0;
				}
			})
		}
		for e in &*mat {
			assert_eq!(*e, n as f64);
		}
	});
}

fn par_scope_coarse(bencher: Bencher, (m, n): (usize, usize)) {
	let mat = &mut *avec![0.0; m * n];
	let n_jobs = rayon::current_num_threads();

	bencher.bench(|| {
		mat.fill(0.0);

		syncthreads::scope(n_jobs, |scope| {
			for _ in 0..n {
				scope.for_each(mat.chunks_exact_mut(m * n / n_jobs).collect(), |_, cols| {
					for col in cols.chunks_exact_mut(m) {
						for e in col {
							*e += 1.0;
						}
					}
				});
			}
		});
		for e in &*mat {
			assert_eq!(*e, n as f64);
		}
	});
}

fn par_scope_fine(bencher: Bencher, (m, n): (usize, usize)) {
	let mat = &mut *avec![0.0; m * n];
	let n_jobs = rayon::current_num_threads() * 8;

	bencher.bench(|| {
		mat.fill(0.0);

		syncthreads::scope(n_jobs, |scope| {
			for _ in 0..n {
				scope.for_each(mat.chunks_exact_mut(m * n / n_jobs).collect(), |_, cols| {
					for col in cols.chunks_exact_mut(m) {
						for e in col {
							*e += 1.0;
						}
					}
				});
			}
		});
		for e in &*mat {
			assert_eq!(*e, n as f64);
		}
	});
}

fn par_sync_free(bencher: Bencher, (m, n): (usize, usize)) {
	let mat = &mut *avec![0.0; m * n];

	bencher.bench(|| unsafe {
		mat.fill(0.0);
		{
			let mat = &*(mat as *mut [f64] as *const [UnsafeCell<f64>]);

			rayon::broadcast(|cx| {
				let tid = cx.index();
				let n_threads = cx.num_threads();

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
		for e in &*mat {
			assert_eq!(*e, n as f64);
		}
	});
}

fn main() -> std::io::Result<()> {
	let n = 256usize.next_multiple_of(rayon::current_num_threads() * 8);

	let mut bench = Bench::new(BenchConfig::from_args()?);
	bench.register_many(
		list![seq, par_scope_fine, par_scope_coarse, par_rayon, par_sync_free],
		[(256, n), (512, 2 * n), (2048, 2 * n), (4096, 2 * n), (8192, 2 * n)],
	);
	bench.run()?;

	Ok(())
}
