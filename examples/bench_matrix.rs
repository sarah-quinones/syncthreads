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

fn par_scope(bencher: Bencher, (m, n): (usize, usize)) {
	let mat = &mut *avec![0.0; m * n];
	let n_threads = rayon::current_num_threads();

	bencher.bench(|| {
		mat.fill(0.0);

		syncthreads::scope(n_threads, |scope| {
			for _ in 0..n {
				assert_eq!(n % n_threads, 0);
				scope.for_each(mat.chunks_exact_mut(m * n / n_threads).collect(), |_, cols| {
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
	let mut bench = Bench::new(BenchConfig::from_args()?);
	bench.register_many(
		list![seq, par_scope, par_rayon, par_sync_free],
		[(256, 256), (512, 512), (2048, 512), (4096, 512)],
	);
	bench.run()?;

	Ok(())
}
