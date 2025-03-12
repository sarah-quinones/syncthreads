use std::any::Any;
use std::cell::{Cell, UnsafeCell};
use std::ptr::null_mut;
use std::sync::atomic::Ordering::*;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicUsize};

pub const SPIN_LIMIT: AtomicU32 = AtomicU32::new(1u32 << 20);

mod barrier;

pub struct DropFn<F: FnMut()>(F);
impl<F: FnMut()> Drop for DropFn<F> {
	fn drop(&mut self) {
		self.0()
	}
}

pub struct Scope<'a> {
	n_jobs: usize,
	barrier: barrier::Barrier<'a>,
	waker: &'a AtomicU32,

	func: &'a UnsafeCell<Option<fn(*const (), usize, *mut (), *const AtomicPtr<Box<dyn Any + Send>>)>>,
	func_data: &'a AtomicPtr<()>,

	data: &'a AtomicPtr<()>,
	sizeof: &'a AtomicUsize,

	started: &'a AtomicUsize,
	team_size: &'a AtomicUsize,
	panic_slot: &'a AtomicPtr<Box<dyn Any + Send>>,
}

impl Scope<'_> {
	pub fn n_jobs(&self) -> usize {
		self.n_jobs
	}

	pub fn for_each<T, F: Sync + Fn(usize, T)>(&mut self, data: Vec<T>, f: F) {
		assert_eq!(data.len(), self.n_jobs);
		let mut data = data;

		unsafe {
			data.set_len(0);
			let data = data.as_mut_ptr();

			let storage = &mut std::mem::MaybeUninit::<Box<dyn Any + Send>>::uninit();
			let f = &f;

			*self.func.get() = Some(
				|f, tid, ptr, panic_slot| match std::panic::catch_unwind(|| (&*(f as *const F))(tid, (ptr as *mut T).read())) {
					Ok(()) => {},
					Err(panic) => {
						let ptr = (*panic_slot).swap(null_mut(), Relaxed);
						if !ptr.is_null() {
							ptr.write(panic);
						}
					},
				},
			);
			self.sizeof.store(size_of::<T>(), Relaxed);
			self.data.store(data as *mut (), Relaxed);
			let team_size = self.barrier.init.current_nthreads();
			self.team_size.store(team_size, Relaxed);
			self.started.store(team_size, Relaxed);
			self.panic_slot.store(storage.as_mut_ptr(), Relaxed);

			self.waker.store(1, Relaxed);
			self.func_data.store(f as *const F as *mut (), Release);
			atomic_wait::wake_all(self.waker);

			{
				let success = Cell::new(false);

				let __guard__ = DropFn(|| {
					self.barrier.wait_and_null(self.func_data);
					self.waker.store(0, Relaxed);

					if self.panic_slot.load(Relaxed).is_null() {
						let panic = storage.as_ptr().read();
						if success.get() {
							std::panic::resume_unwind(panic);
						}
					}
				});

				{
					(*f)(0, data.read());

					let mut id = self.started.load(Relaxed);
					while id < self.n_jobs {
						match self.started.compare_exchange_weak(id, id + 1, Relaxed, Relaxed) {
							Ok(_) => {
								f(id, data.add(id).read());
							},
							Err(new) => id = new,
						}
					}
				}
				success.set(true);
			}
		}
	}
}

struct UnsafeSync<T>(T);
unsafe impl<T> Sync for UnsafeSync<T> {}

pub fn scope<R>(n_jobs: usize, f: impl FnOnce(&mut Scope) -> R) -> R {
	assert_ne!(n_jobs, 0);

	let init = &barrier::BarrierInit::new();

	let func = &UnsafeSync(UnsafeCell::new(None));
	let func_data = &AtomicPtr::new(null_mut());
	let data = &AtomicPtr::new(null_mut());
	let sizeof = &AtomicUsize::new(0);
	let team_size = &AtomicUsize::new(0);

	let started = &AtomicUsize::new(0);

	let done = &AtomicBool::new(false);
	let waker = &AtomicU32::new(0);
	let panic_slot = &AtomicPtr::new(null_mut());

	let mut scope = Scope {
		panic_slot,
		waker,
		n_jobs,
		barrier: init.leader(),
		func: &func.0,
		func_data,
		data,
		sizeof,
		started,
		team_size,
	};

	rayon::in_place_scope(|thd_scope| {
		for _ in 0..n_jobs - 1 {
			thd_scope.spawn(move |_| unsafe {
				let mut barrier = init.follower();

				let tid = barrier.thread_id();
				let mut spin = 0;
				loop {
					if done.load(Acquire) {
						break;
					}

					let f_data = func_data.load(Acquire);
					if !f_data.is_null() {
						spin = 0;
						let f = (*func.0.get()).unwrap();
						let sizeof = sizeof.load(Relaxed);
						let base = data.load(Relaxed);
						let team_size = team_size.load(Relaxed);

						if tid < team_size {
							let id = tid;
							let data = base.wrapping_byte_add(sizeof * id);
							f(f_data, id, data, panic_slot);
						}

						let mut id = started.load(Relaxed);
						while id < n_jobs {
							match started.compare_exchange_weak(id, id + 1, Relaxed, Relaxed) {
								Ok(_) => {
									let data = base.wrapping_byte_add(sizeof * id);
									f(f_data, id, data, panic_slot);
								},
								Err(new) => id = new,
							}
						}
						barrier.wait_and_null(func_data);
					} else {
						if spin < SPIN_LIMIT.load(Relaxed) {
							spin += 1;
							core::hint::spin_loop();
						} else {
							spin = 0;
							atomic_wait::wait(&waker, 0);
						}
					}
				}
			});
		}

		let __guard__ = DropFn(|| {
			done.store(true, Release);
			atomic_wait::wake_all(waker);
		});
		f(&mut scope)
	})
}

#[cfg(test)]
fn std_scope<R>(n_jobs: usize, f: impl FnOnce(&mut Scope) -> R) -> R {
	assert_ne!(n_jobs, 0);

	let init = &barrier::BarrierInit::new();

	let func = &UnsafeSync(UnsafeCell::new(None));
	let func_data = &AtomicPtr::new(null_mut());
	let data = &AtomicPtr::new(null_mut());
	let sizeof = &AtomicUsize::new(0);
	let team_size = &AtomicUsize::new(0);

	let started = &AtomicUsize::new(0);

	let done = &AtomicBool::new(false);
	let waker = &AtomicU32::new(0);
	let panic_slot = &AtomicPtr::new(null_mut());

	let mut scope = Scope {
		panic_slot,
		waker,
		n_jobs,
		barrier: init.leader(),
		func: &func.0,
		func_data,
		data,
		sizeof,
		started,
		team_size,
	};

	std::thread::scope(|std_scope| {
		for _ in 0..n_jobs - 1 {
			std_scope.spawn(move || unsafe {
				let mut barrier = init.follower();

				let tid = barrier.thread_id();
				let mut spin = 0;
				loop {
					if done.load(Acquire) {
						break;
					}

					let f_data = func_data.load(Acquire);
					if !f_data.is_null() {
						spin = 0;
						let f = (*func.0.get()).unwrap();
						let sizeof = sizeof.load(Relaxed);
						let base = data.load(Relaxed);
						let team_size = team_size.load(Relaxed);

						if tid < team_size {
							let id = tid;
							let data = base.wrapping_byte_add(sizeof * id);
							f(f_data, id, data, panic_slot);
						}

						let mut id = started.load(Relaxed);
						while id < n_jobs {
							match started.compare_exchange_weak(id, id + 1, Relaxed, Relaxed) {
								Ok(_) => {
									let data = base.wrapping_byte_add(sizeof * id);
									f(f_data, id, data, panic_slot);
								},
								Err(new) => id = new,
							}
						}
						barrier.wait_and_null(func_data);
					} else {
						if spin < SPIN_LIMIT.load(Relaxed) {
							spin += 1;
							core::hint::spin_loop();
						} else {
							spin = 0;
							atomic_wait::wait(&waker, 0);
						}
					}
				}
			});
		}

		let __guard__ = DropFn(|| {
			done.store(true, Release);
			atomic_wait::wake_all(waker);
		});
		f(&mut scope)
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_thread() {
		{
			let m = 1;
			let n = 8;
			let n_threads = 2;
			let mat = &mut *aligned_vec::avec![0.0; m * n];
			for _ in 0..128 {
				mat.fill(0.0);

				std_scope(n_threads, |scope| {
					for _ in 0..n {
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
			}
		}
	}
}
