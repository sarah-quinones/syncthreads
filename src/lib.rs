#![allow(non_snake_case)]

use std::any::Any;
use std::cell::{Cell, UnsafeCell};
use std::hint::black_box;
use std::ptr::{null, null_mut};
use std::sync::Arc;
use std::sync::atomic::Ordering::*;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicUsize};

use aarc::AtomicArc;
use barrier::{BarrierInit, Exit};
use crossbeam::utils::CachePadded;
use rayon::iter::plumbing::{Producer, ProducerCallback};
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

pub const SPIN_LIMIT: AtomicU32 = AtomicU32::new(65536);
pub const PAUSE_PER_SPIN: AtomicU32 = AtomicU32::new(8);

mod barrier;

#[cfg(not(miri))]
type ThdScope<'a, 'b> = &'a rayon::Scope<'b>;
#[cfg(miri)]
type ThdScope<'a, 'b> = &'a std::thread::Scope<'a, 'b>;

struct Defer<F: FnMut()>(F);
impl<F: FnMut()> Drop for Defer<F> {
	fn drop(&mut self) {
		self.0()
	}
}

struct Node {
	children: Box<[(AtomicBool, AtomicArc<Node>)]>,
	parent: *const Node,

	init: Arc<BarrierInit>,

	func: UnsafeCell<Option<fn(*const (), usize, *const AtomicPtr<Box<dyn Any + Send>>)>>,
	func_data: AtomicPtr<()>,

	jobs_rem: Box<[CachePadded<AtomicUsize>]>,
	n_jobs: AtomicUsize,
	rem: AtomicUsize,
	team_size: AtomicUsize,
	panic_slot: AtomicPtr<Box<dyn Any + Send>>,

	barrier: UnsafeCell<barrier::Barrier>,
}

impl Node {
	fn new(init: Arc<BarrierInit>, parent: *const Node) -> Self {
		let n_threads = init.thread_ids.len();
		Node {
			children: std::iter::repeat_with(|| (AtomicBool::new(false), AtomicArc::new(None)))
				.take(n_threads)
				.collect::<Box<[_]>>(),
			parent,
			n_jobs: AtomicUsize::new(0),
			barrier: UnsafeCell::new(init.clone().barrier().unwrap()),
			jobs_rem: std::iter::repeat_with(|| AtomicUsize::new(0).into())
				.take(n_threads)
				.collect::<Box<[_]>>(),
			panic_slot: AtomicPtr::new(null_mut()),
			func: UnsafeSync(UnsafeCell::new(None)).0,
			func_data: AtomicPtr::new(null_mut()),
			rem: AtomicUsize::new(0),
			team_size: AtomicUsize::new(0),
			init,
		}
	}
}

unsafe impl Send for Node {}
unsafe impl Sync for Node {}

struct Scope<'a, 'b> {
	n_threads: usize,
	waker: AtomicU32,
	spawn: &'a (dyn Sync + Fn(&Scope, ThdScope)),
	n_spawn: &'a AtomicUsize,
	rayon: Option<ThdScope<'a, 'b>>,
	node: Node,
	children: Box<[(AtomicUsize, AtomicArc<Scope<'static, 'static>>)]>,
	children_thread_request: AtomicUsize,
	done: AtomicBool,
}

thread_local! {
	static SELF: Cell<*mut Node> = const { Cell::new(null_mut()) };
	static PARENT: Cell<*const Node> = const { Cell::new(null_mut()) };
	static ROOT: Cell<*const Scope<'static, 'static>> = const { Cell::new(null_mut()) };
}

pub fn current_num_threads() -> usize {
	if ROOT.get().is_null() {
		rayon::current_num_threads()
	} else {
		unsafe { (*ROOT.get()).node.init.current_nthreads() }
	}
}

pub fn current_thread_index() -> usize {
	if ROOT.get().is_null() {
		rayon::current_thread_index().unwrap_or(0)
	} else {
		unsafe {
			if SELF.get().is_null() {
				(*(*PARENT.get()).barrier.get()).thread_id()
			} else {
				(*(*SELF.get()).barrier.get()).thread_id()
			}
		}
	}
}

pub fn with_lock<R: Send>(max_threads: usize, f: impl Send + FnOnce() -> R) -> R {
	assert_ne!(max_threads, 0);

	#[cfg(not(miri))]
	let n_threads = rayon::current_num_threads();
	#[cfg(miri)]
	let n_threads = usize::MAX;

	let n_threads = Ord::min(n_threads, max_threads);
	let init = Arc::new(barrier::BarrierInit::new(n_threads));

	let done = AtomicBool::new(false);
	let waker = AtomicU32::new(0);
	let n_spawn = &AtomicUsize::new(1);

	let node = Node::new(init.clone(), null());
	let children_thread_request = AtomicUsize::new(0);

	let top_level = ROOT.get().is_null();

	let mut scope = Scope {
		done,
		waker,
		spawn: &|scope, _: ThdScope| unsafe {
			worker_loop(scope, top_level);
		},
		n_threads,
		n_spawn,
		rayon: None,
		node,
		children: std::iter::repeat_with(|| (AtomicUsize::new(usize::MAX), AtomicArc::new(None)))
			.take(n_threads)
			.collect::<Box<[_]>>(),
		children_thread_request,
	};
	let spawn = scope.spawn;

	if ROOT.get().is_null() {
		#[cfg(not(miri))]
		use rayon::scope as __scope__;
		#[cfg(miri)]
		use std::thread::scope as __scope__;

		__scope__(|rayon_scope| {
			scope.rayon = Some(unsafe { core::mem::transmute(rayon_scope) });

			for _ in 1..n_threads {
				scope.n_spawn.fetch_add(1, Relaxed);
				#[cfg(not(miri))]
				rayon_scope.spawn(|s| spawn(&scope, s));
				#[cfg(miri)]
				rayon_scope.spawn(|| spawn(&scope, rayon_scope));
			}

			let this = SELF.replace(&raw const scope.node as _);
			let root = ROOT.replace(&raw const scope as _);

			let __guard__ = Defer(|| {
				scope.waker.store(1, Release);
				scope.done.store(true, Release);
				atomic_wait::wake_all(&scope.waker);
				SELF.set(this);
				ROOT.set(root);
			});

			f()
		})
	} else {
		unsafe {
			let root = &*ROOT.get();
			scope.rayon = core::mem::transmute(root.rayon);
			scope.n_spawn = root.n_spawn;
			scope.spawn = root.spawn;
			let scope = aarc::Arc::<Scope>::new(core::mem::transmute(scope));

			let root = &*ROOT.replace(&raw const *scope as _);
			let this = SELF.replace(&raw const scope.node as _);
			let parent = PARENT.replace(null_mut());

			let idx;
			let waker;

			root.children_thread_request.fetch_add(n_threads - 1, Release);
			waker = root.waker.fetch_or(1, Release);
			atomic_wait::wake_all(&root.waker);
			'register: loop {
				for (i, p) in root.children.iter().enumerate() {
					if p.0.load(Relaxed) == usize::MAX {
						if p.0.compare_exchange(usize::MAX, n_threads - 1, AcqRel, Relaxed).is_ok() {
							idx = i;
							p.1.store(Some(&scope));

							break 'register;
						}
					}
				}
			}

			let __guard__ = Defer(|| {
				PARENT.set(parent);
				ROOT.set(root);
				SELF.set(this);

				root.waker.store(waker, Release);
				scope.waker.store(1, Release);
				scope.done.store(true, Release);
				atomic_wait::wake_all(&scope.waker);

				root.children[idx].1.store(None::<&aarc::Arc<Scope>>);
				let empty_seats = root.children[idx].0.swap(usize::MAX, AcqRel);
				root.children_thread_request.fetch_sub(empty_seats, Release);

				(*scope.node.barrier.get()).wait_and_null(&AtomicPtr::new(null_mut()), Exit::Late);
			});

			f()
		}
	}
}

unsafe fn worker_loop(scope: &Scope<'_, '_>, top_level: bool) {
	let node = &scope.node;
	let children_thread_request = &scope.children_thread_request;
	let waker = &scope.waker;
	let init = &scope.node.init;
	let n_spawn = &scope.n_spawn;
	let done = &scope.done;

	unsafe {
		let root = ROOT.replace(&raw const *scope as _);
		let parent = PARENT.replace(&raw const scope.node as _);
		let this = SELF.replace(null_mut());
		let __guard__ = Defer(|| {
			PARENT.set(parent);
			ROOT.set(root);
			SELF.set(this);
		});
		let node = node;

		if scope.done.load(Acquire) {
			if top_level {
				n_spawn.fetch_sub(1, Relaxed);
			}
			return;
		}
		let barrier = 'search_loop: loop {
			let barrier = match init.clone().barrier() {
				Some(b) => b,
				None => panic!(),
			};

			let tid = barrier.thread_id();

			let mut spin = 0;
			let max_spin = PAUSE_PER_SPIN.load(Relaxed);
			let limit = SPIN_LIMIT.load(Relaxed) / max_spin;

			'work_loop: loop {
				if done.load(Acquire) {
					break 'search_loop barrier;
				}

				let f_data = node.func_data.load(Acquire);
				if !f_data.is_null() {
					spin = 0;

					thd_work(tid, node, None, false);

					if barrier.wait_and_null(&node.func_data, Exit::WhenPositive(children_thread_request)) {
						break 'work_loop;
					}
				} else {
					if children_thread_request.load(Acquire) > 0 {
						barrier.wait_and_null(&node.func_data, Exit::Early);
						break 'work_loop;
					}

					{
						if spin < limit {
							for _ in 0..max_spin {
								core::hint::spin_loop();
							}
							spin += 1;
						} else {
							atomic_wait::wait(&waker, 0);
						}
					}
				}
			}

			for (idx, child) in scope.children.iter().enumerate() {
				_ = idx;
				let mut child_jobs = child.0.load(Acquire);
				while child_jobs != usize::MAX && child_jobs > 0 {
					match child.0.compare_exchange(child_jobs, child_jobs - 1, AcqRel, Acquire) {
						Ok(_) => {
							if children_thread_request.fetch_sub(1, Release) == 0 {
								panic!();
							}
							loop {
								if let Some(child) = child.1.load() {
									worker_loop(&*child, false);
									continue 'search_loop;
								}
								if child.0.load(Acquire) == usize::MAX {
									break;
								}
							}
						},
						Err(new) => child_jobs = new,
					}
				}
			}
		};
		if top_level {
			n_spawn.fetch_sub(1, Relaxed);
		} else {
			barrier.wait_and_null(&AtomicPtr::new(null_mut()), Exit::Early);
		}
	}
}

fn for_each_raw_imp(n_jobs: usize, f: &(dyn Sync + Fn(usize))) {
	type F<'a> = &'a (dyn Sync + Fn(usize));

	let root = ROOT.with(|p| p.get());

	let this = SELF.with(|p| p.replace(null_mut()));
	let parent = PARENT.with(|p| p.replace(null_mut()));
	let __guard__ = Defer(move || {
		PARENT.with(|p| p.set(parent));
		SELF.with(|p| p.set(this));
	});

	if root.is_null() {
		(0..n_jobs).into_par_iter().for_each(f);
	} else {
		unsafe {
			let root = &*root;
			if this.is_null() {
				let node = aarc::Arc::new(Node::new(Arc::new(BarrierInit::new(root.n_threads)), parent));
				let node_ptr = &raw const *node;
				SELF.with(|p| p.set(node_ptr as _));
				let __guard__ = Defer(|| SELF.with(|p| p.set(null_mut())));

				assert!(!parent.is_null());
				let parent = &*parent;

				let idx;
				'register: loop {
					for (i, p) in parent.children.iter().enumerate() {
						if !p.0.load(Relaxed) {
							if !p.0.fetch_or(true, Relaxed) {
								idx = i;
								p.1.store(Some(&node));
								break 'register;
							}
						}
					}
				}
				let __guard__ = Defer(|| {
					parent.children[idx].1.store(None::<&aarc::Arc<Node>>);
					parent.children[idx].0.store(false, Relaxed);
				});

				for_each_raw_imp(n_jobs, f);
			} else {
				assert!(parent.is_null());
				PARENT.with(|p| p.set(this));
				let __guard__ = Defer(|| PARENT.set(null_mut()));

				let this = &*{ this };
				#[cfg(not(miri))]
				if let Some(rayon) = root.rayon {
					if false {
						for _ in root.n_spawn.load(Relaxed)..root.n_threads {
							(root.spawn)(&*root, rayon);
						}
					}
				}

				let storage = &mut std::mem::MaybeUninit::<Box<dyn Any + Send>>::uninit();
				let f = &f;

				let func = (|f: *const (), tid: usize, panic_slot: *const AtomicPtr<Box<dyn Any + Send>>| match std::panic::catch_unwind(|| {
					(&*(f as *const F))(tid)
				}) {
					Ok(()) => {},
					Err(panic) => {
						let ptr = (*panic_slot).swap(null_mut(), Relaxed);
						if !ptr.is_null() {
							ptr.write(panic);
						}
					},
				}) as fn(*const (), usize, *const AtomicPtr<Box<dyn Any + Send>>);

				*this.func.get() = Some(func);
				let team_size = (*this.barrier.get()).init.current_nthreads();
				this.team_size.store(team_size, Relaxed);
				let (div, rem) = (n_jobs / team_size, n_jobs % team_size);
				this.rem.store(rem, Relaxed);
				for j in &this.jobs_rem {
					j.store(div, Relaxed);
				}
				this.panic_slot.store(storage.as_mut_ptr(), Relaxed);
				this.n_jobs.store(n_jobs, Relaxed);

				root.waker.store(1, Relaxed);
				let f_data = f as *const F as *mut ();
				this.func_data.store(f_data, Release);
				atomic_wait::wake_all(&root.waker);

				thd_work((*this.barrier.get()).thread_id(), &this, Some(this), false);

				(*this.barrier.get()).wait_and_null(&this.func_data, Exit::Late);
				root.waker.store(0, Relaxed);

				if this.panic_slot.load(Relaxed).is_null() {
					let panic = storage.as_ptr().read();
					std::panic::resume_unwind(panic);
				}
			}
		}
	}
}

fn for_each_imp<T, I: IndexedParallelIterator<Item = T>>(n_jobs: usize, iter: I, f: &(dyn Sync + Fn(T))) {
	struct C<'a, T>(&'a (dyn Sync + Fn(T)), usize, usize);
	impl<T> ProducerCallback<T> for C<'_, T> {
		type Output = ();

		fn callback<P>(self, mut producer: P) -> Self::Output
		where
			P: Producer<Item = T>,
		{
			let len = self.1;
			let n_jobs = self.2;

			let mut v = Vec::with_capacity(len);

			let div = len / n_jobs;
			let rem = len % n_jobs;

			for _ in 0..rem {
				let left;
				(left, producer) = producer.split_at(div + 1);
				v.push(UnsafeSync(UnsafeCell::new(Some(left))));
			}
			for _ in rem..n_jobs {
				let left;
				(left, producer) = producer.split_at(div);
				v.push(UnsafeSync(UnsafeCell::new(Some(left))));
			}

			let f = self.0;

			for_each_raw(n_jobs, |idx: usize| unsafe {
				let p = (&mut *v[idx].0.get()).take().unwrap();
				p.into_iter().for_each(f);
			});
		}
	}
	let len = iter.len();

	iter.with_producer(C(f, len, n_jobs));
}

pub fn for_each<T, I: IntoParallelIterator<Iter: IndexedParallelIterator, Item = T>>(n_jobs: usize, iter: I, f: impl Sync + Fn(T)) {
	for_each_imp(n_jobs, iter.into_par_iter(), &f);
}

pub fn for_each_raw(n_jobs: usize, f: impl Sync + Fn(usize)) {
	for_each_raw_imp(n_jobs, (&f) as &(dyn Sync + Fn(usize)));
}

#[doc(hidden)]
#[derive(Copy, Clone, Debug)]
pub struct UnsafeSync<T>(pub T);
unsafe impl<T> Sync for UnsafeSync<T> {}
unsafe impl<T> Send for UnsafeSync<T> {}

unsafe fn thd_work(thd_id: usize, node: &Node, watch: Option<&Node>, called_from_parent: bool) {
	unsafe {
		let n_jobs = node.n_jobs.load(Relaxed);
		let team_size = node.team_size.load(Relaxed);

		let jobs_per_thread = n_jobs / team_size;

		let f = (*node.func.get()).unwrap();
		let f_data = node.func_data.load(Relaxed);

		let arrived = || watch.is_some_and(|watch| watch.rem.load(Acquire) == usize::MAX);

		'child_search: loop {
			let len = node.children.len();
			for child in node.children.iter().cycle().skip(thd_id).take(len) {
				if child.0.load(Relaxed) {
					if let Some(child) = child.1.load().as_ref() {
						if !child.func_data.load(Relaxed).is_null() {
							if let Some(barrier) = child.init.clone().barrier() {
								if !child.func_data.load(Acquire).is_null() {
									if watch.is_some_and(|watch| !core::ptr::eq(watch, node)) {
										thd_work(barrier.thread_id(), child, watch, true);
									} else {
										thd_work(barrier.thread_id(), child, None, true);
									}
									barrier.wait_and_null(&child.func_data, Exit::Early);

									if arrived() {
										return;
									}

									continue 'child_search;
								}
							}
						}
					}
				}
			}
			break 'child_search;
		}

		if thd_id < team_size {
			loop {
				let rem = node.jobs_rem[thd_id].load(Acquire);
				if rem == 0 {
					break;
				}

				if node.jobs_rem[thd_id].compare_exchange(rem, rem - 1, Release, Relaxed).is_ok() {
					let job = jobs_per_thread * thd_id + jobs_per_thread - rem;
					f(f_data, job, &node.panic_slot);
					if arrived() {
						return;
					}

					if rem == 1 {
						break;
					}
				}
			}
		}

		loop {
			let mut max = 0;
			let mut argmax = 0;
			for other in 0..team_size {
				let val = node.jobs_rem[other].load(Acquire);
				if val > max {
					max = val;
					argmax = other;
				}
				if max == jobs_per_thread {
					break;
				}
			}
			if max == 0 {
				break;
			}

			let thd_id = argmax;
			loop {
				let rem = node.jobs_rem[thd_id].load(Acquire);
				if rem == 0 {
					break;
				}

				if node.jobs_rem[thd_id].compare_exchange(rem, rem - 1, Release, Relaxed).is_ok() {
					let job = jobs_per_thread * thd_id + jobs_per_thread - rem;
					f(f_data, job, &node.panic_slot);
					if rem == 1 {
						break;
					}
				}
			}
		}
		let mut rem = node.rem.load(Acquire);
		while rem > 0 && rem != usize::MAX {
			match node.rem.compare_exchange_weak(rem, rem - 1, Release, Acquire) {
				Ok(_) => {
					let job = n_jobs - rem;
					f(f_data, job, &node.panic_slot);
					if arrived() {
						return;
					}
				},
				Err(new) => rem = new,
			}
		}
		node.rem.store(usize::MAX, Release);

		if arrived() {
			return;
		}
		if !node.parent.is_null() && !called_from_parent {
			let parent = &*node.parent;
			thd_work((*parent.barrier.get()).thread_id(), parent, watch, false);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use aligned_vec::avec;
	use rayon::prelude::*;

	#[test]
	fn test_nested_for_each() {
		let m = 10;
		let n = 16;

		let A = &mut *avec![0.0; m * n];
		let n_jobs = 8;

		A.fill(0.0);
		with_lock(n_jobs, || {
			for _ in 0..n {
				for_each(2, A.par_chunks_exact_mut(m * n / n_jobs), |cols| {
					for_each(4, cols.par_chunks_mut(m), |col| {
						for e in col {
							*e += 1.0;
						}
					});
				});
			}
		});

		for e in &*A {
			assert_eq!(*e, n as f64);
		}
	}

	#[test]
	fn test_nested_lock() {
		let mut A = [1; 128];
		with_lock(16, || {
			with_lock(4, || {
				for_each(4, &mut A, |x| {
					*x = 0;
				});
			});
		});
		assert_eq!(A, [0; 128]);
	}

	#[test]
	fn test_scope_recursive() {
		let m = 16;
		let n = 16;
		let A = &mut *avec![0.0; m * n];
		let B = &mut *avec![0.0; m * n];
		let n_jobs = 16;

		for _ in 0..4 {
			with_lock(n_jobs, || {
				A.fill(0.0);
				B.fill(0.0);

				for_each(2, [&mut *A, &mut *B].into_par_iter(), |A| {
					with_lock(n_jobs, || {
						for _ in 0..n {
							for_each(n_jobs, A.par_chunks_mut(m * n / n_jobs), |cols| {
								for col in cols.chunks_mut(m) {
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
				for e in &*B {
					assert_eq!(*e, n as f64);
				}
			});
		}
	}
}
