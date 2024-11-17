//! `syncthreads` is a library providing a synchronous type and dedicated threadpool.
//! These can be constructed using [`BarrierInit`].
//!
//! # Example
//! Sequential code:
//! ```
//! let n = 1000;
//! let x = &mut *vec![1.0; n];
//! for i in 0..n / 2 {
//!     let head = x[i];
//!     for x in &mut x[i + 1..] {
//!         *x += head;
//!     }
//! }
//!
//! for i in 0..n / 2 {
//!     assert_eq!(x[i], 2.0f64.powi(i as _));
//! }
//! ```
//!
//! Multithreaded code using [`Barrier`].
//! ```
//! use syncthreads::{iter, sync, BarrierInit};
//!
//! let n = 1000;
//! let x = &mut *vec![1.0; n];
//! let nthreads = 3;
//! let init = BarrierInit::new(&mut *x, nthreads, Default::default(), Default::default());
//! std::thread::scope(|scope| {
//!     for _ in 0..nthreads {
//!         scope.spawn(|| {
//!             let mut barrier = init.barrier_ref();
//!
//!             for i in 0..n / 2 {
//!                 let (head, mine) = sync!(barrier, |x| {
//!                     let (head, x) = x[i..].split_at_mut(1);
//!
//!                     (head[0], iter::partition_mut(x, nthreads))
//!                 })
//!                 .unwrap();
//!
//!                 let head = *head;
//!
//!                 for x in mine.iter_mut() {
//!                     *x += head;
//!                 }
//!             }
//!         });
//!     }
//! });
//!
//! for i in 0..n / 2 {
//!     assert_eq!(x[i], 2.0f64.powi(i as _));
//! }
//! ```
use core::{cell::UnsafeCell, fmt, sync::atomic::AtomicUsize};
use crossbeam::utils::CachePadded;
use equator::assert;
use std::sync::atomic::AtomicBool;
pub use sync::SpinLimit;

extern crate alloc;

/// Iterator utilities for splitting tasks between a fixed number of threads.
pub mod iter;
/// Low level synchronization primitives.
pub mod sync;

pub mod pool;

mod backoff;

mod dyn_vec;
use dyn_vec::DynVec;

/// Error indicating that one of the barriers in a group was dropped while the others are still
/// waiting.
#[derive(Copy, Clone, Debug)]
pub struct DropError;

impl fmt::Display for DropError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        <Self as fmt::Debug>::fmt(self, f)
    }
}

impl std::error::Error for DropError {}

/// Structure used to initialize instances of [`Barrier`].
#[derive(Debug)]
pub struct BarrierInit<T> {
    inner: sync::BarrierInit,
    data: UnsafeCell<T>,
    shared: UnsafeCell<DynVec>,
    exclusive: UnsafeCell<DynVec>,
    tid: AtomicUsize,
    tag: UnsafeCell<pretty::TypeId>,
}

/// Synchronous barrier for cooperation between multiple independent threads.
#[derive(Debug)]
pub struct Barrier<'a, T> {
    inner: sync::BarrierRef<'a>,
    data: &'a UnsafeCell<T>,
    shared: &'a UnsafeCell<DynVec>,
    exclusive: &'a UnsafeCell<DynVec>,
    tid: usize,
    tag: &'a UnsafeCell<pretty::TypeId>,
}

unsafe impl<T: Sync + Send> Sync for BarrierInit<T> {}
unsafe impl<T: Sync + Send> Send for BarrierInit<T> {}
unsafe impl<T: Sync + Send> Sync for Barrier<'_, T> {}
unsafe impl<T: Sync + Send> Send for Barrier<'_, T> {}

pub struct Shared<T> {
    taken: AtomicBool,
    value: UnsafeCell<T>,
}

pub struct Guard<'a, T> {
    taken: &'a AtomicBool,
    value: &'a mut T,
}

impl<T> Drop for Guard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.taken
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

impl<T> core::ops::Deref for Guard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<T> core::ops::DerefMut for Guard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}

impl<T> Shared<T> {
    /// Creates a new instance protecting the given value.
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            taken: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    /// Returns a mutable reference to the protected value.
    pub fn get_mut(&mut self) -> &mut T {
        self.value.get_mut()
    }

    /// Consumes `self` to return the protected value.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }

    /// Returns mutable access to the inner value if it is already taken.
    ///
    /// # Panics
    /// Panics if the value is already borrowed by a previous call to `lock`.
    #[inline]
    pub fn lock(&self) -> Guard<'_, T> {
        let taken = self
            .taken
            .fetch_or(true, std::sync::atomic::Ordering::Relaxed);
        assert!(!taken);
        Guard {
            taken: &self.taken,
            value: unsafe { &mut *self.value.get() },
        }
    }
}

/// Allocation hint provided to barrier initializers.
#[derive(Debug, Default)]
pub struct AllocHint {
    pub shared: Alloc,
    pub exclusive: Alloc,
}

/// Pre-allocated storage.
#[derive(Debug)]
pub struct Storage {
    alloc: DynVec,
}

/// Allocation or hint provided for barrier initializers.
#[derive(Debug)]
pub enum Alloc {
    /// Initial allocation size hint.
    Hint {
        /// Size of the allocation.
        size_bytes: usize,
        /// Alignment of the allocation.
        align_bytes: usize,
    },
    /// Pre-allocated storage.
    Storage(Storage),
}

impl Alloc {
    fn make_vec(self) -> DynVec {
        match self {
            Alloc::Hint {
                size_bytes,
                align_bytes,
            } => DynVec::with_capacity(align_bytes, size_bytes),
            Alloc::Storage(storage) => storage.alloc,
        }
    }
}

impl Default for Alloc {
    #[inline]
    fn default() -> Self {
        Self::Hint {
            size_bytes: 0,
            align_bytes: 1,
        }
    }
}

impl<T> BarrierInit<T> {
    /// Creates a new [`BarrierInit`] protecting the given value, with the specified number of
    /// threads.
    pub fn new(value: T, num_threads: usize, hint: AllocHint, limit: SpinLimit) -> Self {
        BarrierInit {
            inner: sync::BarrierInit::new(num_threads, limit),
            data: UnsafeCell::new(value),
            shared: UnsafeCell::new(hint.shared.make_vec()),
            exclusive: UnsafeCell::new(hint.exclusive.make_vec()),
            tid: AtomicUsize::new(0),
            tag: UnsafeCell::new(type_id_of_val(&())),
        }
    }

    /// Consumes `self`, returning the protected value and the current allocation.
    pub fn into_inner(self) -> (T, AllocHint) {
        (
            self.data.into_inner(),
            AllocHint {
                shared: Alloc::Storage(Storage {
                    alloc: self.shared.into_inner(),
                }),
                exclusive: Alloc::Storage(Storage {
                    alloc: self.exclusive.into_inner(),
                }),
            },
        )
    }

    /// Returns a mutable reference to the protected value.
    pub fn get_mut(&mut self) -> &mut T {
        self.data.get_mut()
    }

    /// Creates a new barrier referencing `self`.
    ///
    /// # Panics
    /// Panics if more than `self.thread_count()` barriers have been created since the creation of
    /// `self`, or the last time [`Self::reset`] was called.
    pub fn barrier_ref(&self) -> Barrier<'_, T> {
        let tid = self.tid.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        assert!(tid < self.num_threads());
        Barrier {
            inner: self.inner.barrier_ref(tid),
            data: &self.data,
            shared: &self.shared,
            exclusive: &self.exclusive,
            tid,
            tag: &self.tag,
        }
    }

    /// Returns the number of threads the barriers need to wait for.
    #[inline]
    pub fn num_threads(&self) -> usize {
        self.inner.num_threads()
    }

    /// Resets the barrier initializer.
    pub fn reset(&mut self) {
        *self.tid.get_mut() = 0;
    }
}

impl<'a, T> Barrier<'a, T> {
    /// Waits until `self.num_threads()` threads have called this function, at which point the last
    /// thread to arrive will call `f` with a reference to the data, splitting it into a shared
    /// section and mutable section. These are respectively sent to the other threads through a
    /// shared and exclusive reference.
    ///
    /// # Safety
    /// Threads from the same group that wait at this function at the same time must agree on the
    /// same types for `Shared`, `Exclusive`, and the type of `tag`.
    pub unsafe fn sync<'b, Shared: Sync, Exclusive: Send, I: IntoIterator<Item = Exclusive>>(
        &'b mut self,
        f: impl FnOnce(&'b mut T) -> (Shared, I),
        tag: impl core::any::Any,
    ) -> Result<(&'b Shared, Exclusive), DropError> {
        let tag = type_id_of_val(&tag);
        match self.inner.wait() {
            sync::BarrierWaitResult::Leader => {
                let exclusive = unsafe { &mut *self.exclusive.get() };
                let shared = unsafe { &mut *self.shared.get() };

                let f = f(unsafe { &mut *self.data.get() });
                shared.collect(core::iter::once(f.0));
                let mut iter = f.1.into_iter();
                exclusive.collect(
                    (&mut iter)
                        .take(self.num_threads())
                        .map(Some)
                        .map(UnsafeCell::new)
                        .map(CachePadded::new),
                );
                assert!(all(
                    exclusive.len == self.num_threads(),
                    iter.next().is_none(),
                ));
                unsafe { *self.tag.get() = tag };
                self.inner.lead();
            }
            sync::BarrierWaitResult::Follower => {
                self.inner.follow();
            }
            sync::BarrierWaitResult::Dropped => return Err(DropError),
        }
        let self_tag = unsafe { *self.tag.get() };
        assert!(tag == self_tag);

        Ok((
            unsafe { &(&*self.shared.get()).assume_ref::<Shared>()[0] },
            (unsafe {
                &mut *((&*self.exclusive.get())
                    .assume_ref::<CachePadded<UnsafeCell<Option<Exclusive>>>>()[self.tid]
                    .get())
            })
            .take()
            .unwrap(),
        ))
    }

    /// Returns the unique (among the barriers created by the same initializer) id of `self`, which
    /// is a value between `0` and `self.num_threads()`.
    #[inline]
    pub fn thread_id(&self) -> usize {
        self.tid
    }

    /// Returns the number of threads the barriers need to wait for.
    #[inline]
    pub fn num_threads(&self) -> usize {
        self.inner.num_threads()
    }
}

fn type_id_of_val<T: 'static>(_: &T) -> pretty::TypeId {
    pretty::TypeId {
        id: core::any::TypeId::of::<T>(),
        name: pretty::Str(core::any::type_name::<T>()),
    }
}

/// Safe wrapper around [`Barrier::sync`].
#[macro_export]
macro_rules! sync {
    ($bar: expr, $f:expr) => {{
        #[allow(unused_unsafe)]
        let x = unsafe { ($bar).sync($f, || {}) };
        x
    }};
}

mod pretty {
    use core::fmt;

    #[derive(Copy, Clone)]
    pub struct Str(pub &'static str);

    impl fmt::Debug for Str {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    #[derive(Copy, Clone, Debug)]
    pub struct TypeId {
        #[allow(dead_code)]
        pub name: Str,
        pub id: core::any::TypeId,
    }

    impl PartialEq for TypeId {
        #[inline]
        fn eq(&self, other: &Self) -> bool {
            self.id == other.id
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_affinity::CoreId;

    const N: usize = 1000;
    const ITERS: usize = 1;
    const SAMPLES: usize = 1;

    fn default<T: Default>() -> T {
        T::default()
    }

    #[test]
    fn test_barrier() {
        let n = N;
        let x = &mut *vec![1.0; n];
        let nthreads = rayon::current_num_threads();

        for _ in 0..SAMPLES {
            let now = std::time::Instant::now();
            for _ in 0..ITERS {
                let init = BarrierInit::new(&mut *x, nthreads, default(), default());
                std::thread::scope(|s| {
                    for _ in 0..nthreads {
                        s.spawn(|| {
                            let mut barrier = init.barrier_ref();

                            for i in 0..n / 2 {
                                let Ok((head, mine)) = sync!(barrier, |x| {
                                    let (head, x) = x[i..].split_at_mut(1);

                                    (head[0], iter::partition_mut(x, nthreads))
                                }) else {
                                    panic!();
                                };

                                for x in mine.iter_mut() {
                                    *x += head;
                                }
                            }
                        });
                    }
                });
            }
            dbg!(now.elapsed());
        }
    }

    #[test]
    fn test_barrier_threadpool() {
        let n = N;
        let nthreads = rayon::current_num_threads();
        let pool = threadpool::ThreadPool::new(nthreads);
        for tid in 0..nthreads {
            pool.execute(move || {
                core_affinity::set_for_current(CoreId { id: tid });
                std::thread::sleep(std::time::Duration::from_secs_f64(0.1));
            });
        }
        pool.join();

        for _ in 0..SAMPLES {
            let now = std::time::Instant::now();
            for _ in 0..ITERS {
                let x = &mut *vec![1.0; n];
                let nthreads = rayon::current_num_threads();
                let init = BarrierInit::new(&mut *x, nthreads, default(), default());
                threadpool_scope::scope_with(&pool, |scope| {
                    for _ in 0..nthreads {
                        scope.execute(|| {
                            let mut barrier = init.barrier_ref();

                            for i in 0..n / 2 {
                                let Ok((&head, mine)) = sync!(barrier, |x| {
                                    let (head, x) = x[i..].split_at_mut(1);
                                    (head[0], iter::partition_mut(x, nthreads))
                                }) else {
                                    panic!();
                                };
                                for x in mine.iter_mut() {
                                    *x += head;
                                }
                            }
                        });
                    }
                });
            }
            dbg!(now.elapsed());
        }
    }

    #[test]
    fn test_barrier_rayon() {
        let n = N;
        let nthreads = rayon::current_num_threads();

        let pool = rayon::ThreadPoolBuilder::new()
            .start_handler(|tid| {
                core_affinity::set_for_current(CoreId { id: tid });
            })
            .num_threads(nthreads)
            .build()
            .unwrap();

        for _ in 0..SAMPLES {
            let now = std::time::Instant::now();
            for _ in 0..ITERS {
                let x = &mut *vec![1.0; n];
                let init = BarrierInit::new(&mut *x, nthreads, default(), default());
                pool.in_place_scope(|s| {
                    for _ in 0..nthreads {
                        s.spawn(|_| {
                            let mut barrier = init.barrier_ref();

                            for i in 0..n / 2 {
                                let Ok((head, mine)) = sync!(barrier, |x| {
                                    let (head, x) = x[i..].split_at_mut(1);
                                    (head[0], iter::partition_mut(x, nthreads))
                                }) else {
                                    panic!();
                                };

                                for x in mine.iter_mut() {
                                    *x += head;
                                }
                            }
                        });
                    }
                });
            }
            dbg!(now.elapsed());
        }
    }

    #[test]
    fn test_rayon() {
        use rayon::prelude::*;
        let n = N;
        let x = &mut *vec![1.0; n];
        for _ in 0..SAMPLES {
            let now = std::time::Instant::now();
            for _ in 0..ITERS {
                for i in 0..n / 2 {
                    let [head, x @ ..] = &mut x[i..] else {
                        panic!()
                    };

                    let head = *head;
                    let len = x.len();
                    if len > 0 {
                        x.par_chunks_mut(len.div_ceil(rayon::current_num_threads()))
                            .for_each(|x| {
                                for x in x {
                                    *x += head;
                                }
                            })
                    }
                }
            }
            dbg!(now.elapsed());
        }
    }

    #[test]
    fn test_seq() {
        let n = N;
        let x = &mut *vec![1.0; n];
        for _ in 0..SAMPLES {
            let now = std::time::Instant::now();
            for _ in 0..ITERS {
                for i in 0..n / 2 {
                    let head = x[i];
                    for x in &mut x[i + 1..] {
                        *x += head;
                    }
                }
            }
            dbg!(now.elapsed());
        }
    }
}
