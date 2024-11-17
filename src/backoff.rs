use crate::sync::{SpinLimit, YieldLimit};
use core::{cell::Cell, fmt};
use std::hint;

/// Performs exponential backoff in spin loops.
///
/// Backing off in spin loops reduces contention and improves overall performance.
///
/// This primitive can execute *YIELD* and *PAUSE* instructions, yield the current thread to the OS
/// scheduler, and tell when is a good time to block the thread using a different synchronization
/// mechanism. Each step of the back off procedure takes roughly twice as long as the previous
/// step.
///
/// # Examples
///
/// Backing off in a lock-free loop:
///
/// ```
/// use crossbeam_utils::Backoff;
/// use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
///
/// fn fetch_mul(a: &AtomicUsize, b: usize) -> usize {
///     let backoff = Backoff::new();
///     loop {
///         let val = a.load(SeqCst);
///         if a.compare_exchange(val, val.wrapping_mul(b), SeqCst, SeqCst)
///             .is_ok()
///         {
///             return val;
///         }
///         backoff.spin();
///     }
/// }
/// ```
///
/// Waiting for an [`AtomicBool`] to become `true`:
///
/// ```
/// use crossbeam_utils::Backoff;
/// use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
///
/// fn spin_wait(ready: &AtomicBool) {
///     let backoff = Backoff::new();
///     while !ready.load(SeqCst) {
///         backoff.snooze();
///     }
/// }
/// ```
///
/// Waiting for an [`AtomicBool`] to become `true` and parking the thread after a long wait.
/// Note that whoever sets the atomic variable to `true` must notify the parked thread by calling
/// [`unpark()`]:
///
/// ```
/// use crossbeam_utils::Backoff;
/// use std::{
///     sync::atomic::{AtomicBool, Ordering::SeqCst},
///     thread,
/// };
///
/// fn blocking_wait(ready: &AtomicBool) {
///     let backoff = Backoff::new();
///     while !ready.load(SeqCst) {
///         if backoff.is_completed() {
///             thread::park();
///         } else {
///             backoff.snooze();
///         }
///     }
/// }
/// ```
///
/// [`is_completed`]: Backoff::is_completed
/// [`std::thread::park()`]: std::thread::park
/// [`Condvar`]: std::sync::Condvar
/// [`AtomicBool`]: std::sync::atomic::AtomicBool
/// [`unpark()`]: std::thread::Thread::unpark
pub struct Backoff {
    step: Cell<u32>,
    spin_limit: u32,
    yield_limit: u32,
    nthreads: u32,
}

impl Backoff {
    /// Creates a new `Backoff`.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_utils::Backoff;
    ///
    /// let backoff = Backoff::new();
    /// ```
    #[inline]
    pub fn new(spin_limit: u32, yield_limit: u32, nthreads: u32) -> Self {
        Backoff {
            step: Cell::new(0),
            spin_limit,
            yield_limit,
            nthreads,
        }
    }

    /// Resets the `Backoff`.
    ///
    /// # Examples
    ///
    /// ```
    /// use crossbeam_utils::Backoff;
    ///
    /// let backoff = Backoff::new();
    /// backoff.reset();
    /// ```
    #[inline]
    pub fn reset(&self) {
        self.step.set(0);
    }

    /// Backs off in a lock-free loop.
    ///
    /// This method should be used when we need to retry an operation because another thread made
    /// progress.
    ///
    /// The processor may yield using the *YIELD* or *PAUSE* instruction.
    ///
    /// # Examples
    ///
    /// Backing off in a lock-free loop:
    ///
    /// ```
    /// use crossbeam_utils::Backoff;
    /// use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    ///
    /// fn fetch_mul(a: &AtomicUsize, b: usize) -> usize {
    ///     let backoff = Backoff::new();
    ///     loop {
    ///         let val = a.load(SeqCst);
    ///         if a.compare_exchange(val, val.wrapping_mul(b), SeqCst, SeqCst)
    ///             .is_ok()
    ///         {
    ///             return val;
    ///         }
    ///         backoff.spin();
    ///     }
    /// }
    ///
    /// let a = AtomicUsize::new(7);
    /// assert_eq!(fetch_mul(&a, 8), 7);
    /// assert_eq!(a.load(SeqCst), 56);
    /// ```
    #[inline]
    pub fn spin(&self) {
        for _ in 0..(1 << self.step.get()).min(self.nthreads.saturating_mul(8).next_power_of_two())
        {
            hint::spin_loop();
        }

        if self.step.get() <= self.spin_limit {
            self.step.set(self.step.get() + 1);
        }
    }

    /// Backs off in a blocking loop.
    ///
    /// This method should be used when we need to wait for another thread to make progress.
    ///
    /// The processor may yield using the *YIELD* or *PAUSE* instruction and the current thread
    /// may yield by giving up a timeslice to the OS scheduler.
    ///
    /// In `#[no_std]` environments, this method is equivalent to [`spin`].
    ///
    /// If possible, use [`is_completed`] to check when it is advised to stop using backoff and
    /// block the current thread using a different synchronization mechanism instead.
    ///
    /// [`spin`]: Backoff::spin
    /// [`is_completed`]: Backoff::is_completed
    ///
    /// # Examples
    ///
    /// Waiting for an [`AtomicBool`] to become `true`:
    ///
    /// ```
    /// use crossbeam_utils::Backoff;
    /// use std::sync::Arc;
    /// use std::sync::atomic::AtomicBool;
    /// use std::sync::atomic::Ordering::SeqCst;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// fn spin_wait(ready: &AtomicBool) {
    ///     let backoff = Backoff::new();
    ///     while !ready.load(SeqCst) {
    ///         backoff.snooze();
    ///     }
    /// }
    ///
    /// let ready = Arc::new(AtomicBool::new(false));
    /// let ready2 = ready.clone();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_millis(100));
    ///     ready2.store(true, SeqCst);
    /// });
    ///
    /// assert_eq!(ready.load(SeqCst), false);
    /// spin_wait(&ready);
    /// assert_eq!(ready.load(SeqCst), true);
    /// # std::thread::sleep(std::time::Duration::from_millis(500)); // wait for background threads closed: https://github.com/rust-lang/miri/issues/1371
    /// ```
    ///
    /// [`AtomicBool`]: std::sync::atomic::AtomicBool
    #[inline]
    pub fn snooze(&self) {
        if self.step.get() <= self.spin_limit {
            for _ in
                0..(1 << self.step.get()).min(self.nthreads.saturating_mul(8).next_power_of_two())
            {
                hint::spin_loop();
            }
        } else {
            ::std::thread::yield_now();
        }

        if self.step.get() <= self.yield_limit {
            self.step.set(self.step.get() + 1);
        }
    }

    /// Returns `true` if exponential backoff has completed and blocking the thread is advised.
    ///
    /// # Examples
    ///
    /// Waiting for an [`AtomicBool`] to become `true` and parking the thread after a long wait:
    ///
    /// ```
    /// use crossbeam_utils::Backoff;
    /// use std::sync::Arc;
    /// use std::sync::atomic::AtomicBool;
    /// use std::sync::atomic::Ordering::SeqCst;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// fn blocking_wait(ready: &AtomicBool) {
    ///     let backoff = Backoff::new();
    ///     while !ready.load(SeqCst) {
    ///         if backoff.is_completed() {
    ///             thread::park();
    ///         } else {
    ///             backoff.snooze();
    ///         }
    ///     }
    /// }
    ///
    /// let ready = Arc::new(AtomicBool::new(false));
    /// let ready2 = ready.clone();
    /// let waiter = thread::current();
    ///
    /// thread::spawn(move || {
    ///     thread::sleep(Duration::from_millis(100));
    ///     ready2.store(true, SeqCst);
    ///     waiter.unpark();
    /// });
    ///
    /// assert_eq!(ready.load(SeqCst), false);
    /// blocking_wait(&ready);
    /// assert_eq!(ready.load(SeqCst), true);
    /// # std::thread::sleep(std::time::Duration::from_millis(500)); // wait for background threads closed: https://github.com/rust-lang/miri/issues/1371
    /// ```
    ///
    /// [`AtomicBool`]: std::sync::atomic::AtomicBool
    #[inline]
    pub fn is_completed(&self) -> bool {
        self.step.get() > self.yield_limit
    }
}

impl fmt::Debug for Backoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backoff")
            .field("step", &self.step)
            .field("is_completed", &self.is_completed())
            .finish()
    }
}

pub fn best_limit_bench(nthreads: usize) -> (SpinLimit, YieldLimit) {
    let mut pool = crate::pool::ThreadPool::new(nthreads, |_| std::thread::Builder::new()).unwrap();

    let min_time = std::time::Duration::from_millis(10);
    let max_time = 5 * min_time;
    let n_spin = 256;

    let mut n_iters = 2;

    let mut work = |n_iters: usize, spin_limit: SpinLimit, yield_limit: YieldLimit| {
        let init = crate::sync::BarrierInit::new(nthreads, spin_limit, yield_limit);
        pool.all().broadcast(|tid| {
            let mut barrier = init.barrier_ref(tid);
            for _ in 0..n_iters {
                barrier.wait();
                for _ in 0..n_spin {
                    hint::spin_loop();
                }
            }
        });
    };

    loop {
        let now = std::time::Instant::now();

        work(n_iters, SpinLimit(6), YieldLimit(10));

        let delta = now.elapsed();
        if delta > min_time {
            break;
        }
        n_iters *= 2;
    }
    n_iters /= 2;

    let mut best = 0;
    let mut time = std::time::Duration::from_nanos(u64::MAX);

    for limit in 0..20 {
        let now = std::time::Instant::now();
        work(n_iters, SpinLimit(limit), YieldLimit(10));

        let delta = now.elapsed();
        if delta < time {
            time = delta;
            best = limit;
        }
        if delta > max_time {
            break;
        }
    }
    let spin_limit = SpinLimit(best);

    let mut best = 0;
    let mut time = std::time::Duration::from_nanos(u64::MAX);
    for limit in spin_limit.0..40 {
        let now = std::time::Instant::now();
        work(n_iters, spin_limit, YieldLimit(limit));

        let delta = now.elapsed();
        if delta < time {
            time = delta;
            best = limit;
        }
        if delta > max_time {
            break;
        }
    }

    let yield_limit = YieldLimit(best);
    (spin_limit, yield_limit)
}
