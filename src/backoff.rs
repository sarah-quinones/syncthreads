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
}

#[allow(dead_code)]
impl Backoff {
    #[inline]
    pub fn new(spin_limit: u32, yield_limit: u32) -> Self {
        Backoff {
            step: Cell::new(0),
            spin_limit: 1 << spin_limit,
            yield_limit: Ord::max(1 << spin_limit, 1 << yield_limit),
        }
    }

    pub fn reset(&self) {
        self.step.set(0);
    }

    #[inline]
    pub fn snooze(&self) {
        if self.step.get() <= self.spin_limit {
            hint::spin_loop();
        } else {
            ::std::thread::yield_now();
        }

        if self.step.get() <= self.yield_limit {
            self.step.set(self.step.get() + 1);
        }
    }

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

pub fn best_limit_bench(nthreads: usize, min_time: std::time::Duration) -> (SpinLimit, YieldLimit) {
    // don't call ThreadPool::new to avoid recursing on AUTOTUNE
    let mut pool =
        crate::pool::ThreadPool::new_imp(nthreads, &mut |_| std::thread::Builder::new()).unwrap();

    let min_time = min_time / 10;
    let max_time = 5 * min_time;
    let n_spin = 128;

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

    for limit in (0..20).step_by(2) {
        let now = std::time::Instant::now();
        work(n_iters, SpinLimit(limit), YieldLimit(10));

        let delta = now.elapsed();
        if delta < time {
            time = delta;
            best = limit;
        }
        if delta > max_time {
            continue;
        }
    }
    let spin_limit = SpinLimit(best);

    let mut best = 0;
    let mut time = std::time::Duration::from_nanos(u64::MAX);
    for limit in (spin_limit.0..25).step_by(2) {
        let now = std::time::Instant::now();
        work(n_iters, spin_limit, YieldLimit(limit));

        let delta = now.elapsed();
        if delta < time {
            time = delta;
            best = limit;
        }
        if delta > max_time {
            continue;
        }
    }

    let yield_limit = YieldLimit(best);
    (spin_limit, yield_limit)
}
