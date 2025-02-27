use std::{
    any::Any,
    mem::MaybeUninit,
    panic::{RefUnwindSafe, UnwindSafe},
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicUsize, Ordering},
        Arc, RwLock,
    },
    thread::{Builder, JoinHandle},
};

use crate::backoff::Backoff;
use crossbeam::utils::CachePadded;

struct Worker {
    handle: JoinHandle<()>,
}

pub struct Task {
    job_fn: fn(*const (), usize),
    job_data: *const (),
    job_count: usize,
    n_done: CachePadded<AtomicUsize>,
    n_observers: CachePadded<AtomicUsize>,
    done: AtomicU32,

    started: *const AtomicU32,
}

unsafe impl Sync for Task {}
unsafe impl Send for Task {}

pub struct Node {
    pub task: Task,
    pub next: AtomicPtr<Node>,
    pub prev: AtomicPtr<Node>,
}

#[derive(Debug)]
pub struct List {
    pub lock: RwLock<()>,
    pub head: AtomicPtr<Node>,
}

impl List {
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
            lock: RwLock::new(()),
        }
    }

    pub unsafe fn push(&self, node: *const Node) {
        let node = node as *mut Node;

        let __lock__ = self.lock.write().unwrap();

        let head = self.head.load(Ordering::Relaxed);
        (*node).next.store(head, Ordering::Relaxed);

        if !head.is_null() {
            (*head).prev.store(node, Ordering::Relaxed);
        }
        self.head.store(node, Ordering::Relaxed);
    }

    pub unsafe fn peek(&self) -> *const Node {
        if self.head.load(Ordering::Relaxed).is_null() {
            return ptr::null();
        }
        let __lock__ = self.lock.read().unwrap();

        let head = self.head.load(Ordering::Relaxed);
        if !head.is_null() {
            (*head).task.n_observers.fetch_add(1, Ordering::Relaxed);
        };
        head
    }

    pub unsafe fn remove(&self, node: *const Node) {
        let __lock__ = self.lock.write().unwrap();

        let next = (*node).next.load(Ordering::Relaxed);
        let prev = (*node).prev.load(Ordering::Relaxed);
        if !next.is_null() {
            (*next).prev.store(prev, Ordering::Relaxed);
        }
        if !prev.is_null() {
            (*prev).next.store(next, Ordering::Relaxed);
        } else {
            (self.head).store(ptr::null_mut(), Ordering::Relaxed);
        }
    }
}

pub struct ThreadPool {
    has_work: Arc<AtomicU32>,
    dropped: Arc<AtomicBool>,
    workers: Vec<Worker>,
    tasks: Arc<List>,
}

impl UnwindSafe for Worker {}
impl RefUnwindSafe for Worker {}

struct Unwind<T>(T);
impl<T> RefUnwindSafe for Unwind<T> {}
impl<T> UnwindSafe for Unwind<T> {}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.has_work.store(1, Ordering::Release);
        self.dropped.store(true, Ordering::Release);
        atomic_wait::wake_all(&*self.has_work);

        for worker in core::mem::take(&mut self.workers) {
            worker.handle.join().unwrap();
        }
    }
}

impl ThreadPool {
    pub(crate) fn new_imp(
        nthreads: usize,
        builder: &mut dyn FnMut(usize) -> Builder,
    ) -> Result<Self, std::io::Error> {
        let nthreads = nthreads;
        let dropped = Arc::new(AtomicBool::new(false));
        let tasks = Arc::new(List::new());
        let has_work = Arc::new(AtomicU32::new(0));

        let workers = (1..nthreads)
            .map(|tid| -> std::io::Result<Worker> {
                let tasks = tasks.clone();
                let dropped = dropped.clone();
                let has_work = has_work.clone();

                Ok(Worker {
                    handle: builder(tid).spawn(run(tid, dropped, tasks, has_work))?,
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(Self {
            has_work,
            workers,
            dropped,
            tasks,
        })
    }

    pub fn new(
        nthreads: usize,
        builder: impl FnMut(usize) -> Builder,
    ) -> Result<Self, std::io::Error> {
        _ = *crate::AUTOTUNE;
        Self::new_imp(nthreads, &mut { builder })
    }

    pub fn num_threads(&self) -> usize {
        self.workers.len() + 1
    }

    fn collect_imp<R: Send, F: Sync + Fn(usize) -> R>(&self, job_count: usize, f: F) -> Vec<R> {
        let mut ret = Vec::with_capacity(job_count);

        let started = (0..job_count.div_ceil(32))
            .map(|_| AtomicU32::new(0))
            .collect::<Vec<_>>();
        let tid = 0usize;

        let mut panic_storage = MaybeUninit::<Box<dyn Any + Send>>::uninit();

        unsafe {
            struct Job<F> {
                f: F,
                ret: *mut (),
                panic: AtomicPtr<()>,
            }

            let job = Job {
                f: Unwind(f),
                ret: ret.as_mut_ptr() as *mut (),
                panic: AtomicPtr::new((&mut panic_storage) as *mut _ as *mut ()),
            };

            let node = &Node {
                task: Task {
                    job_fn: |job, tid| {
                        let job = &*(job as *const Job<Unwind<F>>);

                        match std::panic::catch_unwind(|| {
                            let ret = ((*job).f.0)(tid);
                            ((*job).ret as *mut R).add(tid).write(ret);
                        }) {
                            Ok(()) => {}
                            Err(load) => {
                                let panic_slot =
                                    (*job).panic.swap(ptr::null_mut(), Ordering::Relaxed);
                                if !panic_slot.is_null() {
                                    (panic_slot as *mut Box<dyn Any + Send>).write(load);
                                }
                            }
                        }
                    },
                    job_data: (&raw const job) as *const (),
                    job_count,
                    n_done: AtomicUsize::new(0).into(),
                    n_observers: AtomicUsize::new(0).into(),
                    done: AtomicU32::new(0),

                    started: started.as_ptr(),
                },
                next: AtomicPtr::new(ptr::null_mut()),
                prev: AtomicPtr::new(ptr::null_mut()),
            };

            self.tasks.push(node);
            atomic_wait::wake_all(&*self.has_work);

            let task = &(*node).task;
            let f = task.job_fn;
            let data = task.job_data;
            let n_done = &task.n_done;
            let done_ptr = &raw const task.done;
            let started = task.started;

            main_loop(
                started,
                job_count,
                tid % job_count,
                f,
                data,
                n_done,
                done_ptr,
            );

            self.tasks.remove(node);

            let backoff = Backoff::new(10, 20);
            while node.task.n_observers.load(Ordering::Acquire) > 0 {
                if backoff.is_completed() {
                    atomic_wait::wait(&node.task.done, 0);
                } else {
                    backoff.snooze();
                }
            }

            if job.panic.load(Ordering::Relaxed).is_null() {
                std::panic::resume_unwind(panic_storage.assume_init());
            }
        }
        unsafe { ret.set_len(job_count) };

        ret
    }

    pub fn collect<R: Send>(&self, job_count: usize, f: impl Sync + Fn(usize) -> R) -> Vec<R> {
        self.collect_imp(job_count, f)
    }
}

unsafe fn main_loop(
    started: *const AtomicU32,
    job_count: usize,
    mut idx: usize,
    f: fn(*const (), usize),
    data: *const (),
    n_done: &CachePadded<AtomicUsize>,
    done_ptr: *const AtomicU32,
) {
    'main: loop {
        if (*done_ptr).load(Ordering::Acquire) != 0 {
            break 'main;
        }
        let started = core::slice::from_raw_parts(started, job_count.div_ceil(32));

        let pos = idx / 32;
        let shift = idx % 32;
        if (started[pos].fetch_or(1 << shift, Ordering::AcqRel) >> shift) & 1 == 1 {
            let div = job_count / 32;
            let rem = job_count % 32;
            for (i, started) in started[..div].iter().enumerate() {
                let started = started.load(Ordering::Relaxed);
                if started != u32::MAX {
                    idx = i * 32 + started.trailing_ones() as usize;
                    continue 'main;
                }
            }
            if rem != 0 {
                let started = started[div].load(Ordering::Relaxed);
                if started != (1 << rem) - 1 {
                    idx = div * 32 + started.trailing_ones() as usize;
                    continue 'main;
                }
            }
            break 'main;
        } else {
            f(data, idx);
            let n_done = n_done.fetch_add(1, Ordering::Release);

            if n_done + 1 == job_count {
                (*done_ptr).store(1, Ordering::Release);
                break 'main;
            }
        }
    }
}

fn run(
    tid: usize,
    dropped: Arc<AtomicBool>,
    tasks: Arc<List>,
    has_work: Arc<AtomicU32>,
) -> impl FnOnce() {
    move || {
        let backoff = crossbeam::utils::Backoff::new();
        loop {
            if dropped.load(Ordering::Acquire) {
                break;
            }

            unsafe {
                let node = tasks.peek();

                if !node.is_null() {
                    let task = &(*node).task;
                    let f = task.job_fn;
                    let data = task.job_data;
                    let job_count = task.job_count;
                    let n_done = &task.n_done;
                    let done_ptr = &raw const task.done;
                    let started = task.started;

                    main_loop(
                        started,
                        job_count,
                        tid % job_count,
                        f,
                        data,
                        n_done,
                        done_ptr,
                    );

                    if task.n_observers.fetch_sub(1, Ordering::Release) - 1 == 0 {
                        atomic_wait::wake_one(done_ptr);
                    }

                    backoff.reset();
                } else {
                    if backoff.is_completed() {
                        atomic_wait::wait(&*has_work, 0);
                    } else {
                        backoff.snooze();
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::UnsafeCell;

    #[derive(Copy, Clone)]
    struct AssumeMtSafe<T>(T);
    unsafe impl<T> Sync for AssumeMtSafe<T> {}
    unsafe impl<T> Send for AssumeMtSafe<T> {}

    #[test]
    fn test_pool2() {
        let nthreads = 2;
        let n = 20;
        let x = &mut *vec![1.0; n];

        let pool = ThreadPool::new(nthreads, |_| Builder::new()).unwrap();

        x.fill(1.0);
        let x = &*UnsafeCell::from_mut(x);
        let x = AssumeMtSafe(unsafe { &*(x as *const _ as *const [UnsafeCell<f64>]) });

        for i in 0..n {
            pool.collect(nthreads, |tid| unsafe {
                let x = { x }.0;

                let head = *x[i].get();
                let x = &x[i + 1..];
                let mine = crate::iter::partition(x, nthreads).nth(tid).unwrap();
                let mine = &mut *UnsafeCell::raw_get(mine as *const _ as *const UnsafeCell<[f64]>);

                for x in mine.iter_mut() {
                    *x += head;
                }
            });
        }
    }
}
