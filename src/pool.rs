// use atomic_wait::wait;
use std::{
    any::Any,
    cell::UnsafeCell,
    mem::MaybeUninit,
    panic::{RefUnwindSafe, UnwindSafe},
    ptr::null_mut,
    sync::{
        atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
        Arc,
    },
    thread::{Builder, JoinHandle, Thread},
};

use crate::backoff::Backoff;
use crossbeam::utils::CachePadded;

struct FnDataCommon {
    f: *const (),
    jobs_left: CachePadded<AtomicUsize>,
    panic_slot: AtomicPtr<()>,
}
struct FnDataPerWorkerInner {
    subgroup_size: usize,
    jobs_left_in_subgroup: *const AtomicUsize,
    parent: Option<Thread>,
    return_slot: *mut (),
    tid: usize,
    batch: *mut [Worker],
}

struct FnDataPerWorker(UnsafeCell<FnDataPerWorkerInner>);

struct AssumeMtSafe<T>(T);

unsafe impl Sync for FnDataCommon {}
unsafe impl Send for FnDataCommon {}

unsafe impl<T> Sync for AssumeMtSafe<T> {}
unsafe impl<T> Send for AssumeMtSafe<T> {}

unsafe impl Sync for FnDataPerWorker {}
unsafe impl Send for FnDataPerWorker {}

impl UnwindSafe for Worker {}
impl RefUnwindSafe for Worker {}

struct Unwind<T>(T);
impl<T> RefUnwindSafe for Unwind<T> {}
impl<T> UnwindSafe for Unwind<T> {}

type FnType = unsafe fn(data: &FnDataCommon, per_worker: &mut FnDataPerWorkerInner);

struct Worker {
    job_fn: Arc<AtomicPtr<()>>,
    job_data: Arc<AtomicPtr<FnDataCommon>>,
    worker_data: Arc<FnDataPerWorker>,
    handle: JoinHandle<()>,
}

pub struct ThreadPool {
    dropped: Arc<AtomicBool>,
    workers: Vec<Worker>,
}

#[repr(transparent)]
pub struct ThreadGroup {
    workers: [Worker],
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
        for worker in core::mem::take(&mut self.workers) {
            worker.handle.thread().unpark();
            worker.handle.join().unwrap();
        }
    }
}

impl ThreadPool {
    fn new_imp(
        nthreads: usize,
        builder: &mut dyn FnMut(usize) -> Builder,
    ) -> Result<Self, std::io::Error> {
        let nthreads = nthreads;
        let dropped = Arc::new(AtomicBool::new(false));

        let workers = (1..nthreads)
            .map(|tid| -> std::io::Result<Worker> {
                let dropped = dropped.clone();
                let job_fn = Arc::new(AtomicPtr::new(null_mut()));
                let job_data = Arc::new(AtomicPtr::new(null_mut()));
                let worker_data =
                    Arc::new(FnDataPerWorker(UnsafeCell::new(FnDataPerWorkerInner {
                        subgroup_size: 0,
                        return_slot: null_mut(),
                        parent: None,
                        tid,
                        batch: &mut [],
                        jobs_left_in_subgroup: null_mut(),
                    })));

                Ok(Worker {
                    job_fn: job_fn.clone(),
                    job_data: job_data.clone(),
                    worker_data: worker_data.clone(),
                    handle: builder(tid).spawn(move || {
                        let backoff = Backoff::new(4);
                        loop {
                            if dropped.load(Ordering::Relaxed) {
                                break;
                            }
                            let f = job_fn.load(Ordering::Acquire);

                            unsafe {
                                if !f.is_null() {
                                    let worker_data = &mut *worker_data.0.get();
                                    let data = &*job_data.load(Ordering::Relaxed);

                                    let f = core::mem::transmute::<*mut (), FnType>(f);
                                    job_fn.store(null_mut(), Ordering::Relaxed);

                                    f(data, worker_data);

                                    let jobs_left_in_subgroup = (*worker_data
                                        .jobs_left_in_subgroup)
                                        .fetch_sub(1, Ordering::AcqRel)
                                        - 1;

                                    if jobs_left_in_subgroup == 0 {
                                        let jobs_left = data.jobs_left.fetch_sub(
                                            worker_data.subgroup_size,
                                            Ordering::Release,
                                        ) - worker_data.subgroup_size;

                                        if jobs_left == 0 {
                                            worker_data.parent.as_ref().unwrap().unpark();
                                        }
                                    }

                                    backoff.reset();
                                } else {
                                    if backoff.is_completed() {
                                        std::thread::park();
                                    } else {
                                        backoff.snooze();
                                    }
                                }
                            }
                        }
                    })?,
                })
            })
            .collect::<Result<_, _>>()?;

        Ok(Self { workers, dropped })
    }

    pub fn new(
        nthreads: usize,
        builder: impl FnMut(usize) -> Builder,
    ) -> Result<Self, std::io::Error> {
        Self::new_imp(nthreads, &mut { builder })
    }

    pub fn num_threads(&self) -> usize {
        1 + self.workers.len()
    }

    pub fn all(&mut self) -> &mut ThreadGroup {
        ThreadGroup::from_mut(&mut self.workers)
    }
}

impl ThreadGroup {
    fn from_mut(workers: &mut [Worker]) -> &mut Self {
        unsafe { &mut *(workers as *mut _ as *mut ThreadGroup) }
    }

    pub fn num_threads(&self) -> usize {
        1 + self.workers.len()
    }

    fn fork_imp<R: Send, F: Sync + Fn(usize, &mut ThreadGroup) -> R>(
        &mut self,
        mut group_sizes: impl Clone + ExactSizeIterator<Item = usize>,
        f: &F,
    ) -> Vec<R> {
        let n_groups = group_sizes.len();
        assert!(n_groups > 0);
        for size in group_sizes.clone() {
            equator::assert!(size > 0);
        }
        equator::assert!(
            group_sizes.clone().map(|x| x as u128).sum::<u128>() <= self.num_threads() as u128
        );

        let mut v = Vec::with_capacity(n_groups);

        let subgroup_size = {
            let mut subgroup_size = 1;
            while subgroup_size * subgroup_size <= n_groups {
                subgroup_size *= 2;
            }
            subgroup_size / 2
        };
        let mut waits = (0..n_groups.div_ceil(subgroup_size))
            .map(|_| CachePadded::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();

        let mut count = 0;
        let mut idx = 0;
        while count < n_groups {
            waits[idx] =
                CachePadded::new(AtomicUsize::new(Ord::min(n_groups - count, subgroup_size)));

            count += subgroup_size;
            idx += 1;
        }

        let uninit = v.as_mut_ptr() as *mut MaybeUninit<R>;

        let workers = &mut self.workers;
        let (batch, mut workers) = workers.split_at_mut(group_sizes.next().unwrap() - 1);

        let mut panic_storage = MaybeUninit::<Box<dyn Any + Send>>::uninit();

        let data = FnDataCommon {
            f: f as *const F as *const (),
            jobs_left: CachePadded::new(AtomicUsize::new(n_groups)),
            panic_slot: AtomicPtr::new((&mut panic_storage) as *mut _ as *mut ()),
        };
        let parent = std::thread::current();

        unsafe {
            let mut subgroup = 0;

            for k in 1..n_groups {
                if k % subgroup_size == 0 {
                    subgroup += 1;
                }

                let (leader, workers_) = workers.split_first_mut().unwrap();
                let (batch, workers_) = workers_.split_at_mut(group_sizes.next().unwrap() - 1);

                leader
                    .job_data
                    .store((&data) as *const _ as *mut _, Ordering::Relaxed);
                let inner = &mut *leader.worker_data.0.get();
                inner.tid = k;
                inner.return_slot = uninit.add(k) as *mut ();
                inner.batch = batch;
                inner.subgroup_size = if subgroup + 1 == waits.len() {
                    if n_groups % subgroup_size == 0 {
                        subgroup_size
                    } else {
                        n_groups % subgroup_size
                    }
                } else {
                    subgroup_size
                };
                inner.jobs_left_in_subgroup = &*waits[subgroup];
                inner.parent = Some(parent.clone());

                let f = (|data: &FnDataCommon, per_worker: &mut FnDataPerWorkerInner| {
                    match std::panic::catch_unwind(|| {
                        (*(per_worker.return_slot as *mut MaybeUninit<R>)).write((*(data.f
                            as *const F))(
                            per_worker.tid,
                            ThreadGroup::from_mut(&mut *(per_worker.batch)),
                        ));
                    }) {
                        Ok(()) => (),
                        Err(panic_load) => {
                            let panic_slot = data.panic_slot.swap(null_mut(), Ordering::Relaxed);
                            if !panic_slot.is_null() {
                                (panic_slot as *mut Box<dyn Any + Send>).write(panic_load);
                            }
                        }
                    };
                }) as FnType as *mut ();

                // last step, as this doubles as a lock
                leader.job_fn.store(f, Ordering::Release);
                leader.handle.thread().unpark();
                workers = workers_;
            }

            let f = Unwind(f);
            let uninit = uninit as *mut ();
            let batch = batch as *mut _;

            match std::panic::catch_unwind(|| {
                (*(uninit as *mut MaybeUninit<R>))
                    .write(({ f }.0)(0, ThreadGroup::from_mut(&mut *batch)));
            }) {
                Ok(()) => (),
                Err(panic_load) => {
                    let panic_slot = data.panic_slot.swap(null_mut(), Ordering::Relaxed);
                    if !panic_slot.is_null() {
                        (panic_slot as *mut Box<dyn Any + Send>).write(panic_load);
                    }
                }
            };
            let jobs_left_in_subgroup = waits[0].fetch_sub(1, Ordering::AcqRel) - 1;
            if jobs_left_in_subgroup == 0 {
                data.jobs_left.fetch_sub(subgroup_size, Ordering::Release);
            }
        }

        let backoff = Backoff::new(4);
        loop {
            if data.jobs_left.load(Ordering::Acquire) == 0 {
                break;
            }

            if backoff.is_completed() {
                std::thread::park();
            } else {
                backoff.snooze();
            }
        }

        if data.panic_slot.load(Ordering::Relaxed).is_null() {
            std::panic::panic_any(unsafe { panic_storage.assume_init() });
        }

        unsafe { v.set_len(n_groups) };

        v
    }

    pub fn fork<R: Send>(
        &mut self,
        group_sizes: &[usize],
        op: impl Sync + Fn(usize, &mut ThreadGroup) -> R,
    ) -> Vec<R> {
        self.fork_imp(group_sizes.iter().cloned(), &op)
    }

    pub fn fork2<RA: Send, RB: Send>(
        &mut self,
        group_sizes: &[usize; 2],
        op_a: impl Sync + FnOnce(&mut ThreadGroup) -> RA,
        op_b: impl Sync + FnOnce(&mut ThreadGroup) -> RB,
    ) -> (RA, RB) {
        enum Either<T, U> {
            Left(T),
            Right(U),
        }

        let op_a = AssumeMtSafe(UnsafeCell::new(Some(op_a)));
        let op_b = AssumeMtSafe(UnsafeCell::new(Some(op_b)));

        let mut ret = self.fork_imp(group_sizes.iter().cloned(), &|tid, group| {
            if tid == 0 {
                let op_a = unsafe { (*{ &op_a }.0.get()).take().unwrap() };
                Either::Left(op_a(group))
            } else {
                let op_b = unsafe { (*{ &op_b }.0.get()).take().unwrap() };
                Either::Right(op_b(group))
            }
        });
        let Some(Either::Right(rb)) = ret.pop() else {
            panic!()
        };
        let Some(Either::Left(ra)) = ret.pop() else {
            panic!()
        };

        (ra, rb)
    }

    pub fn broadcast<R: Send>(&mut self, f: impl Sync + Fn(usize) -> R) -> Vec<R> {
        self.fork_imp(std::iter::repeat_n(1, self.num_threads()), &|tid, _| f(tid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{sync, AllocHint, BarrierInit};
    use std::thread::Builder;

    #[test]
    fn test_pool() {
        let nthreads = 12;
        let mut pool = ThreadPool::new(nthreads, |_| Builder::new()).unwrap();

        let n = 10;
        let x = &mut *vec![1.0; n];
        x.fill(1.0);
        let init = BarrierInit::new(
            &mut *x,
            nthreads,
            AllocHint::default(),
            sync::SpinLimit::best(nthreads),
        );

        pool.all().broadcast(|_| {
            let mut barrier = init.barrier_ref();

            for i in 0..n {
                let Ok((head, mine)) = sync!(barrier, |x| {
                    let (head, x) = x[i..].split_at_mut(1);
                    (head[0], crate::iter::partition_mut(x, nthreads))
                }) else {
                    return;
                };

                for x in mine.iter_mut() {
                    *x += head;
                }
            }
        });
    }

    #[test]
    #[should_panic]
    fn test_pool_panic() {
        let nthreads = 12;
        let mut pool = ThreadPool::new(nthreads, |_| Builder::new()).unwrap();

        pool.all().broadcast(|_| {
            panic!();
        });
    }
}
