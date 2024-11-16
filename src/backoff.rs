pub struct Backoff {
    spin_count: usize,
    spin_limit: usize,
}

impl Backoff {
    #[inline]
    pub fn new(spin_limit: usize) -> Self {
        Backoff {
            spin_count: 1,
            spin_limit,
        }
    }

    #[inline]
    pub fn spin(&mut self) {
        let spin = self.spin_count.min(self.spin_limit);
        for _ in 0..spin {
            std::hint::spin_loop();
        }

        if self.spin_count <= self.spin_limit {
            self.spin_count += spin;
        }
    }

    #[inline]
    pub fn is_completed(&self) -> bool {
        self.spin_count > self.spin_limit
    }
}
