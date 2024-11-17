use core::mem::take;

/// Iterator obtained from [`partition`].
pub struct Partition<'a, T> {
    slice: &'a [T],
    div: usize,
    rem: usize,
    count: usize,
}

/// Iterator obtained from [`partition_mut`].
pub struct PartitionMut<'a, T> {
    slice: &'a mut [T],
    div: usize,
    rem: usize,
    count: usize,
}

/// Returns an iterator producing `chunk_count` contiguous slices of `slice`.
#[inline]
pub fn partition<T>(slice: &[T], chunk_count: usize) -> Partition<'_, T> {
    let div = slice.len() / chunk_count;
    let rem = slice.len() % chunk_count;
    Partition {
        slice,
        div,
        rem,
        count: chunk_count,
    }
}

/// Returns an iterator producing `chunk_count` contiguous slices of `slice`.
#[inline]
pub fn partition_mut<T>(slice: &mut [T], chunk_count: usize) -> PartitionMut<'_, T> {
    let div = slice.len() / chunk_count;
    let rem = slice.len() % chunk_count;
    PartitionMut {
        slice,
        div,
        rem,
        count: chunk_count,
    }
}

impl<'a, T> Iterator for PartitionMut<'a, T> {
    type Item = &'a mut [T];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.count == 0 {
            None
        } else {
            let next;

            let bonus = (self.rem > 0) as usize;
            (next, self.slice) = take(&mut self.slice).split_at_mut(self.div + bonus);
            self.rem -= bonus;

            self.count -= 1;
            Some(next)
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.count, Some(self.count))
    }
}

impl<'a, T> Iterator for Partition<'a, T> {
    type Item = &'a [T];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.count == 0 {
            None
        } else {
            let next;

            let bonus = (self.rem > 0) as usize;
            (next, self.slice) = take(&mut self.slice).split_at(self.div + bonus);
            self.rem -= bonus;

            self.count -= 1;
            Some(next)
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.count, Some(self.count))
    }
}

impl<'a, T> DoubleEndedIterator for PartitionMut<'a, T> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.count == 0 {
            None
        } else {
            let len = self.slice.len();
            let next;

            let bonus = (self.count == self.rem) as usize;
            (self.slice, next) = take(&mut self.slice).split_at_mut(len - (self.div + bonus));

            self.count -= 1;
            Some(next)
        }
    }
}

impl<'a, T> DoubleEndedIterator for Partition<'a, T> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.count == 0 {
            None
        } else {
            let len = self.slice.len();
            let next;

            let bonus = (self.count == self.rem) as usize;
            (self.slice, next) = take(&mut self.slice).split_at(len - (self.div + bonus));

            self.count -= 1;
            Some(next)
        }
    }
}

impl<'a, T> ExactSizeIterator for PartitionMut<'a, T> {
    #[inline]
    fn len(&self) -> usize {
        self.count
    }
}

impl<'a, T> ExactSizeIterator for Partition<'a, T> {
    #[inline]
    fn len(&self) -> usize {
        self.count
    }
}
