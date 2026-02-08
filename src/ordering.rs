use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, PartialEq, Eq)]
pub enum ApplyDecision {
    ApplyInOrder,
    DropOld,
    GapDetected { expected: u64, got: u64 },
}

#[derive(Debug)]
pub struct OrderingGuard {
    last_applied: AtomicU64,
}

impl OrderingGuard {
    pub fn new() -> Self {
        Self {
            last_applied: AtomicU64::new(0),
        }
    }

    pub fn classify(&self, incoming: u64) -> ApplyDecision {
        loop {
            let last = self.last_applied.load(Ordering::SeqCst);

            if incoming <= last {
                return ApplyDecision::DropOld;
            }

            let expected = last + 1;

            if incoming != expected {
                return ApplyDecision::GapDetected {
                    expected,
                    got: incoming,
                };
            }

            match self.last_applied.compare_exchange(
                last,
                incoming,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return ApplyDecision::ApplyInOrder,
                Err(_) => continue,
            }
        }
    }

    pub fn last(&self) -> u64 {
        self.last_applied.load(Ordering::SeqCst)
    }

    pub fn reset(&self) {
        self.last_applied.store(0, Ordering::SeqCst);
    }

    /// Force the watermark to a specific value (used after recovery paths).
    pub fn set(&self, value: u64) {
        self.last_applied.store(value, Ordering::SeqCst);
    }
}

impl Default for OrderingGuard {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_versions_apply() {
        let guard = OrderingGuard::new();
        assert_eq!(guard.classify(1), ApplyDecision::ApplyInOrder);
        assert_eq!(guard.classify(2), ApplyDecision::ApplyInOrder);
        assert_eq!(guard.classify(3), ApplyDecision::ApplyInOrder);
        assert_eq!(guard.last(), 3);
    }

    #[test]
    fn old_version_is_dropped() {
        let guard = OrderingGuard::new();
        assert_eq!(guard.classify(1), ApplyDecision::ApplyInOrder);
        assert_eq!(guard.classify(1), ApplyDecision::DropOld);
        assert_eq!(guard.classify(0), ApplyDecision::DropOld);
    }

    #[test]
    fn gap_detected() {
        let guard = OrderingGuard::new();
        assert_eq!(guard.classify(1), ApplyDecision::ApplyInOrder);
        assert_eq!(
            guard.classify(5),
            ApplyDecision::GapDetected {
                expected: 2,
                got: 5
            }
        );
        assert_eq!(guard.last(), 1);
    }

    #[test]
    fn reset_allows_reuse() {
        let guard = OrderingGuard::new();
        assert_eq!(guard.classify(1), ApplyDecision::ApplyInOrder);
        guard.reset();
        assert_eq!(guard.last(), 0);
        assert_eq!(guard.classify(1), ApplyDecision::ApplyInOrder);
    }

    #[test]
    fn set_allows_advancing_after_gap() {
        let guard = OrderingGuard::new();
        assert_eq!(guard.classify(1), ApplyDecision::ApplyInOrder);
        // Simulate recovery that jumps watermark forward
        guard.set(5);
        assert_eq!(guard.last(), 5);
        // Next event should apply in order
        assert_eq!(guard.classify(6), ApplyDecision::ApplyInOrder);
    }
}
