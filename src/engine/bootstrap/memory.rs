use super::*;

/// Conservative admission ledger for state materialized before live engine accounting exists.
///
/// Startup discovery and recovery are deliberately single-threaded. A plain value therefore
/// suffices, and keeping the ledger monotonic makes every error's `required` value an exact next
/// threshold: a budget of `required` admits the operation that a budget of `required - 1` rejects.
#[derive(Debug, Clone, Copy)]
pub(super) struct StartupMemoryAdmission {
    budget_bytes: usize,
    reserved_bytes: usize,
}

impl StartupMemoryAdmission {
    pub(super) fn new(budget_bytes: usize) -> Self {
        Self {
            budget_bytes,
            reserved_bytes: 0,
        }
    }

    pub(super) fn admit(&mut self, additional_bytes: usize) -> Result<()> {
        let required = self.reserved_bytes.saturating_add(additional_bytes);
        if self.budget_bytes != usize::MAX && required > self.budget_bytes {
            return Err(TsinkError::MemoryBudgetExceeded {
                budget: self.budget_bytes,
                required,
            });
        }
        self.reserved_bytes = required;
        Ok(())
    }

    pub(super) fn budget_bytes(self) -> usize {
        self.budget_bytes
    }

    #[cfg(test)]
    pub(super) fn reserved_bytes(self) -> usize {
        self.reserved_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_admission_accepts_exact_n_and_rejects_n_plus_one() {
        let mut exact = StartupMemoryAdmission::new(17);
        exact.admit(7).unwrap();
        exact.admit(10).unwrap();
        assert_eq!(exact.reserved_bytes(), 17);
        assert!(matches!(
            exact.admit(1),
            Err(TsinkError::MemoryBudgetExceeded {
                budget: 17,
                required: 18
            })
        ));
        assert_eq!(exact.reserved_bytes(), 17);

        let mut too_small = StartupMemoryAdmission::new(16);
        too_small.admit(7).unwrap();
        assert!(matches!(
            too_small.admit(10),
            Err(TsinkError::MemoryBudgetExceeded {
                budget: 16,
                required: 17
            })
        ));
        assert_eq!(too_small.reserved_bytes(), 7);
    }

    #[test]
    fn startup_admission_reports_overflow_as_an_unrepresentable_requirement() {
        let mut admission = StartupMemoryAdmission::new(usize::MAX - 1);
        admission.admit(usize::MAX - 2).unwrap();
        assert!(matches!(
            admission.admit(8),
            Err(TsinkError::MemoryBudgetExceeded {
                budget,
                required
            }) if budget == usize::MAX - 1 && required == usize::MAX
        ));
    }
}
