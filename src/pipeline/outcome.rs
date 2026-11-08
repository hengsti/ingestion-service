/// Terminal disposition of one admitted pipeline job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PipelineOutcome {
    /// The configured output was appended to the WAL durably.
    OutputStored,
    /// The invalid input was durably stored in quarantine.
    RejectionStored,
    /// Processing or durable storage could not be completed.
    Deferred(DeferredReason),
}

impl PipelineOutcome {
    /// Returns whether this outcome permits acknowledging the source publication.
    #[must_use]
    pub const fn is_ack_eligible(self) -> bool {
        matches!(self, Self::OutputStored | Self::RejectionStored)
    }

    pub(crate) const fn metric_label(self) -> &'static str {
        match self {
            Self::OutputStored => "output_stored",
            Self::RejectionStored => "rejection_stored",
            Self::Deferred(_) => "deferred",
        }
    }
}

/// Bounded operational reason that prevented a terminal storage outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeferredReason {
    /// A pipeline stage failed for an unclassified reason.
    StageFailure,
    /// WAL admission, writing, or synchronization failed.
    PersistenceFailure,
    /// Durable rejection storage failed.
    QuarantineFailure,
    /// A stage stopped without recording a rejection or operational failure.
    UnexpectedStop,
    /// The pipeline completed without a durable persistence stage.
    MissingPersistence,
}
