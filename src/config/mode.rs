/// The high-level database mode, according to
/// the trade-offs of the RUM conjecture.
#[derive(Debug, Clone, Copy)]
pub enum Mode {
    /// In this mode, the database will make
    /// decisions that favor using less space
    /// instead of supporting the highest possible
    /// write throughput. This mode will also
    /// rewrite data more frequently as it
    /// strives to reduce fragmentation.
    LowSpace,
    /// In this mode, the database will try
    /// to maximize write throughput while
    /// potentially using more disk space.
    HighThroughput,
}
