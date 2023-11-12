use log::error;
use std::convert::TryFrom;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::config::{gen_temp_path, Inner, DEFAULT_PATH};
use crate::ebr::Atomic;
#[cfg(feature = "event_log")]
use crate::event_log::EventLog;
use crate::pagecache::iobuf::{AlignedBuf, AlignedSegment};
use crate::{config, sys_limits, Config, Db, Error, Mode};

#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct ConfigBuilder<SEG: AlignedSegment> {
    #[doc(hidden)]
    pub cache_capacity: usize,
    #[doc(hidden)]
    pub flush_every_ms: Option<u64>,
    #[doc(hidden)]
    pub segment: PhantomData<SEG>,
    #[doc(hidden)]
    pub path: PathBuf,
    #[doc(hidden)]
    pub create_new: bool,
    #[doc(hidden)]
    pub mode: Mode,
    #[doc(hidden)]
    pub temporary: bool,
    #[doc(hidden)]
    pub use_compression: bool,
    #[doc(hidden)]
    pub compression_factor: i32,
    #[doc(hidden)]
    pub idgen_persist_interval: u64,
    #[doc(hidden)]
    pub snapshot_after_ops: u64,
    #[doc(hidden)]
    pub version: (usize, usize),
    tmp_path: PathBuf,
    pub(crate) global_error: Arc<Atomic<Error>>,
    #[cfg(feature = "event_log")]
    /// an event log for concurrent debugging
    pub event_log: Arc<EventLog>,
}

impl<S: AlignedSegment> Default for ConfigBuilder<S> {
    fn default() -> Self {
        Self {
            // generally useful
            path: PathBuf::from(DEFAULT_PATH),
            tmp_path: gen_temp_path(),
            create_new: false,
            cache_capacity: 1024 * 1024 * 1024, // 1gb
            mode: Mode::LowSpace,
            use_compression: false,
            compression_factor: 5,
            temporary: false,
            version: config::crate_version(),

            // useful in testing
            segment: Default::default(), // 512kb in bytes
            flush_every_ms: Some(500),
            idgen_persist_interval: 1_000_000,
            snapshot_after_ops: if cfg!(feature = "for-internal-testing-only") {
                10
            } else {
                1_000_000
            },
            global_error: Arc::new(Atomic::default()),
            #[cfg(feature = "event_log")]
            event_log: Arc::new(crate::event_log::EventLog::default()),
        }
    }
}

macro_rules! builder {
    ($(($name:ident, $t:ty, $desc:expr)),*) => {
        $(
            #[doc=$desc]
            pub fn $name(mut self, to: $t) -> Self {
                self.$name = to;
                self
            }
        )*
    }
}
impl<S: AlignedSegment> ConfigBuilder<S> {
    pub fn build(self) -> Config<Inner<S>> {
        self.into()
    }

    /// Opens a `Db` based on the provided config.
    pub fn open(self) -> crate::Result<Db<Inner<S>>> {
        self.build().open()
    }

    // Get the path of the database
    #[doc(hidden)]
    #[inline]
    pub fn get_path(&self) -> PathBuf {
        if self.temporary && self.path == PathBuf::from(DEFAULT_PATH) {
            self.tmp_path.clone()
        } else {
            self.path.clone()
        }
    }

    pub fn path<P: AsRef<Path>>(mut self, path: P) -> ConfigBuilder<S> {
        self.path = path.as_ref().to_path_buf();
        self
    }

    #[inline]
    pub fn db_path(&self) -> PathBuf {
        self.get_path().join("db")
    }

    #[inline]
    pub fn config_path(&self) -> PathBuf {
        self.get_path().join("conf")
    }

    #[inline]
    pub(crate) fn normalize<T>(&self, value: T) -> T
    where
        T: Copy
            + TryFrom<usize>
            + std::ops::Div<Output = T>
            + std::ops::Mul<Output = T>,
        <T as TryFrom<usize>>::Error: Debug,
    {
        let segment_size: T = T::try_from(S::SIZE).unwrap();
        value / segment_size * segment_size
    }

    #[doc(hidden)]
    #[inline]
    pub fn segment_size<const SIZE: usize>(
        self,
    ) -> ConfigBuilder<AlignedBuf<SIZE>> {
        let Self {
            cache_capacity,
            flush_every_ms,
            path,
            create_new,
            mode,
            temporary,
            use_compression,
            compression_factor,
            idgen_persist_interval,
            snapshot_after_ops,
            version,
            tmp_path,
            global_error,
            ..
        } = self;
        ConfigBuilder {
            cache_capacity,
            flush_every_ms,
            segment: Default::default(),
            path,
            create_new,
            mode,
            temporary,
            use_compression,
            compression_factor,
            idgen_persist_interval,
            snapshot_after_ops,
            version,
            tmp_path,
            global_error,
            #[cfg(feature = "event_log")]
            event_log: self.event_log,
        }
    }

    #[doc(hidden)]
    pub fn flush_every_ms(mut self, every_ms: Option<u64>) -> Self {
        self.flush_every_ms = every_ms;
        self
    }

    #[doc(hidden)]
    pub fn idgen_persist_interval(mut self, interval: u64) -> Self {
        self.idgen_persist_interval = interval;
        self
    }

    pub fn limit_cache_max_memory(&mut self) {
        if let Some(limit) = sys_limits::get_memory_limit() {
            if self.cache_capacity > limit {
                self.cache_capacity = limit;
                error!(
                    "cache capacity is limited to the cgroup memory \
                 limit: {} bytes",
                    self.cache_capacity
                );
            }
        }
    }

    builder!(
        (
            cache_capacity,
            usize,
            "maximum size in bytes for the system page cache"
        ),
        (
            mode,
            Mode,
            "specify whether the system should run in \"small\" or \"fast\" mode"
        ),
        (use_compression, bool, "whether to use zstd compression"),
        (
            compression_factor,
            i32,
            "the compression factor to use with zstd compression. Ranges from 1 up to 22. Levels >= 20 are 'ultra'."
        ),
        (
            temporary,
            bool,
            "deletes the database after drop. if no path is set, uses /dev/shm on linux"
        ),
        (
            create_new,
            bool,
            "attempts to exclusively open the database, failing if it already exists"
        ),
        (
            snapshot_after_ops,
            u64,
            "take a fuzzy snapshot of pagecache metadata after this many ops"
        )
    );
}
