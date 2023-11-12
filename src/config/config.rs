use std::fs::File;
use std::io::{ErrorKind, Read, Seek, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::{Acquire, SeqCst};
use std::sync::Arc;
use std::{fs, io};

use log::{error, warn};

use crate::config::const_config::ConstConfig;
use crate::config::running_config::RunningConfig;
use crate::config::storage_parameters::StorageParameters;
use crate::config::{ConfigBuilder, DefaultConfig, DefaultSegment, Inner};
use crate::debug_delay;
use crate::ebr::{Owned, Shared};
use crate::pagecache::iobuf::{AlignedBuf, AlignedSegment};
use crate::pagecache::{arr_to_u32, u32_to_arr, Heap};
use crate::Mode;
use crate::{crc32, maybe_fsync_directory, pin, sys_limits, Db, Error};

macro_rules! supported {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            return Err(Error::Unsupported($msg));
        }
    };
}

/// Top-level configuration for the system.
///
/// # Examples
///
/// ```
/// let _config = sled::Config::default()
///     .path("/path/to/data".to_owned())
///     .cache_capacity(10_000_000_000)
///     .flush_every_ms(Some(1000));
/// ```
#[derive(Debug, Default)]
pub struct Config<C: ConstConfig = DefaultConfig>(Arc<C>);

impl<S: AlignedSegment> From<ConfigBuilder<S>> for Config<Inner<S>> {
    fn from(mut value: ConfigBuilder<S>) -> Self {
        value.limit_cache_max_memory();
        Self(Arc::new(value.into()))
    }
}

impl<C: ConstConfig> Clone for Config<C> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<C: ConstConfig> Deref for Config<C> {
    type Target = C;

    fn deref(&self) -> &C {
        &self.0
    }
}

impl<C: ConstConfig> Config<C> {
    /// Returns a default `Config`
    pub fn new() -> Config<C> {
        Config::default()
    }

    pub fn builder() -> ConfigBuilder<DefaultSegment> {
        ConfigBuilder::default()
    }

    /// Opens a `Db` based on the provided config.
    pub fn open(&self) -> crate::Result<Db<C>> {
        // only validate, setup directory, and open file once
        self.validate()?;

        let mut config = self.clone();

        let file = config.open_file()?;

        let heap_path = config.get_path().join("heap");
        let heap = Heap::start(&heap_path)?;
        maybe_fsync_directory(heap_path)?;

        // seal config in a Config
        let config = RunningConfig {
            inner: config,
            file: Arc::new(file),
            heap: Arc::new(heap),
        };

        Db::start_inner(config)
    }

    // panics if config options are outside of advised range
    fn validate(&self) -> crate::Result<()> {
        supported!(
            C::Segment::SIZE.count_ones() == 1,
            "segment_size should be a power of 2"
        );
        supported!(
            C::Segment::SIZE >= 256,
            "segment_size should be hundreds of kb at minimum, and we won't start if below 256"
        );
        supported!(
            C::Segment::SIZE <= 1 << 24,
            "segment_size should be <= 16mb"
        );
        if self.use_compression {
            supported!(
                !cfg!(feature = "no_zstd"),
                "the 'no_zstd' feature is set, but Config.use_compression is also set to true"
            );
        }
        supported!(
            self.compression_factor >= 1,
            "compression_factor must be >= 1"
        );
        supported!(
            self.compression_factor <= 22,
            "compression_factor must be <= 22"
        );
        supported!(
            self.idgen_persist_interval > 0,
            "idgen_persist_interval must be above 0"
        );
        Ok(())
    }

    fn open_file(&self) -> crate::Result<File> {
        let heap_dir: PathBuf = self.get_path().join("heap");

        if !heap_dir.exists() {
            fs::create_dir_all(heap_dir)?;
        }

        self.verify_config()?;

        // open the data file
        let mut options = fs::OpenOptions::new();

        let _ = options.create(true);
        let _ = options.read(true);
        let _ = options.write(true);

        if self.create_new {
            options.create_new(true);
        }

        let _ = std::fs::File::create(
            self.get_path().join("DO_NOT_USE_THIS_DIRECTORY_FOR_ANYTHING"),
        );

        let file = self.try_lock(options.open(&self.db_path())?)?;
        maybe_fsync_directory(self.get_path())?;
        Ok(file)
    }

    fn try_lock(&self, file: File) -> crate::Result<File> {
        #[cfg(all(
            not(miri),
            any(windows, target_os = "linux", target_os = "macos")
        ))]
        {
            use fs2::FileExt;

            let try_lock = if cfg!(any(
                feature = "for-internal-testing-only",
                feature = "light_testing"
            )) {
                // we block here because during testing
                // there are many filesystem race condition
                // that happen, causing locks to be held
                // for long periods of time, so we should
                // block to wait on reopening files.
                file.lock_exclusive()
            } else {
                file.try_lock_exclusive()
            };

            if try_lock.is_err() {
                return Err(Error::Io(
                    ErrorKind::Other,
                    "could not acquire database file lock",
                ));
            }
        }

        Ok(file)
    }

    fn verify_config(&self) -> crate::Result<()> {
        match self.read_config() {
            Ok(Some(old)) => {
                if self.use_compression {
                    supported!(
                        old.use_compression,
                        "cannot change compression configuration across restarts. \
                        this database was created without compression enabled."
                    );
                } else {
                    supported!(
                        !old.use_compression,
                        "cannot change compression configuration across restarts. \
                        this database was created with compression enabled."
                    );
                }

                supported!(
                    C::Segment::SIZE == old.segment_size,
                    "cannot change the io buffer size across restarts."
                );

                if self.version != old.version {
                    error!(
                        "This database was created using \
                         pagecache version {}.{}, but our pagecache \
                         version is {}.{}. Please perform an upgrade \
                         using the sled::Db::export and sled::Db::import \
                         methods.",
                        old.version.0,
                        old.version.1,
                        self.version.0,
                        self.version.1,
                    );
                    supported!(
                        self.version == old.version,
                        "The stored database must use a compatible sled version.
                        See error log for more details."
                    );
                }
                Ok(())
            }
            Ok(None) => self.write_config(),
            Err(e) => Err(e),
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let persisted_config = StorageParameters {
            version: self.version,
            segment_size: C::Segment::SIZE,
            use_compression: self.use_compression,
        };

        persisted_config.serialize()
    }

    fn write_config(&self) -> crate::Result<()> {
        let bytes = self.serialize();
        let crc: u32 = crc32(&*bytes);
        let crc_arr = u32_to_arr(crc);

        let temp_path = self.get_path().join("conf.tmp");
        let final_path = self.config_path();

        let mut f =
            fs::OpenOptions::new().write(true).create(true).open(&temp_path)?;

        io_fail!(self, "write_config bytes");
        f.write_all(&*bytes)?;
        io_fail!(self, "write_config crc");
        f.write_all(&crc_arr)?;
        io_fail!(self, "write_config fsync");
        f.sync_all()?;
        io_fail!(self, "write_config rename");
        fs::rename(temp_path, final_path)?;
        io_fail!(self, "write_config dir fsync");
        maybe_fsync_directory(self.get_path())?;
        io_fail!(self, "write_config post");
        Ok(())
    }

    fn read_config(&self) -> crate::Result<Option<StorageParameters>> {
        let path = self.config_path();

        let f_res = fs::OpenOptions::new().read(true).open(&path);

        let mut f = match f_res {
            Err(ref e) if e.kind() == ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(other) => {
                return Err(other.into());
            }
            Ok(f) => f,
        };

        if f.metadata()?.len() <= 8 {
            warn!("empty/corrupt configuration file found");
            return Ok(None);
        }

        let mut buf = vec![];
        let _ = f.read_to_end(&mut buf)?;
        let len = buf.len();
        let _ = buf.split_off(len - 4);

        let mut crc_arr = [0_u8; 4];
        let _ = f.seek(io::SeekFrom::End(-4))?;
        f.read_exact(&mut crc_arr)?;
        let crc_expected = arr_to_u32(&crc_arr);

        let crc_actual = crc32(&*buf);

        if crc_expected != crc_actual {
            warn!(
                "crc for settings file {:?} failed! \
                 can't verify that config is safe",
                path
            );
        }

        StorageParameters::deserialize(&buf).map(Some)
    }

    #[cfg(feature = "failpoints")]
    #[cfg(feature = "event_log")]
    #[doc(hidden)]
    // truncate the underlying file for corruption testing purposes.
    pub fn truncate_corrupt(&self, new_len: u64) {
        self.event_log.reset();
        let path = self.db_path();
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.set_len(new_len).expect("should be able to truncate");
    }

    /// Return the global error if one was encountered during
    /// an asynchronous IO operation.
    #[doc(hidden)]
    pub fn global_error(&self) -> crate::Result<()> {
        let guard = pin();
        let ge = self.global_error.load(Acquire, &guard);
        if ge.is_null() {
            Ok(())
        } else {
            #[allow(unsafe_code)]
            unsafe {
                Err(*ge.deref())
            }
        }
    }

    pub fn set_global_error(&self, error_value: Error) {
        let guard = pin();
        let error = Owned::new(error_value);

        let expected_old = Shared::null();

        let _ = self.global_error.compare_and_set(
            expected_old,
            error,
            SeqCst,
            &guard,
        );
    }
    pub fn reset_global_error(&self) {
        let guard = pin();
        let old = self.global_error.swap(Shared::default(), SeqCst, &guard);
        if !old.is_null() {
            #[allow(unsafe_code)]
            unsafe {
                guard.defer_destroy(old);
            }
        }
    }
}
