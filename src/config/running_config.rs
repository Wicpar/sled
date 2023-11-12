use crate::config::ConstConfig;
use crate::pagecache::Heap;
use crate::Config;
use std::fs::File;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};

/// A Configuration that has an associated opened
/// file.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct RunningConfig<C: ConstConfig> {
    pub(crate) inner: Config<C>,
    pub(crate) file: Arc<File>,
    pub(crate) heap: Arc<Heap>,
}

impl<C: ConstConfig> Clone for RunningConfig<C> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            file: self.file.clone(),
            heap: self.heap.clone(),
        }
    }
}

impl<C: ConstConfig> Deref for RunningConfig<C> {
    type Target = Config<C>;

    fn deref(&self) -> &Config<C> {
        &self.inner
    }
}

#[cfg(all(not(miri), any(windows, target_os = "linux", target_os = "macos")))]
impl<C: ConstConfig> Drop for RunningConfig<C> {
    fn drop(&mut self) {
        use fs2::FileExt;
        if Arc::strong_count(&self.file) == 1 {
            let _ = self.file.unlock();
        }
    }
}

impl<C: ConstConfig> RunningConfig<C> {
    // returns the snapshot file paths for this system
    #[doc(hidden)]
    pub fn get_snapshot_files(&self) -> io::Result<Vec<PathBuf>> {
        let conf_path = self.get_path().join("snap.");

        let absolute_path: PathBuf = if Path::new(&conf_path).is_absolute() {
            conf_path
        } else {
            std::env::current_dir()?.join(conf_path)
        };

        let filter = |dir_entry: io::Result<fs::DirEntry>| {
            if let Ok(de) = dir_entry {
                let path_buf = de.path();
                let path = path_buf.as_path();
                let path_str = &*path.to_string_lossy();
                if path_str.starts_with(&*absolute_path.to_string_lossy())
                    && !path_str.ends_with(".generating")
                {
                    Some(path.to_path_buf())
                } else {
                    None
                }
            } else {
                None
            }
        };

        let snap_dir = Path::new(&absolute_path).parent().unwrap();

        if !snap_dir.exists() {
            fs::create_dir_all(snap_dir)?;
        }

        Ok(snap_dir.read_dir()?.filter_map(filter).collect())
    }
}
