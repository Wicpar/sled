use std::{io::BufRead, path::PathBuf};

pub use builder::*;
pub use config::*;
pub use const_config::*;
pub use inner::*;
pub use mode::*;
pub use running_config::*;
pub use storage_parameters::*;

use crate::pagecache::iobuf::AlignedBuf;

pub mod builder;
pub mod config;
pub mod const_config;
pub mod inner;
pub mod mode;
pub mod running_config;
pub mod storage_parameters;

const DEFAULT_PATH: &str = "default.sled";

const DEFAULT_SEGMENT_SIZE: usize = 512 * 1024;

pub type DefaultSegment = AlignedBuf<DEFAULT_SEGMENT_SIZE>;
pub type DefaultConfig = Inner<DefaultSegment>;

pub fn crate_version() -> (usize, usize) {
    let vsn = env!("CARGO_PKG_VERSION");
    let mut parts = vsn.split('.');
    let major = parts.next().unwrap().parse().unwrap();
    let minor = parts.next().unwrap().parse().unwrap();
    (major, minor)
}

pub fn gen_temp_path() -> PathBuf {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;
    use std::time::SystemTime;

    static SALT_COUNTER: AtomicUsize = AtomicUsize::new(0);

    let seed = SALT_COUNTER.fetch_add(1, SeqCst) as u128;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        << 48;

    #[cfg(not(miri))]
    let pid = u128::from(std::process::id());

    #[cfg(miri)]
    let pid = 0;

    let salt = (pid << 16) + now + seed;

    if cfg!(target_os = "linux") {
        // use shared memory for temporary linux files
        format!("/dev/shm/pagecache.tmp.{}", salt).into()
    } else {
        std::env::temp_dir().join(format!("pagecache.tmp.{}", salt))
    }
}
