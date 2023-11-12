use crate::config::builder::ConfigBuilder;
use crate::pagecache::iobuf::{AlignedBuf, AlignedSegment};
use crate::ConstConfig;
use log::debug;
use std::fs;
use std::ops::{Deref, DerefMut};

pub const DEFAULT_SEGMENT_SIZE: usize = 512 * 1024;

#[derive(Clone, Debug)]
pub struct Inner<S: AlignedSegment = AlignedBuf<DEFAULT_SEGMENT_SIZE>>(
    ConfigBuilder<S>,
);

impl<S: AlignedSegment> From<ConfigBuilder<S>> for Inner<S> {
    fn from(value: ConfigBuilder<S>) -> Self {
        Self(value)
    }
}

impl<S: AlignedSegment> Deref for Inner<S> {
    type Target = ConfigBuilder<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S: AlignedSegment> DerefMut for Inner<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<S: AlignedSegment> Drop for Inner<S> {
    fn drop(&mut self) {
        if self.temporary {
            // Our files are temporary, so nuke them.
            debug!("removing temporary storage file {:?}", self.get_path());
            let _res = fs::remove_dir_all(&self.get_path());
        }
    }
}

impl<S: AlignedSegment> Default for Inner<S> {
    fn default() -> Self {
        ConfigBuilder::default().into()
    }
}

impl<S: AlignedSegment> ConstConfig for Inner<S> {
    type Segment = S;
}
