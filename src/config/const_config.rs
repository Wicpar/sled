use crate::config::builder::ConfigBuilder;
use crate::pagecache::iobuf::AlignedSegment;
use std::fmt::Debug;
use std::ops::DerefMut;

pub trait ConstConfig:
    DerefMut<Target = ConfigBuilder<Self::Segment>>
    + Clone
    + Default
    + Debug
    + Send
    + Sync
    + 'static
{
    type Segment: AlignedSegment;
}
