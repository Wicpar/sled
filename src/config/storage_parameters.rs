use std::collections::HashMap as Map;
use std::io::Write;
use std::io::{BufRead, BufReader};

use log::error;

use crate::Error;

/// A persisted configuration about high-level
/// storage file information
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct StorageParameters {
    pub segment_size: usize,
    pub use_compression: bool,
    pub version: (usize, usize),
}

impl StorageParameters {
    pub fn serialize(&self) -> Vec<u8> {
        let mut out = vec![];

        writeln!(&mut out, "segment_size: {}", self.segment_size).unwrap();
        writeln!(&mut out, "use_compression: {}", self.use_compression)
            .unwrap();
        writeln!(&mut out, "version: {}.{}", self.version.0, self.version.1)
            .unwrap();

        out
    }

    pub fn deserialize(bytes: &[u8]) -> crate::Result<StorageParameters> {
        let reader = BufReader::new(bytes);

        let mut lines = Map::new();

        for line in reader.lines() {
            let line = if let Ok(l) = line {
                l
            } else {
                error!(
                    "failed to parse persisted config as UTF-8. \
                     This changed in sled version 0.29"
                );
                return Err(Error::Unsupported(
                    "failed to open database that may \
                     have been created using a sled version \
                     earlier than 0.29",
                ));
            };
            let mut split = line.split(": ").map(String::from);
            let k = if let Some(k) = split.next() {
                k
            } else {
                error!("failed to parse persisted config line: {}", line);
                return Err(Error::corruption(None));
            };
            let v = if let Some(v) = split.next() {
                v
            } else {
                error!("failed to parse persisted config line: {}", line);
                return Err(Error::corruption(None));
            };
            lines.insert(k, v);
        }

        let segment_size: usize = if let Some(raw) = lines.get("segment_size") {
            if let Ok(parsed) = raw.parse() {
                parsed
            } else {
                error!("failed to parse segment_size value: {}", raw);
                return Err(Error::corruption(None));
            }
        } else {
            error!(
                "failed to retrieve required configuration parameter: segment_size"
            );
            return Err(Error::corruption(None));
        };

        let use_compression: bool = if let Some(raw) =
            lines.get("use_compression")
        {
            if let Ok(parsed) = raw.parse() {
                parsed
            } else {
                error!("failed to parse use_compression value: {}", raw);
                return Err(Error::corruption(None));
            }
        } else {
            error!(
                "failed to retrieve required configuration parameter: use_compression"
            );
            return Err(Error::corruption(None));
        };

        let version: (usize, usize) = if let Some(raw) = lines.get("version") {
            let mut split = raw.split('.');
            let major = if let Some(raw_major) = split.next() {
                if let Ok(parsed_major) = raw_major.parse() {
                    parsed_major
                } else {
                    error!(
                        "failed to parse major version value from line: {}",
                        raw
                    );
                    return Err(Error::corruption(None));
                }
            } else {
                error!("failed to parse major version value: {}", raw);
                return Err(Error::corruption(None));
            };

            let minor = if let Some(raw_minor) = split.next() {
                if let Ok(parsed_minor) = raw_minor.parse() {
                    parsed_minor
                } else {
                    error!(
                        "failed to parse minor version value from line: {}",
                        raw
                    );
                    return Err(Error::corruption(None));
                }
            } else {
                error!("failed to parse minor version value: {}", raw);
                return Err(Error::corruption(None));
            };

            (major, minor)
        } else {
            error!(
                "failed to retrieve required configuration parameter: version"
            );
            return Err(Error::corruption(None));
        };

        Ok(StorageParameters { segment_size, use_compression, version })
    }
}
