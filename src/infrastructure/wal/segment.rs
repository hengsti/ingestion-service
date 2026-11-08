#[cfg(windows)]
use std::os::windows::fs::OpenOptionsExt;
use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

pub(crate) fn sync_directory(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(dir)?.sync_all()
    }

    #[cfg(windows)]
    {
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        fs::OpenOptions::new()
            .write(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(dir)?
            .sync_all()
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = dir;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "WAL directory synchronization is unsupported on this platform",
        ))
    }
}

pub(super) fn create_wal_directory(dir: &Path) -> Result<()> {
    let mut missing_parents = Vec::new();
    let mut current = dir;
    loop {
        match fs::metadata(current) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let parent = current
                    .parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                missing_parents.push(parent.to_path_buf());
                current = parent;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("checking WAL directory {}", current.display()));
            }
        }
    }

    fs::create_dir_all(dir).with_context(|| format!("creating WAL directory {}", dir.display()))?;
    if missing_parents.is_empty() {
        if let Some(parent) = dir.parent() {
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            sync_directory(parent)
                .with_context(|| format!("syncing WAL parent directory {}", parent.display()))?;
        }
    } else {
        for parent in missing_parents.iter().rev() {
            sync_directory(parent)
                .with_context(|| format!("syncing WAL parent directory {}", parent.display()))?;
        }
    }
    Ok(())
}

pub fn segment_filename(id: u64) -> String {
    format!("{id:020}.log")
}

pub fn parse_segment_id(name: &str) -> Option<u64> {
    if name.len() != 24 {
        return None;
    }

    let suffix: &str = ".log";
    name.strip_suffix(suffix)
        .and_then(|s| s.parse::<u64>().ok())
}

pub fn list_segments(dir: &Path) -> Result<Vec<u64>> {
    let mut segments = Vec::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();

        let id = name.to_str().and_then(parse_segment_id);

        if let Some(id) = id {
            segments.push(id);
        }
    }

    segments.sort_unstable();

    Ok(segments)
}

pub fn segment_path(dir: &Path, id: u64) -> PathBuf {
    let segment_file = segment_filename(id);

    dir.join(segment_file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_filename() {
        let id = 42;
        let expected = "00000000000000000042.log";
        assert_eq!(segment_filename(id), expected);
    }

    #[test]
    fn test_parse_segment_id() {
        let name = "00000000000000000042.log";
        let expected: Option<u64> = Some(42);
        assert_eq!(parse_segment_id(name), expected);
    }

    #[test]
    fn test_parse_segment_id_invalid_suffix() {
        let name = "00000000000000000042.txt";
        let expected: Option<u64> = None;
        assert_eq!(parse_segment_id(name), expected);
    }

    #[test]
    fn test_parse_segment_id_invalid_length() {
        let name = "42.log";
        let expected: Option<u64> = None;
        assert_eq!(parse_segment_id(name), expected);
    }

    #[test]
    fn test_parse_segment_id_invalid() {
        let name = "invalid_name.log";
        let expected: Option<u64> = None;
        assert_eq!(parse_segment_id(name), expected);
    }

    #[test]
    fn test_list_segments() {
        use std::io::Write;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let file1 = dir.path().join(segment_filename(1));
        let file2 = dir.path().join(segment_filename(2));
        let file3 = dir.path().join("not_a_segment.log");

        fs::File::create(&file1)
            .unwrap()
            .write_all(b"test")
            .unwrap();
        fs::File::create(&file2)
            .unwrap()
            .write_all(b"test")
            .unwrap();
        fs::File::create(&file3)
            .unwrap()
            .write_all(b"test")
            .unwrap();

        let segments = list_segments(dir.path()).unwrap();
        assert_eq!(segments, vec![1, 2]);
    }

    #[test]
    fn test_segment_path() {
        let dir = Path::new("/tmp/wal");
        let id = 42;
        let expected = Path::new("/tmp/wal/00000000000000000042.log");
        assert_eq!(segment_path(dir, id), expected);
    }
}
