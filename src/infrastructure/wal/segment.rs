use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;

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
