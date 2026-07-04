use anyhow::{bail, Result};

/// Topic filter that supports `+` (single level) and `#` (multi level) wildcards.
#[derive(Debug, Clone)]
pub struct TopicPattern {
    segments: Vec<Segment>,
    /// Best-effort: treat the first `+` segment as the device_id position.
    device_id_index: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Lit(String),
    One,
    Multi,
}

impl TopicPattern {
    pub fn new(raw: &str) -> Result<Self> {
        let raw = raw.trim().trim_matches('/').to_string();
        if raw.is_empty() {
            bail!("topic pattern must not be empty");
        }

        let parts: Vec<&str> = raw.split('/').collect();
        let mut segments = Vec::with_capacity(parts.len());
        let mut device_id_index = None;

        for (i, seg) in parts.iter().enumerate() {
            if seg.is_empty() {
                bail!("topic pattern contains empty segment: '{}'", raw);
            }

            match *seg {
                "+" => {
                    if device_id_index.is_none() {
                        device_id_index = Some(i);
                    }
                    segments.push(Segment::One);
                }
                "#" => {
                    if i != parts.len() - 1 {
                        bail!("'#' must be the last segment in topic pattern: '{}'", raw);
                    }
                    segments.push(Segment::Multi);
                }
                lit => segments.push(Segment::Lit(lit.to_string())),
            }
        }

        // Convention fallback: "smarthome/<device_id>/..." even if TopicPattern uses a concrete device id.
        if device_id_index.is_none() && parts.len() >= 2 && parts[0] == "smarthome" {
            device_id_index = Some(1);
        }

        Ok(Self {
            segments,
            device_id_index,
        })
    }

    pub fn matches(&self, topic: &str) -> bool {
        let topic = topic.trim().trim_matches('/');
        let t_parts: Vec<&str> = topic.split('/').collect();

        let mut ti = 0usize;
        for (pi, pseg) in self.segments.iter().enumerate() {
            match pseg {
                Segment::Multi => {
                    // '#' matches the rest; allowed only at the end.
                    return pi == self.segments.len() - 1;
                }
                Segment::One => {
                    if ti >= t_parts.len() {
                        return false;
                    }
                    if t_parts[ti].is_empty() {
                        return false;
                    }
                    ti += 1;
                }
                Segment::Lit(lit) => {
                    if ti >= t_parts.len() {
                        return false;
                    }
                    if t_parts[ti] != lit {
                        return false;
                    }
                    ti += 1;
                }
            }
        }
        ti == t_parts.len()
    }

    pub fn device_id_from_topic<'a>(&self, topic: &'a str) -> Option<&'a str> {
        let idx = self.device_id_index?;
        let topic = topic.trim().trim_matches('/');
        topic.split('/').nth(idx)
    }
}
