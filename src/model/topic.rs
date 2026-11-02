use anyhow::{bail, Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Lit(String),
    One,
    Multi,
}

/// MQTT topic filter for configured routes.
///
/// This type preserves the configured filter exactly and implements MQTT wildcard and
/// system-topic matching rules.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttTopicFilter {
    raw: String,
    segments: Vec<Segment>,
}

#[allow(dead_code)]
impl MqttTopicFilter {
    /// Parses an MQTT topic filter without trimming or normalizing it.
    ///
    /// # Errors
    ///
    /// Returns an error if the filter has empty levels, embedded wildcards, a nonterminal
    /// `#`, a NUL character, or unsupported shared-subscription syntax.
    pub fn new(raw: &str) -> Result<Self> {
        if raw.is_empty() {
            bail!("MQTT topic filter must not be empty");
        }
        if raw.contains('\0') {
            bail!("MQTT topic filter must not contain NUL");
        }
        if raw.starts_with("$share/") {
            bail!("shared subscriptions are not supported");
        }

        let parts: Vec<&str> = raw.split('/').collect();
        let mut segments = Vec::with_capacity(parts.len());
        for (index, part) in parts.iter().enumerate() {
            if part.is_empty() {
                bail!("MQTT topic filter must not contain empty levels");
            }
            match *part {
                "+" => segments.push(Segment::One),
                "#" if index == parts.len() - 1 => segments.push(Segment::Multi),
                "#" => bail!("MQTT multi-level wildcard must be the final level"),
                literal if literal.contains('+') || literal.contains('#') => {
                    bail!("MQTT wildcards must occupy a complete topic level")
                }
                literal => segments.push(Segment::Lit(literal.to_owned())),
            }
        }

        Ok(Self {
            raw: raw.to_owned(),
            segments,
        })
    }

    /// Returns the exact filter text used to create this value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Returns whether this filter matches the supplied MQTT topic name.
    #[must_use]
    pub fn matches(&self, topic: &str) -> bool {
        if !valid_topic_name(topic) {
            return false;
        }

        if topic.starts_with('$')
            && matches!(self.segments.first(), Some(Segment::One | Segment::Multi))
        {
            return false;
        }

        let mut topic_levels = topic.split('/');
        for filter_level in &self.segments {
            match filter_level {
                Segment::Multi => return true,
                Segment::One => {
                    if topic_levels.next().is_none() {
                        return false;
                    }
                }
                Segment::Lit(literal) => {
                    if topic_levels.next() != Some(literal.as_str()) {
                        return false;
                    }
                }
            }
        }
        topic_levels.next().is_none()
    }

    /// Returns whether this filter and another can match the same topic name.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        let Some(left_first) = self.segments.first() else {
            return false;
        };
        let Some(right_first) = other.segments.first() else {
            return false;
        };
        if wildcard_cannot_match_system_level(left_first, right_first)
            || wildcard_cannot_match_system_level(right_first, left_first)
        {
            return false;
        }

        let max_levels = self.segments.len().max(other.segments.len());
        for index in 0..max_levels {
            let left = self.segments.get(index);
            let right = other.segments.get(index);
            match (left, right) {
                (Some(Segment::Multi), _) | (_, Some(Segment::Multi)) => return true,
                (None, _) | (_, None) => return false,
                (Some(Segment::Lit(left)), Some(Segment::Lit(right))) if left != right => {
                    return false;
                }
                (Some(Segment::Lit(literal)), Some(Segment::One))
                | (Some(Segment::One), Some(Segment::Lit(literal)))
                    if index == 0 && literal.starts_with('$') =>
                {
                    return false;
                }
                _ => {}
            }
        }
        true
    }

    /// Extracts a zero-based topic level after confirming the topic matches this filter.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic does not match, or the requested level is absent or
    /// empty.
    pub fn topic_segment<'a>(&self, topic: &'a str, index: usize) -> Result<&'a str> {
        if !self.matches(topic) {
            bail!("topic does not match filter '{}'", self.raw);
        }
        topic
            .split('/')
            .nth(index)
            .filter(|segment| !segment.is_empty())
            .with_context(|| format!("topic segment {index} is missing or empty"))
    }
}

#[allow(dead_code)]
fn valid_topic_name(topic: &str) -> bool {
    !topic.is_empty()
        && !topic.contains('\0')
        && !topic.contains('+')
        && !topic.contains('#')
        && topic.split('/').all(|level| !level.is_empty())
}

#[allow(dead_code)]
fn wildcard_cannot_match_system_level(wildcard: &Segment, other: &Segment) -> bool {
    matches!(wildcard, Segment::One | Segment::Multi)
        && matches!(other, Segment::Lit(level) if level.starts_with('$'))
}

#[cfg(test)]
mod configured_route_tests {
    use super::*;

    #[test]
    fn test_configured_route_topic_matches_supported_filters_exactly() {
        assert!(MqttTopicFilter::new("sensor/room").is_ok_and(|f| f.matches("sensor/room")));
        assert!(!MqttTopicFilter::new("sensor/room").is_ok_and(|f| f.matches("sensor/room/")));
        assert!(MqttTopicFilter::new("sensor/+").is_ok_and(|f| f.matches("sensor/kitchen")));
        assert!(!MqttTopicFilter::new("sensor/+").is_ok_and(|f| f.matches("sensor/")));
        assert!(MqttTopicFilter::new("sensor/#").is_ok_and(|f| f.matches("sensor")));
        assert!(MqttTopicFilter::new("sensor/#").is_ok_and(|f| f.matches("sensor/a/b")));
        assert!(!MqttTopicFilter::new("sensor/#").is_ok_and(|f| f.matches("sensors/a")));
    }

    #[test]
    fn test_configured_route_topic_preserves_filter_text_without_normalizing() {
        for filter in ["/sensor/a", "sensor/a/"] {
            assert!(MqttTopicFilter::new(filter).is_err(), "accepted {filter:?}");
        }

        let spaced = MqttTopicFilter::new(" sensor /a ").expect("spaces are literal topic text");
        assert_eq!(spaced.as_str(), " sensor /a ");
        assert!(spaced.matches(" sensor /a "));
        assert!(!spaced.matches("sensor/a"));
    }

    #[test]
    fn test_configured_route_topic_rejects_unsupported_filter_syntax() {
        for filter in [
            "",
            "sensor//a",
            "sensor/a//",
            "sen+or/a",
            "sensor/a#",
            "sensor/#/a",
        ] {
            assert!(MqttTopicFilter::new(filter).is_err(), "accepted {filter:?}");
        }

        assert!(MqttTopicFilter::new("sensor/\0/a").is_err());
        assert!(MqttTopicFilter::new("$share/group/sensor/+").is_err());
    }

    #[test]
    fn test_configured_route_topic_obeys_leading_dollar_rule() -> Result<()> {
        assert!(!MqttTopicFilter::new("+/status")?.matches("$SYS/status"));
        assert!(!MqttTopicFilter::new("#")?.matches("$SYS/status"));
        assert!(MqttTopicFilter::new("$SYS/#")?.matches("$SYS/status"));
        assert!(MqttTopicFilter::new("+/status")?.matches("room/status"));
        Ok(())
    }

    #[test]
    fn test_configured_route_topic_extracts_explicit_zero_based_segment() -> Result<()> {
        let filter = MqttTopicFilter::new("home/+/sensor")?;

        assert_eq!(filter.topic_segment("home/lab/sensor", 1)?, "lab");
        assert_eq!(filter.topic_segment("home/lab/sensor", 0)?, "home");
        assert!(filter.topic_segment("home/lab/sensor", 3).is_err());
        assert!(filter.topic_segment("home//sensor", 1).is_err());
        assert!(filter.topic_segment("other/lab/sensor", 1).is_err());
        Ok(())
    }

    #[test]
    fn test_topic_overlap_detects_intersecting_filter_languages() -> Result<()> {
        let overlaps = [
            ("a", "a"),
            ("a/+", "a/b"),
            ("a/#", "a"),
            ("a/+/c", "a/b/#"),
            ("a/+", "a/#"),
        ];

        for (left, right) in overlaps {
            assert!(
                MqttTopicFilter::new(left)?.overlaps(&MqttTopicFilter::new(right)?),
                "{left:?} should overlap {right:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_topic_overlap_allows_disjoint_filters_and_system_topic_separation() -> Result<()> {
        let disjoint = [
            ("a/b", "a/c"),
            ("a/+", "b/+"),
            ("+", "$SYS/#"),
            ("#", "$SYS/status"),
        ];

        for (left, right) in disjoint {
            assert!(
                !MqttTopicFilter::new(left)?.overlaps(&MqttTopicFilter::new(right)?),
                "{left:?} must not overlap {right:?}"
            );
        }

        assert!(MqttTopicFilter::new("$SYS/+")?.overlaps(&MqttTopicFilter::new("$SYS/status")?));
        Ok(())
    }
}
