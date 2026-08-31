//! Small shared utilities.

use crate::Error;

/// Parse a human duration like `30d`, `12h`, `45m`, `3600s`, `500ms`, `2w`,
/// or a concatenation (`1d12h`). Bare integers are rejected — every duration
/// in the configuration carries its unit, either in the field name (`_secs`,
/// `_ms`) or, here, in the value.
pub fn parse_duration(input: &str) -> crate::Result<std::time::Duration> {
    let s = input.trim();
    if s.is_empty() {
        return Err(Error::Config("duration must not be empty".to_string()));
    }

    let mut total_ms: u128 = 0;
    let mut number = String::new();
    let mut chars = s.chars().peekable();
    let mut parsed_any = false;

    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() {
            number.push(c);
            chars.next();
            continue;
        }
        if number.is_empty() {
            return Err(Error::Config(format!(
                "invalid duration '{input}': expected a number before '{c}'"
            )));
        }
        let mut unit = String::new();
        while let Some(&u) = chars.peek() {
            if u.is_ascii_alphabetic() {
                unit.push(u);
                chars.next();
            } else {
                break;
            }
        }
        let value: u128 = number
            .parse()
            .map_err(|_| Error::Config(format!("invalid duration '{input}': number too large")))?;
        let ms: u128 = match unit.as_str() {
            "ms" => value,
            "s" => value * 1_000,
            "m" => value * 60_000,
            "h" => value * 3_600_000,
            "d" => value * 86_400_000,
            "w" => value * 604_800_000,
            other => {
                return Err(Error::Config(format!(
                    "invalid duration '{input}': unknown unit '{other}' \
                     (expected ms, s, m, h, d or w)"
                )))
            }
        };
        total_ms = total_ms
            .checked_add(ms)
            .ok_or_else(|| Error::Config(format!("duration '{input}' overflows")))?;
        number.clear();
        parsed_any = true;
    }

    if !number.is_empty() {
        return Err(Error::Config(format!(
            "invalid duration '{input}': missing unit after '{number}' \
             (write e.g. '{number}s' or '{number}d')"
        )));
    }
    if !parsed_any {
        return Err(Error::Config(format!("invalid duration '{input}'")));
    }
    if total_ms == 0 {
        return Err(Error::Config(format!(
            "duration '{input}' must be greater than zero"
        )));
    }
    u64::try_from(total_ms)
        .map(std::time::Duration::from_millis)
        .map_err(|_| Error::Config(format!("duration '{input}' overflows")))
}

#[cfg(test)]
mod tests {
    use super::parse_duration;
    use std::time::Duration;

    #[test]
    fn parses_all_units_and_concatenation() {
        for (input, expected_ms) in [
            ("500ms", 500u64),
            ("3600s", 3_600_000),
            ("45m", 2_700_000),
            ("12h", 43_200_000),
            ("30d", 2_592_000_000),
            ("2w", 1_209_600_000),
            ("1d12h", 129_600_000),
            (" 5m ", 300_000),
        ] {
            assert_eq!(
                parse_duration(input).unwrap(),
                Duration::from_millis(expected_ms),
                "{input}"
            );
        }
    }

    #[test]
    fn rejects_bare_numbers_zero_and_garbage() {
        for input in ["30", "", "d", "5x", "0s", "-5m", "1d5"] {
            assert!(parse_duration(input).is_err(), "{input} should be rejected");
        }
    }
}
