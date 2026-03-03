/// Expand `${VAR}` patterns in a string with environment variable values.
///
/// Unset variables are replaced with an empty string. Literal `${...}`
/// sequences that should not be expanded are not common in YAML configs,
/// so this simple approach is sufficient.
pub fn expand_env_vars(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            for c in chars.by_ref() {
                if c == '}' {
                    break;
                }
                var_name.push(c);
            }
            match std::env::var(&var_name) {
                Ok(val) => result.push_str(&val),
                Err(_) => {
                    tracing::warn!(
                        "Environment variable '{}' is not set, using empty string",
                        var_name
                    );
                }
            }
        } else {
            result.push(ch);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_known_var() {
        std::env::set_var("TEST_EXPAND_USER", "alice");
        let result = expand_env_vars("user: ${TEST_EXPAND_USER}");
        assert_eq!(result, "user: alice");
        std::env::remove_var("TEST_EXPAND_USER");
    }

    #[test]
    fn test_expand_unknown_var() {
        std::env::remove_var("CERTAINLY_NOT_SET_12345");
        let result = expand_env_vars("pass: ${CERTAINLY_NOT_SET_12345}");
        assert_eq!(result, "pass: ");
    }

    #[test]
    fn test_no_vars() {
        let input = "plain text without variables";
        assert_eq!(expand_env_vars(input), input);
    }

    #[test]
    fn test_multiple_vars() {
        std::env::set_var("TEST_EXPAND_A", "hello");
        std::env::set_var("TEST_EXPAND_B", "world");
        let result = expand_env_vars("${TEST_EXPAND_A} ${TEST_EXPAND_B}");
        assert_eq!(result, "hello world");
        std::env::remove_var("TEST_EXPAND_A");
        std::env::remove_var("TEST_EXPAND_B");
    }

    #[test]
    fn test_dollar_without_brace_unchanged() {
        let result = expand_env_vars("price is $5");
        assert_eq!(result, "price is $5");
    }
}
