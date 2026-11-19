# Dependency-download retries only (transfer errors, a plugin or artifact that
# could not be resolved, timeouts). Contract/compilation failures still fail
# immediately; each download failure remains in the evidence directory.
registry_maven() {
  local registry_log="$1" registry_attempt registry_try_log
  shift
  for registry_attempt in 1 2 3 4 5; do
    registry_try_log="${registry_log}.attempt-${registry_attempt}"
    # Force refresh of Maven's cached transfer failures on a retry/run.
    if mvn -U "$@" > "$registry_try_log" 2>&1; then
      cp "$registry_try_log" "$registry_log"
      return 0
    fi
    cp "$registry_try_log" "$registry_log"
    if ! grep -Eqi 'could not transfer artifact|failed to transfer|status code: (429|50[0234])|status=50[0234]|no plugin found for prefix|could not find artifact|connection reset|timed out' "$registry_try_log" || [[ "$registry_attempt" == 5 ]]; then
      cat "$registry_try_log" >&2
      return 1
    fi
    # Maven Central sometimes answers "not found" for artifacts that exist
    # (seen on shared CI runners); it recovers within a minute or two.
    echo "Maven dependency transfer failed; retry ${registry_attempt}/5 (evidence: ${registry_try_log})" >&2
    sleep "$((registry_attempt * 20))"
  done
}
