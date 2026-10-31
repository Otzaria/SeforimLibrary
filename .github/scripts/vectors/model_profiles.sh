# Per-family model packages; the default pins stay the packages of released applications.
# Sourced after pins.env, with HERE pointing to this directory.
MODEL_PIN_KEYS=(PASSAGE_QUANTIZATION PASSAGE_PACKAGE_CHECKSUM QUERY_PACKAGE_CHECKSUM QUERY_GRAPH MODEL_REPO MODEL_REVISION MODEL_GRAPH)
MODEL_PIN_DEFAULTS=()
for model_pin in "${MODEL_PIN_KEYS[@]}"; do MODEL_PIN_DEFAULTS+=("${!model_pin}"); done

select_model_profile() {
  local family=$1 entry profile="" i
  # Reset every model pin before switching families, including a switch back to a default.
  for i in "${!MODEL_PIN_KEYS[@]}"; do printf -v "${MODEL_PIN_KEYS[$i]}" '%s' "${MODEL_PIN_DEFAULTS[$i]}"; done
  MODEL_REMOTE_GRAPH=""; QUERY_REMOTE_GRAPH=""
  MODEL_REQUIRE_CACHE=false
  MODEL_REQUIRE_PRERELEASE=false
  for entry in ${FAMILY_MODEL_PROFILES:-}; do
    [ "${entry%%:*}" != "$family" ] || profile=${entry#*:}
  done
  if [ -n "$profile" ]; then
    [[ "$profile" =~ ^[a-z0-9_-]+$ ]] || { echo "invalid model profile '$profile'" >&2; return 64; }
    . "$HERE/model-pins/$profile.env"
  fi
  [ -n "${MODEL_REVISION:-}" ] || { echo "no model revision pinned for $family" >&2; return 64; }
}
