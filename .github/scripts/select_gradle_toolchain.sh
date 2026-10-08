#!/usr/bin/env bash
# Usage: select_gradle_toolchain.sh <tree>
# Picks the durable Gradle that <tree>'s wrapper pins; exports GRADLE to $GITHUB_ENV.
set -euo pipefail

TREE="${1:?usage: select_gradle_toolchain.sh <tree>}"
TOOLCHAINS="${OTZARIA_GRADLE_TOOLCHAINS:-/opt/otzaria-cache/toolchains}"
PROPS="$TREE/gradle/wrapper/gradle-wrapper.properties"

fail() { echo "::error::$*" >&2; exit 1; }

[ -f "$PROPS" ] || fail "no $PROPS"
COUNT="$(grep -cE '^distributionUrl=' "$PROPS" || true)"
[ "$COUNT" = 1 ] || fail "$PROPS must hold exactly one distributionUrl (found $COUNT)"
URL="$(grep -E '^distributionUrl=' "$PROPS" | tr -d '\r')"
re='^distributionUrl=.*/gradle-([0-9]+(\.[0-9]+)+)-(bin|all)\.zip$'
[[ "$URL" =~ $re ]] || fail "unparseable $URL in $PROPS"
VERSION="${BASH_REMATCH[1]}"

BIN="$TOOLCHAINS/gradle-$VERSION/bin/gradle"
[ -d "$TOOLCHAINS/gradle-$VERSION" ] || fail "tree pins Gradle $VERSION but $TOOLCHAINS/gradle-$VERSION is not installed"
[ -x "$BIN" ] || fail "$BIN is missing or not executable"
FOUND="$("$BIN" --version | awk '/^Gradle / {print $2; exit}')"
[ "$FOUND" = "$VERSION" ] || fail "$BIN reports Gradle ${FOUND:-nothing}, expected $VERSION"

echo "Gradle $VERSION for $(git -C "$TREE" rev-parse HEAD 2>/dev/null || echo "$TREE"): $BIN"
if [ -n "${GITHUB_ENV:-}" ]; then echo "GRADLE=$BIN" >> "$GITHUB_ENV"; fi
