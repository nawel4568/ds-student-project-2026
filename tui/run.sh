#!/bin/sh
# Lanterna needs the real terminal, which `gradle run` does not provide: build the launcher, then exec it.
set -e
cd "$(dirname "$0")/.."
./gradlew -q :tui:installDist
exec tui/build/install/tui/bin/tui "$@"
