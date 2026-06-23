#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

smt_dir="$repo_root/smt/kafka-source-id"
plugin_dir="$repo_root/plugin/kafka-source-id-smt"
jar_path="$plugin_dir/kafka-source-id-smt.jar"
gradlew="$smt_dir/gradlew"

if [[ ! -x "$gradlew" ]]; then
  echo "Gradle wrapper is missing or not executable: $gradlew" >&2
  exit 1
fi

mkdir -p "$plugin_dir"

GRADLE_USER_HOME="${GRADLE_USER_HOME:-$repo_root/.gradle}" \
  "$gradlew" --no-daemon -p "$smt_dir" --quiet clean jar

cp "$smt_dir/build/libs/kafka-source-id-smt.jar" "$jar_path"

echo "Built $jar_path"
