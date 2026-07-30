#!/usr/bin/env bash
# Fails when a @EnabledOnOs or @EnabledOnJre annotation gates a test on a condition no CI job can
# ever satisfy, e.g. @EnabledOnOs(MAC) when every shard in maven.yml runs ubuntu-latest, or
# @EnabledOnJre(JAVA_8) when the java: matrix only builds 21 and 25. A test like that does not run
# anywhere in CI; it just sits in the tree looking tested. See issue #467, where five macOS-only
# and three Java-8-only tests had been dead in exactly this way, unnoticed, for a long time.
#
# Why this is not a second list to keep in sync with maven.yml: it parses the operating systems out
# of every runs-on: in that file and the JDKs out of its java: matrix, the same way
# verify-shard-coverage (also in maven.yml) derives shard coverage from the file instead of
# maintaining a duplicate. Whatever maven.yml runs on is what a test is allowed to require.
#
# Deliberately no allow-list or suppression mechanism. After the #467 cleanup this repository has
# zero @EnabledOnOs/@EnabledOnJre annotations, so the guard is green on an empty set, and an
# allow-list would only give the next one somewhere to hide.
#
# No associative arrays: this needs to run on the bash 3.2 that ships with macOS, not just the
# bash 5 on CI runners.

set -euo pipefail

maven_yml=.github/workflows/maven.yml

if [ ! -f "$maven_yml" ]; then
  echo "::error::$maven_yml not found; cannot derive what CI can run."
  exit 1
fi

# ---- What operating systems does CI actually run on? -----------------------
# Space-separated list of JUnit org.junit.jupiter.api.condition.OS names, e.g. "LINUX MAC".
ci_os=""
while IFS= read -r runner; do
  case "$runner" in
    ubuntu-*) ci_os="$ci_os LINUX" ;;
    macos-*) ci_os="$ci_os MAC" ;;
    windows-*) ci_os="$ci_os WINDOWS" ;;
  esac
done < <(grep -oE 'runs-on:[[:space:]]*[A-Za-z0-9_.-]+' "$maven_yml" | sed -E 's/runs-on:[[:space:]]*//' | sort -u)
ci_os=$(echo "$ci_os" | tr ' ' '\n' | sed '/^$/d' | sort -u | tr '\n' ' ')
ci_os=${ci_os% }

if [ -z "$ci_os" ]; then
  echo "::error::Could not parse any runs-on value out of $maven_yml"
  exit 1
fi

# ---- What JDKs does CI actually build and test with? ------------------------
# Space-separated list of major version numbers, e.g. "21 25".
ci_jdk=$(grep -oE 'java:[[:space:]]*\[[^]]*\]' "$maven_yml" | head -1 | grep -oE '[0-9]+' | sort -un | tr '\n' ' ')
ci_jdk=${ci_jdk% }

if [ -z "$ci_jdk" ]; then
  echo "::error::Could not parse any JDK version out of the java: matrix in $maven_yml"
  exit 1
fi

status=0
checked=0

# One row per @EnabledOnOs/@EnabledOnJre annotation, followed by the declaration (method or class)
# it precedes: <file>\t<line>\t<annotation text>\t<declaration text>
#
# Kotlin counts too. No Kotlin test carries either annotation today, but this repository has 332 Kotlin
# test files, so a Java-only scan would let the next one hide.
rows=$(
  while IFS= read -r -d '' file; do
    awk -v file="$file" '
      BEGIN { pending = 0 }
      {
        line = $0
        trimmed = line
        sub(/^[ \t]+/, "", trimmed)
        if (trimmed ~ /^@EnabledOnOs\(/ || trimmed ~ /^@EnabledOnJre\(/) {
          pending_line[pending] = NR
          pending_ann[pending] = trimmed
          pending++
          next
        }
        if (pending > 0) {
          if (trimmed ~ /^@/ || trimmed == "" || trimmed ~ /^\/\// || trimmed ~ /^\*/ || trimmed ~ /^\/\*/) {
            next
          }
          for (i = 0; i < pending; i++) {
            printf("%s\t%s\t%s\t%s\n", file, pending_line[i], pending_ann[i], trimmed)
          }
          pending = 0
        }
      }
    ' "$file"
  done < <(find . -path '*/src/test/*' \( -name '*.java' -o -name '*.kt' \) -print0)
)

if [ -n "$rows" ]; then
  while IFS=$'\t' read -r file lineno annotation declaration; do
    checked=$((checked + 1))

    if [[ "$declaration" == *'`'* ]]; then
      # A Kotlin test name in backticks, e.g. fun `writes to the store`().
      name=$(sed -E 's/^[^`]*`([^`]*)`.*/\1/' <<<"$declaration")
    elif [[ "$declaration" == *class* ]]; then
      name=$(sed -E 's/^.*\bclass[[:space:]]+([A-Za-z_][A-Za-z0-9_]*).*/\1/' <<<"$declaration")
    else
      name=$(sed -E 's/^.*[^A-Za-z0-9_]([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*\(.*/\1/' <<<"$declaration")
    fi

    # The annotation's argument list, e.g. "MAC" or "{MAC, LINUX}" or "JAVA_8".
    args=$(sed -E 's/^@Enabled(OnOs|OnJre)\((.*)\)$/\2/' <<<"$annotation")
    args=${args//[\{\}]/}

    if [[ "$annotation" == @EnabledOnOs\(* ]]; then
      satisfiable=0
      for token in ${args//,/ }; do
        token=${token##*.}
        for os in $ci_os; do
          [ "$token" = "$os" ] && satisfiable=1
        done
      done
      if [ "$satisfiable" -eq 0 ]; then
        echo "::error::$file:$lineno: $name is @EnabledOnOs($args), but CI only runs on: $ci_os. This test never runs in CI."
        status=1
      fi
    else
      satisfiable=0
      for token in ${args//,/ }; do
        token=${token##*.}
        if [[ "$token" =~ JAVA_([0-9]+) ]]; then
          jdk="${BASH_REMATCH[1]}"
          for v in $ci_jdk; do
            [ "$jdk" = "$v" ] && satisfiable=1
          done
        fi
      done
      if [ "$satisfiable" -eq 0 ]; then
        echo "::error::$file:$lineno: $name is @EnabledOnJre($args), but CI only builds and tests with JDK: $ci_jdk. This test never runs in CI."
        status=1
      fi
    fi
  done <<<"$rows"
fi

if [ "$status" -eq 0 ]; then
  echo "No @EnabledOnOs/@EnabledOnJre annotation is stranded outside what CI runs. Checked $checked" \
       "annotation(s) against CI operating systems ($ci_os) and JDKs ($ci_jdk)."
fi
exit "$status"
