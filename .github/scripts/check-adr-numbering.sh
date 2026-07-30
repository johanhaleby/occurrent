#!/usr/bin/env bash
# Fails when two ADRs claim the same number, or when an ADR's markdown heading number does not
# match the number in its filename.
#
# Why this is not redundant with the /adr skill: that skill already refuses a number taken in
# the tree it can see. Every collision so far slipped past it anyway, because two branches each
# added an ADR the other did not have, so both were locally correct. 0067 (resolved in
# 4915884013), 0070 (5acbc7db3) and 0077 (acba9a9e5). A check that sees one working tree
# structurally cannot catch that, which is why this runs on pull_request, where the checkout is
# the branch already merged with main and the collision is visible before it lands.
#
# The heading check guards the other half of a renumber. The number lives in two places, the
# filename and the "# NN. Title" heading, so a rename that forgets the heading leaves an ADR
# claiming to be a number it is not. Both renumbers so far had to fix the heading by hand.

set -euo pipefail

dir=doc/architecture/decisions
if [ -f .adr-dir ]; then
  dir=$(tr -d '[:space:]' < .adr-dir)
fi
if [ ! -d "$dir" ]; then
  echo "No ADR directory at '$dir', nothing to check."
  exit 0
fi

status=0

# Every number is claimed by exactly one file.
while read -r num; do
  [ -n "$num" ] || continue
  echo "::error::ADR number $num is claimed by more than one file:"
  for f in "$dir/$num"-*.md; do
    echo "::error::  $f"
  done
  echo "::error::Renumber the later claimant to the next free number and update every"
  echo "::error::reference to it (changelog.md, doc/, .context/ORCHESTRATOR.md, pom comments)."
  status=1
done < <(for f in "$dir"/[0-9][0-9][0-9][0-9]-*.md; do
           [ -e "$f" ] || continue
           basename "$f" | cut -c1-4
         done | sort | uniq -d)

# The heading number agrees with the filename number.
for f in "$dir"/[0-9][0-9][0-9][0-9]-*.md; do
  [ -e "$f" ] || continue
  file_num=$(basename "$f" | cut -c1-4)
  head_num=$(head -n 1 "$f" | sed -nE 's/^# ([0-9]+)\..*/\1/p')
  if [ -z "$head_num" ]; then
    echo "::error::$f does not start with a '# <number>. <title>' heading"
    status=1
  elif [ "$((10#$file_num))" -ne "$head_num" ]; then
    echo "::error::$f is numbered $((10#$file_num)) in its filename but $head_num in its heading"
    status=1
  fi
done

if [ "$status" -eq 0 ]; then
  echo "ADR numbering is consistent in '$dir': no duplicate numbers, every heading matches its filename."
fi
exit "$status"
