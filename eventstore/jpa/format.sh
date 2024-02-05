#!/bin/bash

google-java-format -r $(find . -type f -name "**.java")

# git ls-files -z | while IFS= read -rd '' f; do if file --mime-encoding "$f" | grep -qv binary; then tail -c1 < "$f" | read -r _ || echo >> "$f"; fi; done
