#!/bin/bash
read -p "Enter the version to release: " releaseVersion
echo "Starting to release Occurrent $releaseVersion"

mvn release:prepare -Prelease -pl !example -pl !test-support -DautoVersionSubmodules=true -Dtag="occurrent-${releaseVersion}" -DreleaseVersion="${releaseVersion}" &&
mvn release:perform -Prelease -pl !example -pl !test-support

echo "Maven release of Occurrent $releaseVersion completed successfully"

