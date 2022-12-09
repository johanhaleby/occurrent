#!/bin/bash
echo "!!!!!DON'T FORGET TO SWTICH TO JAVA 17!!!!!"
read -p "Enter the version to release: " releaseVersion
echo "Starting to release Occurrent $releaseVersion"

mvn release:prepare -Prelease -DautoVersionSubmodules=true -Dtag="occurrent-${releaseVersion}" -DreleaseVersion="${releaseVersion}" && \
mvn release:perform -Prelease && \
echo "Maven release of Occurrent $releaseVersion completed successfully"