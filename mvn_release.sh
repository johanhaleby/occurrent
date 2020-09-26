#!/bin/bash
sdk use java 8.0.222.hs-adpt
read -p "Enter the version to release: " releaseVersion
echo "Starting to release Occurrent $releaseVersion"

mvn release:prepare -Prelease -DautoVersionSubmodules=true -Dtag="occurrent-${releaseVersion}" -DreleaseVersion="${releaseVersion}" && \
mvn release:perform -Prelease && \
echo "Maven release of Occurrent $releaseVersion completed successfully"