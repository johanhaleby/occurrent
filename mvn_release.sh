#!/bin/bash
echo "!!!!!DON'T FORGET TO SWITCH TO JAVA 17!!!!!"
read -p "Enter the version to release: " releaseVersion
echo "Starting to release Occurrent $releaseVersion"

mvn release:prepare -Prelease -DautoVersionSubmodules=true -Dtag="occurrent-${releaseVersion}" -DreleaseVersion="${releaseVersion}" && \
mvn release:perform -Prelease && \

echo "Updating version number for modules that were not included in release build"
snapshotVersionOfMainPom=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout) && \
mvn versions:set -DnewVersion="${snapshotVersionOfMainPom}" -DprocessAllModules -DgenerateBackupPoms=false && \
git commit -am "Updated version number for modules that were not included in release build" && \
git push && \
echo "Maven release of Occurrent $releaseVersion completed successfully"