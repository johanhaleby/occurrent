#!/bin/bash
echo "!!!!!DON'T FORGET TO SWITCH TO JAVA 17!!!!!"
read -p "Enter the version to release: " releaseVersion
read -s -p "Sonatype password: " sonatypePassword
echo "Starting to release Occurrent $releaseVersion"

mvn release:prepare -Prelease -DautoVersionSubmodules=true -Dtag="occurrent-${releaseVersion}" -DreleaseVersion="${releaseVersion}" -Dgpg.passphrase="${sonatypePassword}" && \
mvn release:perform -Prelease && \
echo "Maven release of Occurrent $releaseVersion completed successfully"