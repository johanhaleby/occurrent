#!/bin/bash
echo "!!!!!DON'T FORGET TO SWITCH TO JAVA 17!!!!!"
read -p "Enter the version to release: " releaseVersion
echo "Starting to release Occurrent $releaseVersion"

versionBeforeRelease=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout) && \

mvn release:prepare -Prelease -DautoVersionSubmodules=true -Dtag="occurrent-${releaseVersion}" -DreleaseVersion="${releaseVersion}" && \
mvn release:perform -Prelease && \

echo "Release successful, will update version number for modules that were not included in release build." && \
git pull --rebase # Note that we don't use && here because we may already be up-to-date, and if so, pull returns an "error".
versionAfterRelease=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout) && \
# We normalize versionAfterRelease by escaping dots with \. This is required for sed to work in find.
normalizedVersionBeforeRelease=$(echo "${versionBeforeRelease}" | sed 's/\./\\./g') && \
find . -name "pom.xml" -type f -exec sed -i "" "s/${normalizedVersionBeforeRelease}/${versionAfterRelease}/g" {} + && \
git commit -am "Updated version number for modules that were not included in release build to ${versionAfterRelease}" && \
git push && \
echo "Maven release of Occurrent $releaseVersion completed successfully"