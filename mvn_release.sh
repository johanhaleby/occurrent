#!/bin/bash
releaseVersion=""
skipTests="false"

usage="${PROGNAME} [-h] [-st] [-v] -- Release script for Occurrent

where:
    -h, --help
        Show this help text
    -v, --version
        The version to release (required)
    -st, --skip-tests
        Skip tests when releasing, default false"

if [ "$#" -ne 0 ]; then
	while [ "$#" -gt 0 ]
	do
		case "$1" in
		-h|--help)
		    echo "$usage"
			exit 0
			;;
		-v|--version)
			releaseVersion="$2"
			;;
    -st|--skip-tests)
      skipTests="true"
      ;;
		--)
			break
			;;
		-*)
			echo "Invalid option '$1'. Use --help to see the valid options" >&2
			exit 1
			;;
		# an option mvn_cmd, continue
		*)	;;
		esac
		shift
	done
fi

if [ -z "${version}" ]; then
    echo "$usage"
    exit 1
fi

echo "Preparing release of Occurrent ${releaseVersion}"
echo "!!!!!DON'T FORGET TO SWITCH TO JAVA 17!!!!!"
read -r -e -s -p "Enter sonatype password: " sonatypePassword
echo
echo
echo "Starting to release Occurrent $releaseVersion"

mavenArguments=-Dgpg.passphrase=${sonatypePassword} -DskipTests=${skipTests}"
versionBeforeRelease=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout) && \
mvn release:prepare -Prelease -DautoVersionSubmodules=true -Dtag="occurrent-${releaseVersion}" -DreleaseVersion="${releaseVersion}" -Darguments="${mavenArguments}" && \
mvn release:perform -Prelease -Darguments="${mavenArguments}"
mavenReleaseStatus=$?

if [ $mavenReleaseStatus -eq 0 ]; then
  echo "Release successful, will update version number for modules that were not included in release build." && \
  git pull --rebase # Note that we don't use && here because we may already be up-to-date, and if so, pull returns an "error".
  versionAfterRelease=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout) && \
  # We normalize versionAfterRelease by escaping dots with \. This is required for sed to work in find.
  normalizedVersionBeforeRelease=$(echo "${versionBeforeRelease}" | sed 's/\./\\./g') && \
  find . -name "pom.xml" -type f -exec sed -i "" "s/${normalizedVersionBeforeRelease}/${versionAfterRelease}/g" {} + && \
  git commit -am "Updated version number for modules that were not included in release build to ${versionAfterRelease}" && \
  git push && \
  echo "Maven release of Occurrent $releaseVersion completed successfully"
else
  echo "Maven release of Occurrent $releaseVersion failed"
  exit $mavenReleaseStatus
fi

