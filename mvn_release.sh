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

if [ -z "${releaseVersion}" ]; then
    echo "$usage"
    exit 1
fi

echo "!!!!!DON'T FORGET TO SWITCH TO JAVA 17!!!!!"
echo You may also need to disable "Stay invisible at the local network" in NordVPN, see nordvpn.md.
echo "Preparing release of Occurrent ${releaseVersion} (skip tests=$skipTests)"
read -r -e -s -p "Enter sonatype password: " sonatypePassword
echo
echo
echo "Starting to release Occurrent $releaseVersion (skip tests=$skipTests)"

mvn deploy -Prelease -DskipTests=true -Drevision=${releaseVersion}
mavenReleaseStatus=$?

git checkout master

if [ $mavenReleaseStatus -ne 0 ]; then
  echo "Maven release of Occurrent $releaseVersion failed"
  exit $mavenReleaseStatus
fi