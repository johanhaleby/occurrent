#!/bin/bash
releaseVersion=""
skipTests="false"

usage="${PROGNAME} [-h] [-st] [-v] -- Release script for Occurrent

where:
    -h, --help
        Show this help text
    -v, --version
        The version to deploy locally (required)
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

echo "Starting to deploy Occurrent $releaseVersion locally (skip tests=$skipTests)"
mvn -Prelease -DskipTests=${skipTests} -Dgpg.skip clean install -Drevision=${releaseVersion}