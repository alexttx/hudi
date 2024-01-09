#!/bin/bash

set -u

CMD=${0##*/}

# Options
FAKE=0
MOR=0
QUIET=0
COMPACT=0
TOPIC=
TABLE=

help () {
    cat <<EOF
Usage: $CMD [Options] TOPIC
Use spark-submit to ingest data Kafka topic TOPIC to Hudi table TABLE.
Options:
  -h  // print help
  -v  // verbose
  -n  // show spark-submit command but don't execute it
  -m  // use a MOR table instead of a COW table
  -d  // disable compaction (only meaningful for MOR tables)
  -t TABLE   // ingest in to table TABLE

The default table is:
  - \${TOPIC}_mor if ingesting into a MOR table, and
  - \${TOPIC}_cow if ingesting into a COW table.
EOF
}

warn () {
    while (( $# > 0 )); do echo "$1"; shift; done 1>&2
}
err () {
    warn "$@"
    exit 1
}
syntax () {
    err "$@" "Use -h for help."
}

while getopts ":hvnmdt:" op; do
    case $op in
        (h) help; exit 0;;
        (v) VERBOSE=1;;
        (n) FAKE=1;;
        (m) MOR=1;;
        (d) COMPACT=0;;
        (t) TABLE=$OPTARG;;
        (:) syntax "Option -$OPTARG requires an argument";;
        (\?) syntax "Invalid option: -$OPTARG";;
        (*) syntax "BUG: unhandled getopts flag: -$op";;
    esac
done

shift $((OPTIND - 1))
(( $# == 1 )) || syntax "Missing TOPIC."

if [[ -z "$TABLE" ]]; then
    if (( MOR )); then
        TABLE=${TOPIC}_mor
    else
        TABLE=${TOPIC}_cow
    fi
fi

cmd=()
cmd+=(spark-submit)
cmd+=(--class org.apache.hudi.utilities.streamer.HoodieStreamer "$HUDI_UTILITIES_BUNDLE")
if (( MOR )); then
    cmd+=(--table-type MERGE_ON_READ)
else
    cmd+=(--table-type COPY_ON_WRITE)
    if (( !COMPACT )); then
        cmd+=(--disable-compaction)
    fi
fi
cmd+=(--source-class org.apache.hudi.utilities.sources.JsonKafkaSource)
cmd+=(--source-ordering-field ts)
cmd+=(--target-base-path "/user/hive/warehouse/$TABLE")
cmd+=(--target-table "$TABLE")
cmd+=(--props /var/demo/config/kafka-source.properties)
cmd+=(--schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider)

if (( !QUIET || FAKE )); then
    echo "${cmd[@]}"
fi
if (( !FAKE )); then
    "${cmd[@]}"
fi
    
