#!/bin/bash

topic=stock_ticks
(( $# > 0 )) && { topic=$1; shift; }

case $topic in
    (*_cow) echo "MOR tables shouldn't be named ..._cow"; exit;;
esac

table=${topic}_mor

spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type MERGE_ON_READ \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts \
  --target-base-path /user/hive/warehouse/"$table" \
  --target-table "$table" \
  --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --disable-compaction
