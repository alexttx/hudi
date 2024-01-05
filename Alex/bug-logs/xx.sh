#!/bin/bash

CMD_DIR=$(dirname "$(readlink -f "$0")")
HUDI_TOP=$(readlink -f "$CMD_DIR/..")

set -x
set -u

n=$1
shift

bash "$HUDI_TOP"/docker/setup_demo.sh

cat "$HUDI_TOP"/docker/demo/data/batch_1.json | head -n$n | kcat -b kafkabroker -t stock_ticks -P

kcat -b kafkabroker -L -J | jq -C .

docker exec -i adhoc-1 /bin/bash -x <<'EOF'

spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type MERGE_ON_READ \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts \
  --target-base-path /user/hive/warehouse/stock_ticks_mor \
  --target-table stock_ticks_mor \
  --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --disable-compaction

/var/hoodie/ws/hudi-sync/hudi-hive-sync/run_sync_tool.sh \
  --jdbc-url jdbc:hive2://hiveserver:10000 \
  --user hive \
  --pass hive \
  --partitioned-by dt \
  --base-path /user/hive/warehouse/stock_ticks_mor \
  --database default \
  --table stock_ticks_mor \
  --partition-value-extractor org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
EOF

cat "$HUDI_TOP"/docker/demo/data/batch_2.json | head -n100 | kcat -b kafkabroker -t stock_ticks -P

docker exec -i adhoc-2 /bin/bash -x <<'EOF'

spark-submit \
  --class org.apache.hudi.utilities.streamer.HoodieStreamer $HUDI_UTILITIES_BUNDLE \
  --table-type MERGE_ON_READ \
  --source-class org.apache.hudi.utilities.sources.JsonKafkaSource \
  --source-ordering-field ts \
  --target-base-path /user/hive/warehouse/stock_ticks_mor \
  --target-table stock_ticks_mor \
  --props /var/demo/config/kafka-source.properties \
  --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
  --disable-compaction
EOF



docker exec -i adhoc-1 hadoop --config /etc/hadoop fs -ls -R '/user/hive/warehouse/stock_ticks*/20*'

