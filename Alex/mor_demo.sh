#!/bin/bash

CMD_DIR=$(dirname "$(readlink -f "$0")")
HUDI_TOP=$(readlink -f "$CMD_DIR/..")

set -u

init_counters () {
    declare -g yy=2018 mm=1 dd=1 hh=9
}

bump_counters () {
    declare -g yy mm dd hh

    (( hh++ ))
    (( hh <= 12 )) && return
    (( hh = 9 ))

    (( dd++ ))
    (( dd <= 25 )) && return
    (( dd = 1 ))

    (( mm++ ))
    (( mm <= 12 )) && return
    (( mm = 1 ));

    (( yy++ ))
}

batches=4
batch_size=2000
table=stock_ticks

init_counters

bash "$HUDI_TOP"/docker/setup_demo.sh

for ((bn = 0; bn < batches; bn++)); do

    # Publish a batch to Kafka
    "$CMD_DIR"/stock_ticker.py -y $yy -m $mm -d $dd -t $hh -l $batch_size | kcat -b kafkabroker -t $table -P

    # Ingest from Kafka to MOR
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
EOF

    if (( bn == 0 )); then
        # This tells Hive what it needs to know to run queries on the table.
        # Only do this once, after the table has been created by the first
        # batch.
        docker exec -i adhoc-1 /bin/bash -x <<'EOF'
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
    fi

    docker exec -i adhoc-1 hadoop --config /etc/hadoop fs -ls -R '/user/hive/warehouse/stock_ticks*/20*'

    bump_counters

done
