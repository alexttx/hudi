#!/bin/bash

query () {
    beeline -u jdbc:hive2://hiveserver:10000 \
        --hiveconf hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat \
        --hiveconf hive.stats.autogather=false \
        --silent=true
}

query <<'EOF'
!sh echo #### List tables
show tables;

!sh echo #### Look at partitions that were added
show partitions stock_ticks_mor_rt;

!sh echo #### COW Query
select symbol, max(ts) from stock_ticks_cow group by symbol HAVING symbol = 'GOOG';

!sh echo #### COW Projection
select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_cow where  symbol = 'GOOG';


!sh echo #### MOR ReadOptimized and snapshot queries
!sh echo #### Notice latest timestamps are the same
select symbol, max(ts) from stock_ticks_mor_ro group by symbol HAVING symbol = 'GOOG';
select symbol, max(ts) from stock_ticks_mor_rt group by symbol HAVING symbol = 'GOOG';


!sh echo #### MOR Read Optimized and Snapshot project queries
select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_ro where  symbol = 'GOOG';
select `_hoodie_commit_time`, symbol, ts, volume, open, close  from stock_ticks_mor_rt where  symbol = 'GOOG';

EOF

