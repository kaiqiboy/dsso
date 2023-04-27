bash /home/kaiqi/spark-3.2.1-bin-hadoop3.2/bin/spark-submit\
    --class RecordQueryTimeTpch\
    --master spark://master:7077\
    --executor-memory 16g\
    --total-executor-cores 48\
    --executor-cores 2\
    --driver-memory 50g\
    --conf spark.sql.autoBroadcastJoinThreshold=8g\
    --conf spark.sql.objectHashAggregate.sortBased.fallbackThreshold=4096\
    --conf spark.sql.ceo=true\
    --conf spark.sql.ceoDir=/home/kaiqi/dsso/dsso-deploy\
    --conf spark.sql.ceoServerIP=127.0.0.1\
    --conf spark.sql.ceoPruneAggressive=true\
    --conf spark.sql.ceoMetadataDir=/datasets/tpch30-metadata.json\
    --conf spark.sql.ceoLengthThreshold=32\
    /home/kaiqi/dsso/dsso-test/target/scala-2.12/sql-opt_2.12-0.1.jar /datasets/tpch100 /home/kaiqi/dsso/data/tpch/queries_test/1.sql 0 /datasets/test /home/kaiqi/dsso/tmp 
