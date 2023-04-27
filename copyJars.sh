spark_home="/home/kaiqi/spark-3.2.1-bin-hadoop3.2"
rm -r ${spark_home}/jars
cp -r ~/modified-spark-jars ${spark_home}/jars
for i in {1..5}; do
    ssh worker$i "rm ${spark_home}/modified-spark-jars"
    rsync -auvr ~/modified-spark-jars/ worker$i:${spark_home}/jars/
done
bash ${spark_home}/sbin/stop-all.sh
bash ${spark_home}/sbin/start-all.sh
