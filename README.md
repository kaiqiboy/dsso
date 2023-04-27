# Deep Learning based Query Optimization for Spark SQL
 
Existing distributed data query systems like Spark SQL rely on manually crafted rules to select an execution plan, which is often sub-optimal. While recent studies have attempted to use deep learning models for query optimization in conventional relational databases, integrating deep learning models with Spark SQL poses system challenges in efficient candidate plan exploration and realtime deep learning inference. This paper presents an end-to-end deep-learning-based query optimization that is seamlessly integrated with the native Spark system and practically reduces query execution time. Spark SQL’s core logic is modified to expand the plan exploration space, design an LSTM-based model to estimate the cost of physical execution plans, and establish performance inference of candidate plans. Experimental results evidence that the proposed system leads to over 13% performance improvement on public benchmarks compared to native Spark SQL.

![Overview of DSSO](./overview.png)


## Structure

- The modified Spark is located at `./spark-3.2.1-modified`

- The deep cost estimation *development* module is located at `./dsso-dev`

- The deep cost estimation *deployment* module is located at `./dsso-deploy` 

- Scala code for training data generation and end-to-end evaluation is located at `./dsso-test` 

- The queries used for DL model development is located at `./data`

The complementary data including the example training physical plans and the trained word2vec models can be downloaded from [here](https://drive.google.com/drive/folders/1hY41lU7s6CPEbT1BS9cOrhrEs3H4nxzk?usp=sharing).
## Usage: DL-enhanced Spark SQL Execution

- Build the modified Spark 
```
   cd DIR_TO_MODIFIED_SPARK
    ./build/sbt package
```
- Run a spark-submit application with cost-estimation-based optimization enabled (example)
```
bash DIR_TO_MODIFIED_SPARK/bin/spark-submit\
    --class TestXXX\
    --master spark://master:7077\
    --executor-memory 16g\
    --total-executor-cores 48\
    --executor-cores 2\
    --driver-memory 50g\
    --conf spark.sql.autoBroadcastJoinThreshold=8g\
    --conf spark.sql.objectHashAggregate.sortBased.fallbackThreshold=4096\
    --conf spark.sql.ceo=true\
    --conf spark.sql.ceoDir=xxx/cost-estimation-deploy\
    --conf spark.sql.ceoServerIP=127.0.0.1\
    --conf spark.sql.ceoPruneAggressive=true\
    --conf spark.sql.ceoMetadataDir=xxx/xx-metadata\
    --conf spark.sql.ceoLengthThreshold=32\
    xxx.jar inputArgs
```

| Config| Explantion                                                                                                                                                                       |
|-------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|spark.sql.autoBroadcastJoinThreshold| Set this native configuration to a large value (8g) to ensure thorough exploration.                                                                                              |
spark.sql.objectHashAggregate.sortBased.fallbackThreshold | Set this native configuration to a large value (4096) to ensure thorough exploration.                                                                                            |
|spark.sql.ceo | Set true to enable cost-estimation-based optimization (default = false).                                                                                                         |
|spark.sql.ceoDir | Directory to the deployment folder, which contains the trained model and facilitative files (default = "/").                                                                     |
|spark.sql.ceoServerIP | The IP of the DL companion server. If the server is `localhost`, it can be automatically started by SparkSession, otherwise it has to be manually start (default = "127.0.0.1"). |
|spark.sql.ceoServerPort | The port of the DL companion server (default = "8308").                                                                                                                          |
|spark.sql.ceoPruneAggressive | Set true to enable aggressive pruning (default = false).                                                                                                                         |
|spark.sql.ceoMetadataDir | Directory storing the metada file of the tables (default = ""). Better in HDFS.                                                                                                  |
|spark.sql.ceoLengthThreshold | The plan length threshold of enabling cost-estimation-based optimization (default=500).                                                                                          |

## Usage: DL model development

Dependencies (from `pip`):
```
pytorch
scikit-learn
fse
gensim
```

TPC-H data generation: https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Data%20generation%20tool.30.xml?embedded=true

- Data generation

    ```cd dsso-dev```

    `RecordQueryTime.scala` explains the process of generating training data 

- Moel training

    ```cd dsso-dev```

    First run `node_embedding_xxx.ipynb`, then run `lstm_xxx.ipynb`.

    Once the model is trained, move the trained model and the encoding files to `./dsso-deploy`.


## Usage: DSSO test

- ```cd dsso-test; mkdir lib; cp PATH_TO_MODIFIED_JARS/*.jar lib```
- ```sbt package```