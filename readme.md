# Spark 算子使用大全

**（Scala 和 Java Version）**

## 简介
该项目主要针对Spark项目各种常用算子编写的各种demo，即插即用。十分方便学习和调试spark算子。目前demo案例涵盖了Spark core，Spark SQL和Spark Streaming，有java 和 Scala双版本。


`相关知乎文章`


## 安装
在命令行输入
`mvn install`
安装pom.xml的依赖，如果下载太慢就自己改maven镜像为国内。

## 代码

> *java version*

###  spark core
##### broadcast
- AccumulatorOperator
- oadcastJoin
- BroadCastOperator
- BroadCastTest
- WordCount

##### dijkstra
- Dijkstra
- Node

##### operator
- AccumulatorTest
- AggregateByKeyOperator
- AggregateOperator
- BroadCastJoin
- BroadcastTest
- CacheTest
- CartesianOperator
- CoalesceOperator
- CogroupOperator
- CollectOperator
- CountByKeyOperator
- CountOperator
- DinstinctOperator
- DoubleAggregate
- FilterOperator
- FirstApp
- FirstWordCount
- FlatMapOperator
- ForeachParititonsOperator
- GroupByKeyOperator
- IntersectionOperator
- JavaOperator
- JoinOperator
- LineCount
- MapOperator
- MapPartitionsOperator
- MapPartitonsWithIndexOperator
- MapTest
- PartionerOperator
- PartitionerByOperator
- POperator
- ReduceByKeyOperator
- ReduceOperator
- RepartitionAndSortWithinPartitionsOperator
- RepartitionOperator
- SampleOperator
- SampleTest
- SaprkApp
- SaveAsTextFileOperator
- SortByKeyOperator
- SortObj
- TakeOperator
- TakeSample
- UnionOperator
- WC
- WordCount
- WordCount_test
- DoubleJoin
- DoubelReduceByKey

##### optimization
- DefinedComparator
- FilterMoreKey
- JoinBroadCastOperator
- MapFilter
- ReduceByKeyOperator
- RepartitionAndSortWithinPartitionsOperator
- SkewedJoin

##### persist
- Test

##### puv
- GenerateDataPUV
- SortObj
- UserLogOps

##### secondsort
- SecondarySortTest
- SecondSortKey

### spark SQL

##### createdf
- DataFrameOpsFromFile
- DataFrameOpsFromJsonRdd
- Person
- RDD2DataFrameByProgrammatically
- RDD2DataFrameByReflection
- SparkSQLwithJoin

##### demo
- DailySale
- DailyTop3Operator
- GenericLoadSave
- HiveDataSource
- JDBCDataSource
- JSONDataSource
- ManuallySpecifyOptions
- ParquetLoadData
- ParquetPartitionDiscovery
- RowNumberWindowFunction
- SaveModeTest
- UDAF
- UDF

### Spark Streaming
- SparkStreamingBroadcastAccumulator
- SparkStreamingDataManuallyProducerForKafka
- SparkStreamingOnHDFS
- SparkStreamingOnHDFSToMySQL
- SparkStreamingOnKafkaDirected
- SparkStreamingOnKafkaReceiver
- TransformOperator
- UpdateStateByKeyOperator
- WindowOperator
- WordCountOnline
- WordCountOnTransform

### topn
- GroupByKeyOps
- SecondarySortOps
- SecondSortKey
- TopOps



------------

> scala version 

#### spark core
- BroadcastOperator
- BroadCastTest
- combineByKey
- DistinctOperator
- glom
- JoinOperator
- RandomSplitoperator
- UnionOperator
- zipOperator
- AccumulatorOperator
- AggregateOperator
- CachedTest
- CoalesceOperator
- CogroupOperator
- CollectOperator
- CombineByKeyOperator
- CountByKeyOperator
- CountOperator
- DistinctOperator
- FilterOperator
- FilterRdd
- FlatMapOperator
- GroupByKeyOperator
- JoinOperator
- LineCount
- MapOperator
- MapPartitionsOperator
- MapValuesOperator
- PartionByOperator
- ReduceByKeyOperator
- ReduceOperator
- RepartitionOperator
- SampleOperator
- SaveAsTextFileOperator
- SortByKeyOperator
- SortByOperator
- TakeOperator
- WC
- WordCount

### spark SQL
##### createdf
- DataFrameOpsFromFile
- DataFrameOpsFromJsonRdd
- RDD2DataFrameByProgrammatically
- RDD2DataFrameByReflectionScala

#####  demo
- DailySale
- DailyUV
- JDBCDataSource

##### hive
- HiveDataSource

##### json
- JSONDataSource

##### loadsave
- GenericLoadSave
- ManuallySpecifyOptions
- SaveModeTest

##### parquet
- ParquetDataSourcePartition
- ParquetDataSourceSchemaMerge
- ParquetLoadData

### Spark Streaming
- WordCountOnline

### topn
- GroupTopN
- TopN

### UDF_UDAF
- UDAF
- UDF