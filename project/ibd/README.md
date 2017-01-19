# README

```
.
|__src.main.java/ -- `代码目录`
|  |
|  |__cn.ac.sict/
|     |
|     |__example/
|     |  |__WordCount.java -- `spark driver 程序 wordcount demo`
|     |
|     |__hbase.spark.dao/ -- `hbase-spark DAO 层 API`
|     |
|     |__main/
|     |  |__Main.java -- `spark driver 程序的主入口`
|     |
|     |__signal/ -- `将从 Kafka 接收的 json 数据解析为 java 对象`
|     |  |__Signal.java -- `所有传感器信号的父类, 新的传感器信号必须继承此类`
|     |  |__TemperSignal.java -- `温度传感器`
|     |
|     |__source/
|     |  |__KafkaStreamSource.java -- `用于创建 SparkStreaming 数据流的 Kafka 源`
|     |
|     |__store/ -- `流数据存储相关代码`
|     |  |__Store.java -- `存储到 HBase(调用 hbaseSparkDAO 的 API)`
|     |
|     |__Vis/ -- `可视化相关代码`
|        |__RedisClient.java  -- `redis 资源池`
|        |__KafkaSparkStreaming.java -- `数据流经 Kafka, Spark Streaming, redis 最终用于实时可视化`
|
|__src.main.resource/ -- `资源文件目录`
   |__sysConfig.properties -- `存放公共常量, 读取方式见 cn.ac.sict.main.Main.java`
```
