# big-well
**开发背景:*

confluent 中kudu-sink连接器没有直接连接kudu,而是连接impala. impala的性能是有一定的问题,当数据量越大,同步越慢.再者impala安装环境依赖hive，为了数据的同步引入更多复杂的环境。这不是最佳方案。
所以我们放弃了impala的使用,开发了此插件,用于同步kafka数据到kudu中.

本插件基于confluent platform开发，实现kafka消息同步到kudu功能。单节点(4核8G)每秒可同步万条，支持增删改实时同步.支持1.7版本kudu所有支持的所有类型.对于mysql中date,datetime,timestamp 类型我们一律要求在kudu表中定义为unixtime_micros.

## 项目结构:

连接器:Kafka-connect-kudu

日志服务: log-streams (暂未开发完,与连接器插件没有关联)

## 安装环境:

​	JDK: 1.8

​	Kafka: 2.2.0

​	kudu: 1.7.1 

​	maven: 3.6.X

​	debezium:0.9.4

## 安装:

1.share/java创建一个插件目录如：kafka-kudu-sink

2.Maven install之后将[kafka-connect-kudu-1.0-SNAPSHOT.jar](kafka-connect-kudu-1.0-SNAPSHOT.jar)包放入confluent安装目录下的share/java/kafka-kudu-sink/

3.配置/etc/hosts 文件:IP 主机名称   ex:192.168.1.9 slave1

4.执行:curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d '{"name": "kafka-kudu-sink-19", "config": {"connector.class":"com.yr.connector.KuduSinkConnector", "tasks.max":"1", "topics":"dev.tb_uhome_acct_item_tmp_1", "topic.table.map" : "dev.tb_uhome_acct_item_tmp_1:tb_uhome_acct_item","table.list":"tb_uhome_acct_item,tb_uhome_house","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.storage.StringConverter","kudu.masters":"127.0.0.1:7051","max.retries":"3","[retry.backoff.ms](retry.backoff.ms)":"1000","behavior.on.exception":"FAIL","[linger.ms](linger.ms)":"1000","batch.size":"1000","max.buffered.records":"8000","[flush.timeout.ms](flush.timeout.ms)":"6000","[offset.flush.timeout.ms](offset.flush.timeout.ms)":"5000"}}'

**config信息:**

{

 "name": "kafka-kudu-sink-19",--连接器名称

 "config": {

  "connector.class": "com.yr.connector.KuduSinkConnector",--连接器实现

  "tasks.max": "1",--任务数

  "topics": "dev.tb_uhome_acct_item_tmp_1",--消费topic名称，多个用逗号分开

  "source.sink.map": "tb_uhome_acct_item:tb_uhome_acct_item,tb_uhome_house:tb_uhome_house",--源表同步到kudu表的映射
  "table.list": "tb_uhome_acct_item,tb_uhome_house",--源表同步到kudu表的映射

  "key.converter": "org.apache.kafka.connect.storage.StringConverter",--key类型转换

  "value.converter": "org.apache.kafka.connect.storage.StringConverter",--value类型转换

  "kudu.masters": "127.0.0.1:7051",--kudu masters节点地址，多个逗号分开

  "max.retries": "3",--同步失败重试次数

  "[retry.backoff.ms](retry.backoff.ms)": "1000",--每次重试间隔时间

  "behavior.on.exception": "FAIL",--同步发生异常时如何处理，fail:停止连接器，warn、error：抛异常不停连接器，ignore:直接忽略该异常

  "[linger.ms](linger.ms)": "1000",--消息缓存时间

  "batch.size": "1000",--批处理量

  "max.buffered.records": "8000",--缓存大小，记录数

  "[flush.timeout.ms](flush.timeout.ms)": "6000",--处理消息超时时间

  "[offset.flush.timeout.ms](offset.flush.timeout.ms)": "5000"--提交消息offset的间隔时间

 }

}

## 生产测试效果:



单个分区每5秒可达到13W的吞吐量,每秒的吞吐大小再5M左右

![img](https://github.com/dengbp/big-well/blob/master/kafka-connect-kudu/src/doc/1s.png)

![img](https://github.com/dengbp/big-well/blob/master/kafka-connect-kudu/src/doc/5s.png)



## 后续版本计划:

  1.自定义创建kudu表

  2.异常数据的处理

   ........

如有使用问题及优化建议可在Issues中提出.
:email:215941826@qq.com,576833409@qq.com









# big-well 

* Development background: 

* The connector in confluent is not directly connected to kudu, but connected to impala. The performance of impala is a certain problem, when the amount of data is larger, the synchronization is slower. So we gave up impala Used and developed this plug-in for synchronizing kafka data to kudu. This plug-in is developed based on the confluent platform and implements the function of synchronizing kafka messages to kudu.

* A single node can synchronize tens of thousands of items per second and support real-time synchronization of additions, deletions and changes. All types supported by kudu version 1.7 are supported. For date, datetime, and timestamp types in mysql, we all require that they are defined as unixtime_micros in the kudu table. 

* ## Project Structure: 

  Connector:

*  Kafka-connect-kudu 

* Log service: log-streams (not yet developed, not related to the connector plugin) 

* ## Installation environment:

*  JDK: 1.8 

* Kafka: 2.2.0 

* kudu: 1.7.1

*  maven : 3.6.X 

* debezium: 0.9.4 

* ## Installation:

* 1.share / java to create a plugin directory such as: kafka-kudu-sink

*  2.Maven install will be [kafka-connect-kudu-1.0-SNAPSHOT.jar] ( kafka-connect-kudu-1.0-SNAPSHOT.jar) package into share / java / kafka-kudu-sink / under the confluent installation directory

* 3.Configure the / etc / hosts file: IP host name ex: 192.168.1.9 slave1 

* 4.Implementation: 

  curl -i -X POST -H "Accept: application / json" -H "Content-Type: application / json" http: // localhost: 8083 / connectors / -d '{"name": "kafka-kudu -sink-19 "," config ": {" connector.class ":" com.yr.connector.KuduSinkConnector "," tasks.max ":" 1 "," topics ":" dev.tb_uhome_acct_item_tmp_1","source.sink.map":"tb_uhome_acct_item:tb_uhome_acct_item,tb_uhome_house:tb_uhome_house"," key.converter ":" org.apache.kafka.connect.storage.StringConverter "," value. : "org.apache .kafka.connect.storage.StringConverter "," kudu.masters ":" 127.0.0.1:7051","max.retries":"3","[retry.backoff.ms](retry.backoff.ms) " : "1000", "behavior.on.exception": "FAIL", "[linger.ms] (linger.ms)": "1000", "batch.size": "1000", "max.buffered.records ":" 8000 "," [flush.timeout.ms] (flush.timeout.ms) ":" 6000 "," [offset.flush.timeout.ms] (offset.flush.timeout.ms) ":" 5000 "}} '

  **config information:** 

  {" name ":" kafka-kudu-sink-19 ",-connector name"

   config ": {" connector.class ":" com.yr.connector.KuduSinkConnector ",-The connector implements

  " tasks.max ":" 1 ",-The number of tasks

  " topics ":" dev.tb_uhome_acct_item_tmp_1 ",-Consumption topic names, multiple separated by commas

  "source.sink.map": "tb_uhome_acct_item:tb_uhome_acct_item,tb_uhome_house:tb_uhome_house",-source table to target table map 

  "key.converter": " org.apache.kafka.connect.storage.StringConverter ",-key type conversion

  "value.converter ":" org.apache.kafka.connect.storage.StringConverter ",-value type conversion

  " kudu.masters ":" 127.0.0.1:7051 ", --kudu masters node address, multiple commas separated

  " max.retries ":" 3 ",-Synchronization failed retry count

  " retry.backoff.ms ":" 1000 ",-Every retry interval

  " behavior.on.exception ":" FAIL ", --How to handle when an exception occurs synchronously, fail: stop the connector, warn, error: throw the exception without stopping the connector, ignore: ignore the exception directly

   "linger.ms": "1000",- -Message buffer time

   "batch.size": "1000",-batch amount 

  "max.buffered.records": "8000",-buffer size, number of records

   "flush.timeout.ms ":" 6000 ",-Timeout for processing messages

  " offset.flush.timeout.ms ":" 5000 "-the interval for submitting messages to offset}}

  # Follow-up version plan: 

  1. Customize the creation of kudu tables 

  2. Handling of abnormal data 

  3. ........ 

     If there are usage problems and optimization suggestions, they can be raised in Issues.

     Email: 215941826@qq.com , 576833409@qq.com
