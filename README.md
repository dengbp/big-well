# big-well
本插件基于confleunt platform开发，实现kafka消息同步到kudu功能。单节点每秒可同步万条，支持增删改实时同步.支持1.7版本kudu所有支持的所有类型.对于mysql中date,datetime,timestamp 类型我们一律要求在kudu表中定义为unixtime_micros.

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

  "topic.table.map": "dev.tb_uhome_acct_item_tmp_1:tb_uhome_acct_item",--topic与kudu目标表映射，多个用逗号分开

  "table.list": "tb_uhome_acct_item,tb_uhome_house",--kudu中需要同步的表名

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

## 后续版本计划:

​	1.自定义创建kudu表
​	2.异常数据的处理
​		........

如有使用问题及优化建议可在Issues中提出.
:email:215941826@qq.com,576833409@qq.com

