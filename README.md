# spark-job-praictice

## 三个spark job 完成CDC json 数据 =〉 stage 数据 =〉final表的流程处理。 

每个job可重复执行，通过传入的参数完成相应步骤的事情。
然后我们通过pipeline把他们串联起来就可以作为demo的case了。

### Pipeline的步骤如下

1. 命令行运行job，把cdc changeset生成stage 表中

./spark-submit --class com.saic.delta.CdcChangeSetToStage --master local[2] --jars /usr/local/hadoop-2.7.7/spark-2.4.4/bin/delta-core_2.11-0.4.0.jar /Users/huqianghui/Documents/cdcDeltaDemo/target/cdcDeltaDemo-1.0-SNAPSHOT.jar  file:///Users/huqianghui/Downloads/insert.json /user/huqianghui/test

2. 通过spark shell 来验证数据

./spark-shell --packages io.delta:delta-core_2.11:0.4.0
val df = spark.read.format("delta").load("/user/huqianghui/test/staging")
df.show()

3. 通过syn job 把stage中的数据同步到final 表中

./spark-submit --class com.saic.delta.CdcSynFinal --master local[2] --jars /usr/local/hadoop-2.7.7/spark-2.4.4/bin/delta-core_2.11-0.4.0.jar /Users/huqianghui/Documents/cdcDeltaDemo/target/cdcDeltaDemo-1.0-SNAPSHOT.jar  /user/huqianghui/test/final/lineItem /user/huqianghui/test/staging

校验final 数据
val df = spark.read.format("delta").load("/user/huqianghui/test/final/lineItem")
df.show()

4. 通过clean job把stage数据删除

./spark-submit --class com.saic.delta.StageDataClean --master local[2] --jars /usr/local/hadoop-2.7.7/spark-2.4.4/bin/delta-core_2.11-0.4.0.jar /Users/huqianghui/Documents/cdcDeltaDemo/target/cdcDeltaDemo-1.0-SNAPSHOT.jar /user/huqianghui/test/staging