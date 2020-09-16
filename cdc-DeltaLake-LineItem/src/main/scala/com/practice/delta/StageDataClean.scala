package com.practice.delta

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

object StageDataClean {
  def main(args: Array[String]){
    require(args.length >=1,"Please input the stage table path.")
    val spark = SparkSession.builder().appName("stage clean").getOrCreate();
    val stageTable = DeltaTable.forPath(spark, args(0));
    stageTable.delete();
  }
}
