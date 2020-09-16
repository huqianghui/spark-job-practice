package com.practice.delta

import CdcChangeSetToStage.LineItem
import io.delta.tables.DeltaTable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object CdcSynFinal {
  def main(args: Array[String]){
    require(args.length >=2,"Please input final table path and stage table path.")

    val conf = new SparkConf().setAppName("Cdc ChangeSet")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("Cdc syn final").getOrCreate();
    val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)

    var existingFinalDeltaTable: DeltaTable = null

    if (fs.exists( new org.apache.hadoop.fs.Path(args(0)))){
      existingFinalDeltaTable =DeltaTable.forPath(spark, args(0));
    }else{
      import spark.implicits._
      Seq(LineItem(0,0,0,0,0,0,0,0,0,null,null,null,null,null,null,null,null)).toDF(
                                "id",
                                          "l_orderkey",
                                          "l_partkey",
                                          "l_suppkey",
                                          "l_linenumber",
                                          "l_quantity",
                                          "l_extendedprice",
                                          "l_discount",
                                          "l_tax",
                                          "l_returnflag",
                                          "l_linestatus",
                                          "l_shipdate",
                                          "l_commitdate",
                                          "l_receiptdate",
                                          "l_shipinstruct",
                                          "l_shipmode",
                                          "l_comment"
                                          ).write.format("delta").save(args(0))
      existingFinalDeltaTable =DeltaTable.forPath(spark, args(0));
    }

    val stageTable = DeltaTable.forPath(spark, args(1)).as("stage");
    val stageDf= stageTable.toDF;

    existingFinalDeltaTable.as("final").merge(stageDf,"final.id = stage.id")
      .whenNotMatched().insert(Map(
                                  "id" -> stageDf.col("id"),
                                  "l_orderkey" -> stageDf.col("record.l_orderkey"),
                                  "l_partkey" -> stageDf.col("record.l_partkey"),
                                  "l_suppkey" -> stageDf.col("record.l_suppkey"),
                                  "l_linenumber" -> stageDf.col("record.l_linenumber"),
                                  "l_quantity" -> stageDf.col("record.l_quantity"),
                                  "l_extendedprice" -> stageDf.col("record.l_extendedprice"),
                                  "l_discount" -> stageDf.col("record.l_discount"),
                                  "l_tax" -> stageDf.col("record.l_tax"),
                                  "l_returnflag" -> stageDf.col("record.l_returnflag"),
                                  "l_linestatus" -> stageDf.col("record.l_linestatus"),
                                  "l_shipdate" -> stageDf.col("record.l_shipdate"),
                                  "l_commitdate" -> stageDf.col("record.l_commitdate"),
                                  "l_receiptdate" -> stageDf.col("record.l_receiptdate"),
                                  "l_shipinstruct" -> stageDf.col("record.l_shipinstruct"),
                                  "l_shipmode" -> stageDf.col("record.l_shipmode"),
                                  "l_comment" -> stageDf.col("record.l_comment")
                              ))
      .whenMatched("stage.is_delete ").delete()
      .whenMatched("stage.is_update ").update(Map(
                                                            "l_orderkey" -> stageDf.col("record.l_orderkey"),
                                                            "l_partkey" -> stageDf.col("record.l_partkey"),
                                                            "l_suppkey" -> stageDf.col("record.l_suppkey"),
                                                            "l_linenumber" -> stageDf.col("record.l_linenumber"),
                                                            "l_quantity" -> stageDf.col("record.l_quantity"),
                                                            "l_extendedprice" -> stageDf.col("record.l_extendedprice"),
                                                            "l_discount" -> stageDf.col("record.l_discount"),
                                                            "l_tax" -> stageDf.col("record.l_tax"),
                                                            "l_returnflag" -> stageDf.col("record.l_returnflag"),
                                                            "l_linestatus" -> stageDf.col("record.l_linestatus"),
                                                            "l_shipdate" -> stageDf.col("record.l_shipdate"),
                                                            "l_commitdate" -> stageDf.col("record.l_commitdate"),
                                                            "l_receiptdate" -> stageDf.col("record.l_receiptdate"),
                                                            "l_shipinstruct" -> stageDf.col("record.l_shipinstruct"),
                                                            "l_shipmode" -> stageDf.col("record.l_shipmode"),
                                                            "l_comment" -> stageDf.col("record.l_comment")
                                                            )).execute()
  }
}
