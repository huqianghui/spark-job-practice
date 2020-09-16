package com.saic.delta

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import java.util.Date

import com.saic.delta.bean.CdcOperationEnum

object CdcChangeSetToStage {
  def main(args: Array[String]){

    require(args.length >=2,"Please input the input path and the output path.")
    val spark = SparkSession.builder().appName("Cdc ChangeSet").getOrCreate();


    //val changeRecordEncode :Encoder[ChangeRecord] =  Encoders.kryo[ChangeRecord]
    import spark.implicits._
    val changeSetDF: Dataset[ChangeRecord] = spark.read.json(args(0)).as[ChangeRecord]
    //val changeSetDF = cscData.as(changeRecordEncoder)

    def  mapToStage(cdc: ChangeRecord):StageRecord ={
      if (CdcOperationEnum.isInserting(cdc.o)){
       return new StageRecord(false,false,cdc.after.id,new Date().getTime().toString,cdc.after);
      }

      if (CdcOperationEnum.isDeleting(cdc.o)){
        return new StageRecord(false,true,cdc.before.id,new Date().getTime().toString,null);
      }

      if (CdcOperationEnum.isUpdating(cdc.o)){
        return new StageRecord(true,false,cdc.after.id,new Date().getTime().toString,cdc.after);
      }
      return null;
    }

    // print schema
    changeSetDF.printSchema()

    // print data
    import scala.collection.JavaConversions._
    for( v <- changeSetDF.collectAsList()) println("operation:" + v.o)


    import spark.implicits._
    implicit val myObjEncoder: org.apache.spark.sql.Encoder[Date] = org.apache.spark.sql.Encoders.kryo[Date]
    changeSetDF.map(mapToStage).toDF().write.format("delta").mode("overwrite").save(args(1) + "/staging")

  }




  @SerialVersionUID(1000000L)
  case class LineItem(id: Int,
                      l_orderkey: Int,
                      l_partkey: Int,
                      l_suppkey: Int,
                      l_linenumber: Int,
                      l_quantity:Double,
                      l_extendedprice:Double,
                      l_discount:Double,
                      l_tax:Double,
                      l_returnflag:String,
                      l_linestatus:String,
                      l_shipdate:String,
                      l_commitdate:String,
                      l_receiptdate:String,
                      l_shipinstruct:String,
                      l_shipmode:String,
                      l_comment:String
                     ) extends Serializable


  @SerialVersionUID(10011000L)
  case class ChangeRecord(before: LineItem,after:LineItem,o:String)extends Serializable

  @SerialVersionUID(10022000L)
  case class StageRecord(is_update:Boolean,is_delete:Boolean,id:Int,cdcTime:String,record:LineItem)extends Serializable



}
