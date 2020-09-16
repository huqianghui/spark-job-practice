package com.practice.delta.bean

object CdcOperationEnum{

  object  CdcOperation extends  Enumeration{

    type CdcOperation = Value

    val d,i,u = Value

  }

  def isDeleting(operation:String): Boolean ={
      operation.equals(CdcOperationEnum.CdcOperation.d.toString)
  }

  def isUpdating(operation:String): Boolean ={
    operation.equals(CdcOperationEnum.CdcOperation.u.toString)
  }

  def isInserting(operation:String): Boolean ={
    operation.equals(CdcOperationEnum.CdcOperation.i.toString)
  }


}



