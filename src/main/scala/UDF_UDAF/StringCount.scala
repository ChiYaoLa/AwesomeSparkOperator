package main.scala.UDF_UDAF

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType

/**
 * @author Administrator
 */
class StringCount extends UserDefinedAggregateFunction {  
  
  //输入数据的类型
  def inputSchema: StructType = {
    StructType(Array(StructField("12321", StringType, true)))   
  }
  
  // 聚合操作时，所处理的数据的类型
  def bufferSchema: StructType = {
    StructType(Array(StructField("count", IntegerType, true)))   
  }
  

  def deterministic: Boolean = {
    true
  }

  // 为每个分组的数据执行初始化值
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }
  
  // 每个组，有新的值进来的时候，进行分组对应的聚合值的计算
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }
  
   
  // 最后merger的时候，在各个节点上的聚合值，要进行merge，也就是合并
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)  
  }
  
   // 最终函数返回值的类型
  def dataType: DataType = {
    IntegerType
  }
  
  // 最后返回一个最终的聚合值     要和dataType的类型一一对应
  def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)   
  }
  
}