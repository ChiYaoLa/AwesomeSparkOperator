package main.scala.sql.hive

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
 * @author Administrator
 */
object HiveDataSource {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("HiveDataSource");
    val sc = new SparkContext(conf);
    val hiveContext = new HiveContext(sc);
    hiveContext.sql("DROP TABLE IF EXISTS student_infos");
    hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT) row format delimited fields terminated by '\t'");
    hiveContext.sql("LOAD DATA "
      + "LOCAL INPATH '/root/resource/student_infos' "
      + "INTO TABLE student_infos");

    hiveContext.sql("DROP TABLE IF EXISTS student_scores");
    hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT) row format delimited fields terminated by '\t'");
    hiveContext.sql("LOAD DATA "
      + "LOCAL INPATH '/root/resource/student_scores' "
      + "INTO TABLE student_scores");

    val goodStudentsDF = hiveContext.sql("SELECT si.name, si.age, ss.score "
      + "FROM student_infos si "
      + "JOIN student_scores ss ON si.name=ss.name "
      + "WHERE ss.score>=80");

    hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");
//    goodStudentsDF.saveAsTable("good_student_infos");
    hiveContext.sql("USE result")
    
    //将goodStudentsDF里面的值写入到Hive表中，如果表不存在，会自动创建然后将数据插入到表中
    goodStudentsDF.write.saveAsTable("good_student_infos")

  }
}