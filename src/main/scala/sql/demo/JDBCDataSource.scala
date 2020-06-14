package main.scala.sql.demo

import java.util.HashMap

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * @author liut
 */
object JDBCDataSource{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    var options = new HashMap[String, String]();
    options.put("url", "jdbc:mysql://hadoop1:3306/testdb");
    options.put("user", "spark");
    options.put("password", "spark2016");

    options.put("dbtable", "student_info");
    var studentInfosDF = sqlContext.read.format("jdbc").options(options).load()

    options.put("dbtable", "student_score");
    var studentScoresDF = sqlContext.read.format("jdbc").options(options).load()

    
    studentInfosDF.registerTempTable("student_info")
    studentScoresDF.registerTempTable("student_score")
    
    
    
    
    val sql = "SELECT student_info.name,student_info.age,student_score.score"
              .+(" FROM student_info JOIN student_score ON (student_info.name = student_score.name)")
              .+(" WHERE student_score.score  > 80")
    sqlContext.sql(sql).show()
    

    // 将DataFrame数据保存到MySQL表中
   /* studentsDF.foreach { row =>
      {
        var sql = "insert into good_student_info values(".+("'").+(row.getString(0)).+("',").+(row.getInt(1)).+(",").+(row.getInt(2)).+(")")
        //println(sql)
        Class.forName("com.mysql.jdbc.Driver");
        var conn = DriverManager.getConnection("jdbc:mysql://hadoop1:3306/testdb", "spark", "spark2016");
        var stat = conn.createStatement();
        stat.executeUpdate(sql);

        if (stat != null) {
          stat.close();
        }
        if (conn != null) {
          conn.close();
        }
      }
    }*/
  }
}