package main.java.sql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Hive数据源
 * @author Administrator
 *
 */
public class HiveDataSource {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("HiveDataSource")
				.setMaster("local");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		//是SQLContext的子类。
		SQLContext hiveContext = new HiveContext(sc);
		//删除hive中的student_infos表
		hiveContext.sql("DROP TABLE IF EXISTS student_infos");
		
		//在hive中创建student_infos表
		hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT) row format delimited fields terminated by '\t'");
		
		hiveContext.sql("LOAD DATA "
				+ "LOCAL INPATH '/root/resource/student_infos' "
				+ "INTO TABLE student_infos");
		
		hiveContext.sql("DROP TABLE IF EXISTS student_scores"); 
		hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT) row format delimited fields terminated by '\t'");  
		hiveContext.sql("LOAD DATA "
				+ "LOCAL INPATH '/root/resource/student_scores'"
				+ "INTO TABLE student_scores");
		
		
		DataFrame goodStudentsDF = hiveContext.sql("SELECT si.name, si.age, ss.score "
				+ "FROM student_infos si "
				+ "JOIN student_scores ss ON si.name=ss.name "
				+ "WHERE ss.score>=80");
		
		hiveContext.sql("USE result");
		
		hiveContext.sql("DROP TABLE IF EXISTS good_student_infos");  
		
		goodStudentsDF.write().saveAsTable("good_student_infos");
		
		Row[] goodStudentRows = hiveContext.table("good_student_infos").collect();  
		for(Row goodStudentRow : goodStudentRows) {
			System.out.println(goodStudentRow);  
		}
		sc.close();
	}
	
}
