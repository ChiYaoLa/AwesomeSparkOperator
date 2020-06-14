package main.java.sql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * JDBC数据源
 * 
 * @author Administrator
 *
 */
public class JDBCDataSource {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		 
		SQLContext sqlContext = new SQLContext(sc);
		// 方法1、分别将mysql中两张表的数据加载为DataFrame
		/*
		 * Map<String, String> options = new HashMap<String, String>();
		 * options.put("url", "jdbc:mysql://hadoop1:3306/testdb");
		 * options.put("driver", "com.mysql.jdbc.Driver"); 
		 * options.put("user","spark");
		 * options.put("password", "spark2016");
		 * options.put("dbtable", "student_info"); 
		 * DataFrame studentInfosDF = sqlContext.read().format("jdbc").options(options).load();
		 * 
		 * options.put("dbtable", "student_score"); 
		 * DataFrame studentScoresDF = sqlContext.read().format("jdbc") .options(options).load();
		 */
		// 方法2、分别将mysql中两张表的数据加载为DataFrame
		DataFrameReader reader = sqlContext.read().format("jdbc");
		reader.option("url", "jdbc:mysql://hadoop1:3306/testdb");
		reader.option("dbtable", "student_info");
		reader.option("driver", "com.mysql.jdbc.Driver");
		reader.option("user", "spark");
		reader.option("password", "spark2016");
		DataFrame studentInfosDF = reader.load();

		reader.option("dbtable", "student_score");
		DataFrame studentScoresDF = reader.load();
		// 将两个DataFrame转换为JavaPairRDD，执行join操作
		
		studentInfosDF.registerTempTable("studentInfos");
		studentScoresDF.registerTempTable("studentScores");
		
		String sql = "SELECT studentInfos.name,age,score "
				+ "		FROM studentInfos JOIN studentScores"
				+ "		 ON (studentScores.name = studentInfos.name)"
				+ "	 WHERE studentScores.score > 80";
		
		DataFrame resultDF = sqlContext.sql(sql);
		resultDF.show();
		
		//使用forachPartition来优化
		resultDF.javaRDD().foreach(new VoidFunction<Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Row row) throws Exception {
				String sql = "insert into good_student_info values(" + "'" + String.valueOf(row.getString(0)) + "',"
						+ Integer.valueOf(String.valueOf(row.get(1))) + ","
						+ Integer.valueOf(String.valueOf(row.get(2))) + ")";

				Class.forName("com.mysql.jdbc.Driver");

				Connection conn = null;
				Statement stmt = null;
				try {
					conn = DriverManager.getConnection("jdbc:mysql://hadoop1:3306/testdb", "spark", "spark2016");
					stmt = conn.createStatement();
					stmt.executeUpdate(sql);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if (stmt != null) {
						stmt.close();
					}
					if (conn != null) {
						conn.close();
					}
				}
			}

		}); 

		/**
		 * 将SparkContext 关闭，释放资源
		 */
		sc.close();
	}

}
