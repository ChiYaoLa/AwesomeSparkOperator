package main.java.sql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * JSON数据源
 * @author Administrator
 *
 */
public class JSONDataSource {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("JSONDataSource")
//				.set("spark.default.parallelism", "100")
				.setMaster("local");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame studentScoresDF = sqlContext.read().json("student.json");  
		
		studentScoresDF.registerTempTable("student_scores");
		DataFrame goodStudentScoresDF = sqlContext.sql(
				"select name,count(score) from student_scores where score>=80 group by name");
		
		List<String> goodStudentNames = goodStudentScoresDF.javaRDD().map(
				
				new Function<Row, String>() {
					
					private static final long serialVersionUID = 1L;
		
					@Override
					public String call(Row row) throws Exception {
						return row.getString(0);
					}
					
				}).collect();
		
		List<String> studentInfoJSONs = new ArrayList<String>();
		studentInfoJSONs.add("{\"name\":\"Michael\", \"age\":18}");  
		studentInfoJSONs.add("{\"name\":\"Andy\", \"age\":17}");  
		studentInfoJSONs.add("{\"name\":\"Justin\", \"age\":19}");
		JavaRDD<String> studentInfoJSONsRDD = sc.parallelize(studentInfoJSONs);
		DataFrame studentInfosDF = sqlContext.read().json(studentInfoJSONsRDD);
		
		studentInfosDF.registerTempTable("student_infos");  
		
		String sql = "select name,age from student_infos where name in (";
		for(int i = 0; i < goodStudentNames.size(); i++) {
			sql += "'" + goodStudentNames.get(i) + "'";
			if(i < goodStudentNames.size() - 1) {
				sql += ",";
			}
		}
		sql += ")";
		
		DataFrame goodStudentInfosDF = sqlContext.sql(sql);
		
		JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentsRDD = 
				
				goodStudentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<String, Integer> call(Row row) throws Exception {
						return new Tuple2<String, Integer>(row.getString(0), 
								Integer.valueOf(String.valueOf(row.getLong(1))));  
					}
					
				}).join(goodStudentInfosDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
		
					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<String, Integer> call(Row row) throws Exception {
						return new Tuple2<String, Integer>(row.getString(0),
								Integer.valueOf(String.valueOf(row.getLong(1))));   
					}
					
				}));
		
		JavaRDD<Row> goodStudentRowsRDD = goodStudentsRDD.map(
				
				new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Row call(
							Tuple2<String, Tuple2<Integer, Integer>> tuple)
							throws Exception {
						return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
					}
					
				});
		
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true)); 
		structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));  
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));  
		StructType structType = DataTypes.createStructType(structFields);
		
		DataFrame goodStudentsDF = sqlContext.createDataFrame(goodStudentRowsRDD, structType);
		
		goodStudentsDF.show();
//		goodStudentsDF.write().format("json").save("hdfs://hadoop1:9000/output/good-students");  
	}
	
}
