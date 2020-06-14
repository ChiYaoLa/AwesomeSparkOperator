package main.java.sql.createdf;

import java.util.ArrayList;
import java.util.List;

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

import breeze.linalg.Axis;
import scala.Tuple2;

public class SparkSQLwithJoin {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQLwithJoin");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlcontext = new SQLContext(sc);
		DataFrame peopleDF = sqlcontext.read().json("student.json");
		// 基于Json构建的DataFrame来注册临时表
		peopleDF.registerTempTable("peopleTable");
		// 查询出年龄大于xx的人
		DataFrame peopleScore = sqlcontext.sql("select name,score from peopleTable where score > 70");
		// 在DataFrame的基础上转化成RDD,通过Map操作计算出分数大于90的所有人的姓名
		List<String> peopleList = peopleScore.javaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return row.getAs("name");
			}
		}).collect();

		// 动态组拼出Json
		List<String> peopleInfomation = new ArrayList<String>();
		peopleInfomation.add("{\"name\":\"Michael\",\"age\":20}");
		peopleInfomation.add("{\"name\":\"Andy\",\"age\":17}");
		peopleInfomation.add("{\"name\":\"Justin\",\"age\":19}");

		// 通过内容为Json的RDD来构造DataFrame
		JavaRDD<String> peopleInfomationRDD = sc.parallelize(peopleInfomation);
		DataFrame peopleInfomationDF = sqlcontext.read().json(peopleInfomationRDD);
		peopleInfomationDF.registerTempTable("peopleInfomation");

		String sql = "select name,age from peopleInfomation where name in (";
		for (String string : peopleList) {
			sql += "'" + string + "',";
		}
		sql = sql.substring(0, sql.length() - 1);
		sql += ")";
		DataFrame execellentNameAgeDF = sqlcontext.sql(sql);
		JavaPairRDD<String, Tuple2<Integer, Integer>> resultRDD = peopleScore.javaRDD()
				.mapToPair(new PairFunction<Row, String, Integer>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Row row) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Integer>(String.valueOf(row.getAs("name")),
								Integer.valueOf(String.valueOf(row.getAs("score"))));
					}
				}).join(execellentNameAgeDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Row row) throws Exception {
						return new Tuple2<String, Integer>(String.valueOf(row.getAs("name")),
								Integer.valueOf(String.valueOf(row.getAs("age"))));
					}
				}));
		
		JavaRDD<Row> resultRowRDD = resultRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
				// TODO Auto-generated method stub
				return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
			}
		});
		
		/**
		 * 动态构造DataFrame的元数据，一般而言，有多少列以及每列的具体类型可能来自于Json，也可能来自于DB
		 */
		ArrayList<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		//构建StructType，用于最后DataFrame元数据的描述
		StructType structType = DataTypes.createStructType(structFields);
		
		/**
		 * 基于已有的MetaData以及RDD<Row> 来构造DataFrame
		 */
		DataFrame df = sqlcontext.createDataFrame(resultRowRDD, structType);
		df.show();
		
		//方法2：直接在sql语句中join
		DataFrame sql2 = sqlcontext.sql("select * from peopleTable,peopleInfomation where peopleInfomation.name = peopleTable.name and peopleTable.score>70");
		sql2.show();
	}
}
