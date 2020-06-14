package main.java.sql.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class UDF {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setAppName("UDF")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		SQLContext sqlContext = new SQLContext(sc);
		
		List<String> list = new ArrayList<String>();

		list.add("yarn");
		list.add("Marry");
		list.add("Jack");
		list.add("To	m");
		list.add("Tom");
	
		JavaRDD<String> nameRdd = sc.parallelize(list);
		
		
		JavaRDD<Row> rowRdd = nameRdd.map(new Function<String, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String name) throws Exception {
				return RowFactory.create(name);
			}
		});
		
		
		
		ArrayList<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		StructType structType = DataTypes.createStructType(structFields);
		
		DataFrame nameDF = sqlContext.createDataFrame(rowRdd, structType);
		
		nameDF.registerTempTable("nameTable");
		
		 /**
		  * 根据UDF函数参数的个数来决定是实现哪一个UDF  UDF1，UDF2。。。。UDF1xxx
		  */
		/*sqlContext.udf().register("strLen", new UDF1<String,Integer>() {

			*//**
			 * 
			 *//*
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(String str) throws Exception {
				return str.length();
			}
		}, DataTypes.IntegerType);*/
		
		sqlContext.udf().register("strLen", new UDF2<String, Integer, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String t1, Integer t2) throws Exception {
				Random random = new Random();
				return t1.length()+random.nextInt(t2)+"~";
			}
		}, DataTypes.StringType);
		 
		
		sqlContext.sql("SELECT name,strLen(name,10) from nameTable").show();
		
	}
}
