package main.java.sql.createdf;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class RDD2DataFrameByProgrammatically {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameByProgrammatically");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlcontext = new SQLContext(sc);
		/**
		 * 在RDD的基础上创建类型为Row的RDD
		 */
		JavaRDD<String> lines = sc.textFile("Peoples.txt");
		JavaRDD<Row> rowRDD = lines.map(new Function<String, Row>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String line) throws Exception {
				String[] split = line.split(",");
				return RowFactory.create(Integer.valueOf(split[0]),split[1],Integer.valueOf(split[2]));
			}
		});
		
		/**
		 * 动态构造DataFrame的元数据，一般而言，有多少列以及每列的具体类型可能来自于Json，也可能来自于DB
		 */
		ArrayList<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		//构建StructType，用于最后DataFrame元数据的描述
		StructType schema = DataTypes.createStructType(structFields);
		
		/**
		 * 基于已有的MetaData以及RDD<Row> 来构造DataFrame
		 */
		DataFrame df = sqlcontext.createDataFrame(rowRDD, schema);
		
		/**
		 *注册成为临时表以供后续的SQL操作查询
		 */
		df.registerTempTable("persons");
		
		/**
		 * 进行数据的多维度分析
		 */
		DataFrame result = sqlcontext.sql("select * from persons where age > 7");
		result.show();

		/**
		 * 对结果进行处理，包括由DataFrame转换成为RDD<Row>
		 */
	     List<Row> listRow = result.javaRDD().collect();
		 for (Row row : listRow) {
			System.out.println(row);
		}
	}
}
