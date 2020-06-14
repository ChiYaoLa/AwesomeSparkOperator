package main.java.sql.demo;

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

import java.util.*;


public class DailySale {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DataFrameOps").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		List<String> userSaleLog = Arrays.asList(
				"2016-9-01,55,1122"
				,"2016-9-01,55,1133"
				,"2016-9-01,15,"
				,"2016-9-02,56,1144"
				,"2016-9-02,78,1155"
				,"2016-9-03,113,1123");
		
		JavaRDD<String> userSaleLogRdd = sc.parallelize(userSaleLog);
		
		JavaRDD<String> filteredUserSaleLogRDD = userSaleLogRdd.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String v1) throws Exception {
				return v1.split(",").length==3?true:false;
			}
		});

		
		JavaRDD<Row> userSaleLogRowRDD = filteredUserSaleLogRDD.map(new Function<String, Row>() {

			@Override
			public Row call(String v1) throws Exception {
				String[] split = v1.split(",");
				
				return RowFactory.create(String.valueOf(split[0]),Integer.valueOf(split[1]));
			}
		});

		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("sale_amount", DataTypes.IntegerType, true));
		StructType structType = DataTypes.createStructType(structFields);
		
		
		DataFrame userSaleDF = sqlContext.createDataFrame(userSaleLogRowRDD, structType);
		
		userSaleDF.registerTempTable("sale");
		userSaleDF.show();
		
		Map<String, String> exprs = new HashMap<String, String>();
		exprs.put("sale_amount", "sum");
		userSaleDF.groupBy(userSaleDF.col("date")).agg(exprs).show();
		
//		userSaleDF.groupBy(userSaleDF.col("date")).sum("sale_amount").show();
		
		String sql = "SELECT date,sum(sale_amount) count FROM sale GROUP BY date";
		 sqlContext.sql(sql).show();;
		
		
		
	}
}
