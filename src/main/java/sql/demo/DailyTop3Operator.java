package main.java.sql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

public class DailyTop3Operator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("DailyTop3Keyword");
		JavaSparkContext sc = new JavaSparkContext(conf);
		HiveContext sqlContext = new HiveContext(sc);

		// 第一步：将数据加载到我们的RDD中
		JavaRDD<String> linesRdd = sc.textFile("hdfs://hadoop1:9000/input/log.txt");

		// 第二步 创建过滤的条件 我们只是分析 查询地点为北京和上海 的数据，
		Map<String, List<String>> filterParams = new HashMap<String, List<String>>();
		filterParams.put("city", Arrays.asList("北京", "上海"));

		// 第三步 过滤需要用到我们的过滤条件，那么为了性能更加好 我们使用广播变量
		final Broadcast<Map<String, List<String>>> filterParamsBroadcast = sc.broadcast(filterParams);

		// 第四步进行过滤操作
		JavaRDD<String> filterRDD = linesRdd.filter(new Function<String, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String log) throws Exception {

				String[] logSplited = log.split("\t");

				String city = logSplited[3];

				Map<String, List<String>> queryParamMap = filterParamsBroadcast.value();

				List<String> cities = queryParamMap.get("city");
				if (cities.size() > 0 && !cities.contains(city)) {
					return false;
				}

				return true;
			}
		});

		// 第五步：将数据变成(日期_搜索词,用户)格式
		JavaPairRDD<String, String> dateKeywordUserRDD = filterRDD.mapToPair(

				new PairFunction<String, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(String log) throws Exception {
						String[] splits = log.split("\t");

						String date = splits[0];
						String user = splits[2];
						String keyword = splits[1];

						return new Tuple2<String, String>(date + "_" + keyword, user);
					}

				});

		// 第六步:根据日期_搜索词来分组
		JavaPairRDD<String, Iterable<String>> dateKeywordUsersGroupRDD = dateKeywordUserRDD.groupByKey();

		// 第七步：对组内的重复用户去重复
		JavaPairRDD<String, Long> dateKeywordUvRDD = dateKeywordUsersGroupRDD.mapToPair(

				new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> dateKeywordUsers)
							throws Exception {
						String dateKeyword = dateKeywordUsers._1;
						Iterator<String> users = dateKeywordUsers._2.iterator();

						// 对用户进行去重，并统计去重后的数量
						List<String> distinctUsers = new ArrayList<String>();

						while (users.hasNext()) {
							String user = users.next();
							if (!distinctUsers.contains(user)) {
								distinctUsers.add(user);
							}
						}

						// 获取uv
						long uv = distinctUsers.size();

						return new Tuple2<String, Long>(dateKeyword, uv);
					}
				});
		
		
		// 第八步：将每天 每个 关键词的uv数据，转换成DataFrame
		JavaRDD<Row> dateKeywordUvRowRDD = dateKeywordUvRDD.map(

				new Function<Tuple2<String, Long>, Row>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Row call(Tuple2<String, Long> dateKeywordUv) throws Exception {
						String date = dateKeywordUv._1.split("_")[0];
						String keyword = dateKeywordUv._1.split("_")[1];
						long uv = dateKeywordUv._2;
						return RowFactory.create(date, keyword, uv);
					}
				});
		
		List<StructField> structFields = Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("keyword", DataTypes.StringType, true),
				DataTypes.createStructField("uv", DataTypes.LongType, true));
		StructType structType = DataTypes.createStructType(structFields);
		
		DataFrame dateKeywordUvDF = sqlContext.createDataFrame(dateKeywordUvRowRDD, structType);

		dateKeywordUvDF.registerTempTable("daily_keyword_uv");

		// 第九步：使用sparkSQL开窗函数，统计每天搜索uv排名前3的热点搜索词
		DataFrame dailyTop3KeywordDF = sqlContext.sql(""
				+ "SELECT date,keyword,uv "
				+ "FROM (" 
					+ "SELECT  "
						+ "date, "
						+ "keyword,"
						+ "uv,"
						+ "row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank "
					+ "FROM daily_keyword_uv" 
				+ ") tmp "
				+ "WHERE rank<=3");

		sqlContext.sql("USE result");

		sqlContext.sql("DROP TABLE IF EXISTS dailyTop3KeywordDF");

//		dailyTop3KeywordDF.saveAsTable("dailyTop3KeywordDF");
		dailyTop3KeywordDF.write().saveAsTable("dailyTop3KeywordDF");
		/*
		 * List<Tuple2<String,Long>> collect = dateKeywordUvRDD.collect(); for
		 * (Tuple2<String, Long> tuple2 : collect) {
		 * System.out.println(tuple2._1+"\t"+tuple2._2); }
		 */

	}
}