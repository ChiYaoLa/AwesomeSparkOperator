package main.java.broadcast;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class BroadCastTest {
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.setAppName("BroadCastTest");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		List<String> userNames = Arrays.asList(
				"xuruyun",
				"laochen",
				"liangyongqi",
				"xiaolaoshi",
				"panglaoshi"
				);
		JavaRDD<String> userNameRDD = sc.parallelize(userNames);
		
		/**
		 * 广播变量必须在Driver端定义 
		 */
		final Broadcast<String> blackNameBroadcast = sc.broadcast("xuruyun");
		
		
		JavaRDD<String> filteredUserNameRDD = userNameRDD.filter(new Function<String, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String userName) throws Exception {
				String blackName = blackNameBroadcast.value();
				return !blackName.equals(userName);
			}
		});
				
		List<String> collect = filteredUserNameRDD.collect();
		for (String string : collect) {
			System.out.println(string);
		}
		
		sc.stop();
	}
}
