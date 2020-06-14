package main.java.broadcast;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class BroadCastOperator {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.setAppName("BroadCastOperator");
		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> linesRDD = sc.textFile("cs");
		String blackName = "Spark core";
		final Broadcast<String> broadcast = sc.broadcast(blackName);
		
		
		JavaRDD<String> filtered = linesRDD.filter(new Function<String, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				String blackName = broadcast.value();
				return !blackName.equals(v1);
			}
		});
		
		List<String> collect = filtered.collect();
		for (String string : collect) {
			System.out.println(string);
		}
		/**
		 * 关闭掉SparkContext  释放资源
		 */
		sc.close();
	}
}
