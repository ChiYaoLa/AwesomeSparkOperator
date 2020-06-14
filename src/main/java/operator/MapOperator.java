package main.java.operator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

// 理解里面final使用的原因！

public class MapOperator {

	public static void main(String[] args) {
		/**
		 * SparkConf是设置spark运行时的环境变量，可以设置setMaster  setAppName，设置运行时所需要的资源情况
		 */
		SparkConf conf = new SparkConf().setAppName("MapOperator")
				.setMaster("local");
		//SparkContext非常的重要，SparkContext是通往集群的唯一通道，在SparkContext初始化的时候会创建任务调度器
		JavaSparkContext sc = new JavaSparkContext(conf);
//		sc.addJar("D:\\Jar\\MapOperator.jar");
		// 准备一下数据
		List<String> names = Arrays.asList("xurunyun","liangyongqi","wangfei");
		
		JavaRDD<String> nameRDD = sc.parallelize(names,2);
		
	 
		
		
		
	 	Map<String, Integer> scoreMap = new HashMap<String, Integer>();
		scoreMap.put("xurunyun", 150);
		scoreMap.put("liangyongqi", 100);
		scoreMap.put("wangfei", 90);

		/*	final Broadcast<Map<String, Integer>> mapBroadcast = sc.broadcast(scoreMap);
		
		final Accumulator<Integer> accumulator = sc.accumulator(0); */
		
		 JavaRDD<String> scoreRDD =nameRDD.map(new Function<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1) throws Exception {
				
					//数据库连接
//				Map<String, Integer> scoreMap = mapBroadcast.value();
//				Integer score = scoreMap.get(v1);
//				accumulator.add(score);
//				System.out.println(accumulator.value());
				String path = SparkFiles.get("spark-env.sh");
				@SuppressWarnings("resource")
				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
				StringBuilder str = new StringBuilder();
				String line = br.readLine();
				while(line != null ){
					str.append(line);
					line = br.readLine();
				}
				
				return "hello "+v1+ "_" + str;
			}
		});
		
		scoreRDD.foreach(new VoidFunction<String>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(String score) throws Exception {
				System.out.println(score);
				
				
			}
		});
		List<String> collect = scoreRDD.collect();
		for (String string : collect) {
			System.out.println(string);
		}
 		while(true){}
//		sc.stop();
//		System.out.println("accumulator:"+accumulator.value());
	}
}
