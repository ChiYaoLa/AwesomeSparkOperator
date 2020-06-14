package main.java.broadcast;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * 定义在Driver端，Dirver可以读取值，操作在Executor端，在Executor是不能读取数据的。
 * @author root
 *
 */
public class AccumulatorOperator {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setMaster("local")
				.setAppName("BroadCastOperator");
		final JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> linesRDD = sc.textFile("cs");
		final Accumulator<Integer> countAccumulator = sc.accumulator(0);
		
		linesRDD.map(new Function<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(String v1) throws Exception {
				countAccumulator.add(1);
				System.out.println(countAccumulator.value());
				return v1;
			}
		}).collect();
		
		System.out.println(countAccumulator.value());
		
		
		
		sc.stop();
	}
}	
