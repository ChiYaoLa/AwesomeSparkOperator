package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TakeSample {

	// takeSample = take + sample
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SampleOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> names = Arrays
				.asList("xuruyun", "liangyongqi", "wangfei","xuruyun");
		JavaRDD<String> nameRDD = sc.parallelize(names,1);
		
		List<String> list = nameRDD.takeSample(false,2);
		for(String name :list){
			System.out.println(name);
		}
	}
}
