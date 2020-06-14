package main.java.operator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CountByKeyOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CountByKeyOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<String,String>> scoreList = Arrays.asList(
				new Tuple2<String,String>("70s","xuruyun"),
				new Tuple2<String,String>("70s","wangfei"),
				new Tuple2<String,String>("90s","wangfei"),
				new Tuple2<String,String>("80s","yasaka"),
				new Tuple2<String,String>("80s","zhengzongwu"),
				new Tuple2<String,String>("80s","lixin"));
		JavaPairRDD<String,String> students = sc.parallelizePairs(scoreList);
		
		// 对RDD应用CountByKey算子，统计每个70s 或者 80s，人数分别是多少
		// 说白了就是统计每种Key对应的元素个数！
		Map<String, Object> counts = students.countByKey();
		for(Entry<String, Object> studentCount : counts.entrySet()){
			System.out.println(studentCount.getKey() + ": " +  studentCount.getValue());
		}
		
		Map<Tuple2<String, String>, Long> countByValue = students.countByValue();
		Set<Entry<Tuple2<String, String>, Long>> entrySet = countByValue.entrySet();
		for (Entry<Tuple2<String, String>, Long> entry : entrySet) {
			System.out.println(entry.getKey()+"===="+entry.getValue());
		}
		
		sc.close();
	}
}
