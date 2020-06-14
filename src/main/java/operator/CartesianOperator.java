package main.java.operator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


public class CartesianOperator {

	public static void main(String[] args) {
		// 中文名 笛卡尔乘积
		// 比如说两个RDD，分部有10条数据，用了cartesian算子以后
		// 两个RDD的每一个数据都会和另外一个RDD的每一条数据执行一次JOIN
		// 最终组成一个笛卡尔乘积
		
		// 小案例
		// 比如说，现在有5件衣服，5条裤子，分部属于两个RDD
		// 就是说，需要对每件衣服都和每条裤子做一次JOIN操作，尝试进行服装搭配！
		SparkConf conf = new SparkConf().setAppName("CartesianOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> clothes = Arrays.asList("T恤衫","夹克","皮大衣","衬衫","毛衣");
		List<String> trousers = Arrays.asList("西裤","内裤","铅笔裤","皮裤","牛仔裤");
		JavaRDD<String> clothesRDD = sc.parallelize(clothes);
		JavaRDD<String> trousersRDD = sc.parallelize(trousers);
		
		JavaPairRDD<String,String> pairs = clothesRDD.cartesian(trousersRDD);
		
		for(Tuple2<String,String> pair : pairs.collect()){
			System.out.println(pair);
		}
		
		sc.close();
	}
}
