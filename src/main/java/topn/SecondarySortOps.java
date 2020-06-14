package main.java.topn;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 思路：
	1、将第一个字段和第二个字段封装到一个自定义的对象，自定义的对象应该作为Key
	2、使用sortByKey进行排序
 * @author root
 *
 */
public class SecondarySortOps {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("TopOps");
	 JavaSparkContext sc = new JavaSparkContext(conf);
	 JavaRDD<String> linesRDD = sc.textFile("secondSort.txt");
	 
	 JavaPairRDD<SecondSortKey, String> pairRDD = linesRDD.mapToPair(new PairFunction<String, SecondSortKey, String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<SecondSortKey, String> call(String val) throws Exception {
			SecondSortKey secondSortKey = new SecondSortKey(Integer.valueOf(val.split(" ")[0]), Integer.valueOf(val.split(" ")[1]));
			return new Tuple2<SecondSortKey, String>(secondSortKey, val);
		}
	});
	 
	 
	 JavaPairRDD<SecondSortKey, String> sortByKey = pairRDD.sortByKey();
	 List<Tuple2<SecondSortKey, String>> collect = sortByKey.collect();
	 
	 for (Tuple2<SecondSortKey, String> tuple2 : collect) {
		 System.out.println(tuple2._2);
	}
	 
	 
	 
	}
}
