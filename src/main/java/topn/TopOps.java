package main.java.topn;

import java.util.List;

import javax.swing.plaf.basic.BasicBorders.SplitPaneBorder;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import breeze.linalg.split;
import scala.Tuple2;

/**
 * 按照Id进行排序，去ID最大的前3名
 * @author root
 *
 */
public class TopOps {
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
					.setMaster("local")
					.setAppName("TopOps");
		 JavaSparkContext sc = new JavaSparkContext(conf);
		 /**
		  * 一行行的数据
		  */
		 JavaRDD<String> linesRDD = sc.textFile("top.txt");
		 
		 JavaPairRDD<Integer, String> pairRDD = linesRDD.mapToPair(new PairFunction<String, Integer, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(String val) throws Exception {
				String[] splited = val.split(",");
				Integer userId = Integer.valueOf(splited[0]);
				return new Tuple2<Integer, String>(userId,val);
			}
		});
		 
		 JavaPairRDD<Integer, String> sortByKeyRDD = pairRDD.sortByKey(false);
		 List<Tuple2<Integer, String>> top3 = sortByKeyRDD.take(3);
		 for (Tuple2<Integer, String> tuple2 : top3) {
			System.out.println(tuple2._2);
		}
	}
}
