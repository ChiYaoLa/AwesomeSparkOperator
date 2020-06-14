package main.java.topn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;


/**
 * 
思路：
	java:mapToPair / scala:map
	groupByKey  class1 [100,101,88,99]  
		List  Collections.sort(list) 
		 有什么问题？  某一个key对应的value 有可能非常非常的多，放到list里面会有OOM的风险
			解决办法：
				定义一个定长的数组，通过一个简单的算法
											
 * @author root
 *
 */
public class GroupByKeyOps {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.setAppName("TopOps");
	 JavaSparkContext sc = new JavaSparkContext(conf);
	 JavaRDD<String> linesRDD = sc.textFile("scores.txt");
	 
	 JavaPairRDD<String, Integer> pairRDD = linesRDD.mapToPair(new PairFunction<String, String, Integer>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Integer> call(String str) throws Exception {
			String[] splited = str.split("\t");
			String clazzName = splited[0];
			Integer score = Integer.valueOf(splited[1]);
			return new Tuple2<String, Integer> (clazzName,score);
		}
	});
	 //class1 [1,2,3,4,5]
	 pairRDD.groupByKey().foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
			String clazzName = tuple._1;
			Iterator<Integer> iterator = tuple._2.iterator();
			
			Integer[] top3 = new Integer[3];
			
			while (iterator.hasNext()) {
				Integer score = iterator.next();
				
				for (int i = 0; i < top3.length; i++) {
					if(top3[i] == null){
						top3[i] = score;
						break;
					}else if(score > top3[i]){
						for (int j = 2; j > i; j--) {
							top3[j] = top3[j-1];
						}
						top3[i] = score;
						break;
					}
				}
			}
			System.out.println("class Name:"+clazzName);
			for(Integer sscore : top3){
				System.out.println(sscore);
			}
		}
	});;
	 
	 
	 
	 
	}
}
