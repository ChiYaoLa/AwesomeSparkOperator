package main.java.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.base.Optional;

import scala.Tuple2;

/**
 * 过滤黑名单
 * 
 * @author zfg
 *
 */
public class TransformOperator {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TransformBlacklist");
		final JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

		// 先做一份模拟的黑名单RDD
		List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
		blacklist.add(new Tuple2<String, Boolean>("tom", true));
		final JavaPairRDD<String, Boolean> blacklistRDD = jssc.sparkContext().parallelizePairs(blacklist);
		
		// 这里的日志格式，就简化一下，就是date username的方式
		JavaReceiverInputDStream<String> adsClickLogDStream = jssc.socketTextStream("hadoop1", 8888);

		JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLogDStream.mapToPair(

				new PairFunction<String, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(String adsClickLog) throws Exception {
						return new Tuple2<String, String>(adsClickLog.split(" ")[1], adsClickLog);
					}
				});
				JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
						
						
						/**
						 * 使用transform将DStream里面的RDD抽取出来后，调用了RDD的action类算子
						 * 	List<Tuple2<String, String>> collect = userAdsClickLogRDD.collect();
							for (Tuple2<String, String> tuple2 : collect) {
								System.out.println("transform result:"+tuple2);
							}
						 */
					
						JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joined = userAdsClickLogRDD.leftOuterJoin(blacklistRDD);	
						
						JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filtered = joined.filter(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, Boolean>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
								Optional<Boolean> option = tuple._2._2;
								if(option.isPresent()){
									return !option.get();
								}
								return true;
							}
						});

						return filtered.map(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, String>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							@Override
							public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
								return tuple._2._1;
							}
						});
					}

				});
		validAdsClickLogDStream.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}

}
