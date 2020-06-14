package main.java.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

import scala.Tuple2;

public class SparkStreamingOnHDFSToMySQL {
	public static void main(String[] args) {
	 
		final SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("SparkStreamingOnHDFS");
		// JavaStreamingContext jsc = new JavaStreamingContext(conf,
		// Durations.seconds(5));
		final String checkpointDirectory = "hdfs://hadoop1:9000/library/SparkStreaming/CheckPoint_Data_04018";
		JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
			@Override
			public JavaStreamingContext create() {
				return createContext(checkpointDirectory,conf);
			}
		};
		 
		JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkpointDirectory, factory);
		jsc.start();
		jsc.awaitTermination();
		// jsc.close();
	}

	private static JavaStreamingContext createContext(String checkpointDirectory,SparkConf conf) {

		// If you do not see this printed, that means the StreamingContext has
		// been loaded
		// from the new checkpoint
		System.out.println("Creating new context");
		SparkConf sparkConf = conf;
		// Create the context with a 1 second batch size
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
		ssc.checkpoint(checkpointDirectory);
		
		JavaDStream<String> lines = ssc.textFileStream("hdfs://hadoop1:9000/output/");
		 
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(s.split(" "));
			}
		});

		JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		counts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
			
			private static final long serialVersionUID = 1L;
			
			@Override
			public void call(JavaPairRDD<String, Integer> pairRdd) throws Exception {
				
				pairRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {
					 
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<Tuple2<String, Integer>> vs) throws Exception {
				  		
						JDBCWrapper jdbcWrapper = JDBCWrapper.getJDBCInstance();
					
						List<Object[]> insertParams = new ArrayList<Object[]>();
						while(vs.hasNext()){
							Tuple2<String, Integer> next = vs.next();
							insertParams.add(new Object[]{next._1,next._2});
						}
						System.out.println(insertParams);
						jdbcWrapper.doBatch("INSERT INTO wordcount VALUES(?,?)", insertParams);
					}
				});
				
			}
		});
		counts.print();
		return ssc;
	}

}
