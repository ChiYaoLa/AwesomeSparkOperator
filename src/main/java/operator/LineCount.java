package main.java.operator;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;



//计算一个文本不重复的行数有多少

public class LineCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LineCount")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("test.txt");
        
        // 现在这个文本的元素是每一行，然后我们把每一行变成 line => (line ,1)
        JavaPairRDD<String, Integer> pairlines = lines.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String line) throws Exception {
				return new Tuple2<String, Integer>(line, 1);
			}
        });
        
        JavaPairRDD<String, Integer> results = pairlines.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
        
        results.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> result) throws Exception {
				System.out.println(result._1 + " : " + result._2);
			}
		});

        sc.close();
    }
}