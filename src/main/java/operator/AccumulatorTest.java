package main.java.operator;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class AccumulatorTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LineCount")
                .setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        final Accumulator<Integer> accumulator = sc.accumulator(0);
        
        List<Integer> numbers = Arrays.asList(0,1,2,3,4,5);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        
        // map对每个元素进行操作
        JavaRDD<Integer> results = numberRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer number) throws Exception {
				System.out.println(accumulator.value());
				accumulator.add(number);
				return number;
			}
		});
        
       
        
        results.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer result) throws Exception {
				System.out.println(result);
				
			}
		});
        
        System.out.println(accumulator.value());
        sc.close();
	}
}
