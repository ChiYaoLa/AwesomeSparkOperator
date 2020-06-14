package main.java.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastTest {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LineCount")
                .setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
          final int f = 10;
        
//        List<String> broadList = new ArrayList<String>();
//        broadList.add("hello");
//        broadList.add("bjsxt");
//        broadList.add("shsxt");
//        broadList.add("laoxiao");
//        broadList.add("laogao");
//        broadList.add("xiaogao");
        
          final Broadcast<Integer> broadcast = sc.broadcast(f);
//        final Broadcast<List<String>> broadcast = sc.broadcast(broadList);
        
        
        List<Integer> numbers = Arrays.asList(0,1,2,3,4,5);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        
        // map对每个元素进行操作
        JavaRDD<Integer> results = numberRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer number) throws Exception {
//				return number*f;
				return broadcast.getValue()*number;
//				return broadcast.getValue().get(number).equals("hello")?1:0;
			}
		});
        
       
        
        results.foreach(new VoidFunction<Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Integer result) throws Exception {
				System.out.println(result);
				
			}
		});
        
        sc.close();
	}
}
