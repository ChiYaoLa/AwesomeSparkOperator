package main.java.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class RepartitionOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("RepartitionOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
	 
		// 一个很经典的场景，使用Spark SQL从HIVE中查询数据时候，spark SQL会根据HIVE
		// 对应的hdfs文件的block的数量决定加载出来的RDD的partition有多少个！
		// 这里默认的partition的数量是我们根本无法设置的
		
		// 有些时候，可能它会自动设置的partition的数量过于少了，为了进行优化
		// 可以提高并行度，就是对RDD使用repartition算子！
		
		// 公司要增加部门
		List<String> staffList = Arrays.asList("xuruyun1","xuruyun2","xuruyun3"
				,"xuruyun4","xuruyun5","xuruyun6"
				,"xuruyun7","xuruyun8","xuruyun9"
				,"xuruyun10","xuruyun11","xuruyun12","xuruyun13");
		JavaRDD<String> staffRDD = sc.parallelize(staffList, 3);
		JavaRDD<String> staffRDD2 = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator)
					throws Exception {
				List<String> list = new ArrayList<String>();
				while(iterator.hasNext()){
					String staff = iterator.next();
					list.add("部门["+(index+1)+"]"+staff);
				}
				return list.iterator();
			}
		}, true);
		for(String staffInfo : staffRDD2.collect()){
			System.out.println(staffInfo);
		}
		
		 JavaRDD<String> staffRDD3 = staffRDD2.repartition(2);
		
		JavaRDD<String> staffRDD4 = staffRDD3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator)
					throws Exception {
				List<String> list = new ArrayList<String>();
				while(iterator.hasNext()){
					String staff = iterator.next();
					list.add("部门["+(index+1)+"]"+staff);
				}
				return list.iterator();
			}
		}, true);
		for(String staffInfo : staffRDD4.collect()){
			System.out.println(staffInfo);
		} 
		
		sc.close();
	}
}
