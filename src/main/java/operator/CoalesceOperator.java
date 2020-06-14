package main.java.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * coalesce(int numPartition,boolean shuffle)
 * reaprtition(int numPartitions) = coalesce(numPartitions,true) 
 * @author root
 *
 */

public class CoalesceOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CoalesceOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// coalesce算子，功能是将RDD的partition的数量缩减，减少！！！
		// 将一定的数据压缩到更少的partition分区中去
		// 使用场景！很多时候在filter算子应用之后会优化一下使用coalesce算子
		// filter 算子应用到RDD上面，说白了会应用到RDD对应的里面的每个partition上去
		// 数据倾斜，换句话说就是有可能有的partition里面就剩下了一条数据！
		// 建议使用coalesce算子，从而让各个partition中的数据都更加的紧凑！！
		
		// 公司原先有6个部门
		// 但是呢，可不巧，碰到了公司的裁员！裁员以后呢，有的部门中的人员就没有了！
		// 数据倾斜，不同的部门人员不均匀，做一个部门的整合的操作，将不同的部门的员工进行压缩！
		
		List<String> staffList = Arrays.asList("xuruyun1","xuruyun2","xuruyun3"
				,"xuruyun4","xuruyun5","xuruyun6"
				,"xuruyun7","xuruyun8","xuruyun9"
				,"xuruyun10","xuruyun11","xuruyun12"
				,"xuruyun13","xuruyun14","xuruyun15"
				,"xuruyun16","xuruyun17","xuruyun18"
				,"xuruyun19","xuruyun20","xuruyun21"
				,"xuruyun22","xuruyun23","xuruyun24");
		
		JavaRDD<String> staffRDD = sc.parallelize(staffList, 6);
		JavaRDD<String> staffRDD2 = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator)
					throws Exception {
				List<String> list = new ArrayList<String>();
				while(iterator.hasNext()){
					String staff = iterator.next();
					list.add("部门["+(index)+"]"+staff);
				}
				return list.iterator();
			}
		}, true);
				
		for(String staffInfo : staffRDD2.collect()){
			System.out.println(staffInfo);
		}
		
		
		 
 	 	JavaRDD<String> staffRDD3 = staffRDD2.coalesce(3,false);
	 //	JavaRDD<String> staffRDD3 = staffRDD2.repartition(12);
		
 	 	System.out.println("staffRDD3.partitions().size():"+staffRDD3.partitions().size());
		
		JavaRDD<String> staffRDD4 = staffRDD3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(Integer index, Iterator<String> iterator)
					throws Exception {
				List<String> list = new ArrayList<String>();
				while(iterator.hasNext()){
					String staff = iterator.next();
					list.add("部门["+(index)+"]"+staff);
				}
				return list.iterator();
			}
		}, true);
		for(String staffInfo : staffRDD4.collect()){
			System.out.println(staffInfo);
		}  
	}
}
