package main.java.operator;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SortByOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("SortByKeyOperator")
				.set("spark.shuffle.manager", "hash")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> list = Arrays.asList("zhangsan 100 160","wangwu 90 175","maliujiu 100 186");
		
		JavaRDD<String> linesRDD = sc.parallelize(list);
		
		JavaPairRDD<FacePower, String> mapToPair = linesRDD.mapToPair(new PairFunction<String, FacePower, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<FacePower, String> call(String t) throws Exception {
				String[] split = t.split(" ");
				return new Tuple2<FacePower, String>(new FacePower(Integer.parseInt(split[1]), Integer.parseInt(split[2])), split[0]);
			}
		});
		
		JavaPairRDD<FacePower, Tuple2<FacePower, String>> keyBy = mapToPair.keyBy(new Function<Tuple2<FacePower,String>, FacePower>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public FacePower call(Tuple2<FacePower, String> v1) throws Exception {
				return v1._1;
			}
		});
		JavaRDD<Tuple2<FacePower, String>> values = keyBy.sortByKey().values();
		
		values.foreach(new VoidFunction<Tuple2<FacePower,String>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;
			@Override
			public void call(Tuple2<FacePower, String> t) throws Exception {
				 System.out.println(t._1.toString()+"==="+t._2);
			}
		});
		
		
	}
}
class FacePower implements Serializable,Comparable<FacePower>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer face;
	private Integer height;
	public Integer getFace() {
		return face;
	}
	public void setFace(Integer face) {
		this.face = face;
	}
	public Integer getHeight() {
		return height;
	}
	public void setHeight(Integer height) {
		this.height = height;
	}
	public FacePower(Integer face, Integer height) {
		super();
		this.face = face;
		this.height = height;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return getFace() + "|" + getHeight();
	}
	@Override
	public int compareTo(FacePower o) {
		if(getFace() - o.getFace() == 0 ){
			return getHeight() - o.getHeight();
		}else{
			return getFace() - o.getFace();
		}
	}
}
