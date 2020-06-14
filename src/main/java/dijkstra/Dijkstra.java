package main.java.dijkstra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class Dijkstra {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Dijkstra")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("d:/dijkstra.txt");
        final String outNodeName = "A";
        
        JavaPairRDD<String, Node> mapRDD = rdd.mapToPair(new PairFunction<String, String, Node>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Node> call(String line) throws Exception {
				String[] split = line.split("\t");
				String key = split[0];
				String value;
				String bv = StringUtils.join(Arrays.copyOfRange(split, 1, split.length), "\t");
				if(outNodeName.equals(key)){
					value = "0\t\t"+bv;
				}else{
					value = Integer.MAX_VALUE+"\t\t"+bv;
				}
				Node node = Node.fromMR(value);
				
				
				return new Tuple2<String, Node>(key, node);
			}
		});
        
        while(true) {
        	final Accumulator<Integer> accumulator = sc.accumulator(0);
        	
        	mapRDD = mapRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Node>, String,Node>() {

    			/**
    			 * 
    			 */
    			private static final long serialVersionUID = 1L;

    			@Override
    			public Iterable<Tuple2<String, Node>> call(Tuple2<String, Node> tuple) throws Exception {
    				
    				
    				List<Tuple2<String, Node>> retList = new ArrayList<>();
    				retList.add(tuple);
    				Node node = tuple._2;
    				String key = tuple._1;
    				if(node.isDistanceSet()){
    					accumulator.add(1);
    					String backpointer = node.constructBackpointer(key);
    					for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {//开始扩展

    						String neighbor = node.getAdjacentNodeNames()[i];
    						 
    						int neighborDistance = node.getDistance()
    								+ Integer.parseInt(neighbor.split(":")[1]);
     
    						String tmp_key = neighbor.split(":")[0];//以节点的名字为key

    						Node adjacentNode = new Node()
    								.setDistance(neighborDistance).setBackpointer(backpointer);

    						retList.add(new Tuple2<String, Node>(tmp_key, adjacentNode));
    					}
    				}
    				return retList;
    			}
    		});
        	
        	
        	mapRDD = mapRDD.groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<Node>>, String, Node>() {
	
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;
	
				@Override
				public Tuple2<String, Node> call(Tuple2<String, Iterable<Node>> tuple) throws Exception {
					String key = tuple._1;
					Iterator<Node> iterator = tuple._2.iterator();
					Node originalNode = null;
					Node shortNode = null;
					int minDistance = Node.INFINITE;
					while (iterator.hasNext()) {
						Node node = iterator.next();
						if(node.containsAdjacentNodes()){
							originalNode = node;
						}
						
						if(node.getDistance() < minDistance){
							minDistance = node.getDistance();
							shortNode = node;
						}
					}
					
					if (shortNode != null) {
						originalNode.setDistance(minDistance);
						originalNode.setBackpointer(shortNode.getBackpointer());
					}
					
					return new Tuple2<String, Node>(key,originalNode);
				}
			});
        	
        	
        	mapRDD.foreach(new VoidFunction<Tuple2<String,Node>>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public void call(Tuple2<String, Node> t) throws Exception {
					System.out.println(t);
					
				}
			});
        	if(accumulator.value() == 6){
        		break;
        	}
        }
      
	}
}
