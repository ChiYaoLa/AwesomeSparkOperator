package main.java.puv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class UserLogOps {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("UserLogOps");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> logRDD = sc.textFile("d:/demo/userLog");
		
		/**
		 * 什么是pv
		 *   对某一个页面的访问量，在页面中进行刷新一次就是一次pv
		 */
		String str = "View";
		final Broadcast<String> broadcast = sc.broadcast(str);
		
// 		 pvAnalyze(logRDD,broadcast);
//		
//		
//		uvAnalyze(logRDD,broadcast);
//		
//		 uvAnalyzeOptz(logRDD,broadcast);
		
		  hotChannel(sc,logRDD,broadcast);
  		 hotChannelOpz(sc,logRDD,broadcast); 
	}

	private static void hotChannelOpz(JavaSparkContext sc, JavaRDD<String> logRDD, final Broadcast<String> broadcast) {
		JavaRDD<String> filteredLogRDD = logRDD.filter(new Function<String, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				String actionParam = broadcast.value();
				String action = v1.split("\t")[5];
				return actionParam.equals(action);
			}
		});
		
		JavaPairRDD<String, String> channel2nullRDD = filteredLogRDD.mapToPair(new PairFunction<String, String,String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String val) throws Exception {
				String channel = val.split("\t")[4];
				
				return new Tuple2<String, String>(channel,null);
			}
		});
		
		/**
		 * K:channel
		 * V:pv
		 */
		Map<String, Object> channelPVMap = channel2nullRDD.countByKey();
		
		/**
		 * SortObj自定义的 类型
		 */
		List<SortObj> channels  = new ArrayList<>();

		Set<String> keySet = channelPVMap.keySet();
		for(String channel : keySet){ 
			channels.add(new SortObj(channel, Integer.valueOf(channelPVMap.get(channel)+"")));
		}
		Collections.sort(channels, new Comparator<SortObj>() {

			@Override
			public int compare(SortObj o1, SortObj o2) {
				return o2.getValue() - o1.getValue();
			}
		});
		
		List<String> hotChannelList = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			hotChannelList.add(channels.get(i).getKey());
		}
		
		
		final Broadcast<List<String>> hotChannelListBroadcast = sc.broadcast(hotChannelList);
		
		 
		JavaRDD<String> filtedRDD = logRDD.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String v1) throws Exception {
				List<String> hostChannels = hotChannelListBroadcast.value();
				String channel = v1.split("\t")[4];
				 String userId = v1.split("\t")[2];
				return hostChannels.contains(channel) && !"null".equals(userId);
			}
		});
		
		JavaPairRDD<String, String> user2ChannelRDD = filtedRDD.mapToPair(new PairFunction<String, String,String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String val) throws Exception {
				String[] splited = val.split("\t");
				String userId = splited[2];
				String channel = splited[4];
				return new Tuple2<String, String>(userId,channel);
			}
		});
		
		JavaPairRDD<String, String> userVistChannelsRDD = user2ChannelRDD.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
				String userId = tuple._1;
				Iterator<String> iterator = tuple._2.iterator();
				Map<String, Integer> channelMap = new HashMap<>();
				while (iterator.hasNext()) {
					String channel = iterator.next();
					Integer count = channelMap.get(channel);
					if(count == null)
						count = 1;
					else
						count++;
					channelMap.put(channel, count);
				}
				
				List<Tuple2<String, String>> list = new ArrayList<>();
				Set<String> keys = channelMap.keySet();
				for(String channel : keys){
					 Integer channelNum  = channelMap.get(channel);
					 list.add(new Tuple2<String, String>(channel, userId + "_" + channelNum));
				}
				return list;
			}
		});
		
		
		userVistChannelsRDD.groupByKey().foreach(new VoidFunction<Tuple2<String,Iterable<String>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<String>> tuple) throws Exception {
				String channel = tuple._1;
				Iterator<String> iterator = tuple._2.iterator();
				List<SortObj> list = new ArrayList<>();
				while (iterator.hasNext()) {
					String ucs = iterator.next();
					String[] splited = ucs.split("_");
					String userId = splited[0];
					Integer num = Integer.valueOf(splited[1]);
					list.add(new SortObj(userId, num));
				}
				
				Collections.sort(list,new Comparator<SortObj>() {

					@Override
					public int compare(SortObj o1, SortObj o2) {
						return o2.getValue() - o1.getValue();
					}
				});
				
				System.out.println("HOT_CHANNLE:"+channel);
				for(int i = 0 ; i < 3 ; i++){
					SortObj sortObj = list.get(i);
					System.out.println(sortObj.getKey() + "===" + sortObj.getValue());
				}
			}
		});
		
		
	}

	private static void hotChannel(JavaSparkContext sc,JavaRDD<String> logRDD, final Broadcast<String> broadcast) {
		JavaRDD<String> filteredLogRDD = logRDD.filter(new Function<String, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				String actionParam = broadcast.value();
				String action = v1.split("\t")[5];
				return actionParam.equals(action);
			}
		});
		
		JavaPairRDD<String, String> channel2nullRDD = filteredLogRDD.mapToPair(new PairFunction<String, String,String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String val) throws Exception {
				String channel = val.split("\t")[4];
				
				return new Tuple2<String, String>(channel,null);
			}
		});
		Map<String, Object> channelPVMap = channel2nullRDD.countByKey();
		
		
		Set<String> keySet = channelPVMap.keySet();
		List<SortObj> channels  = new ArrayList<>();
		for(String channel : keySet){ 
			channels.add(new SortObj(channel, Integer.valueOf(channelPVMap.get(channel)+"")));
		}
		Collections.sort(channels, new Comparator<SortObj>() {

			@Override
			public int compare(SortObj o1, SortObj o2) {
				return o2.getValue() - o1.getValue();
			}
		});
		
		List<String> hotChannelList = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			hotChannelList.add(channels.get(i).getKey());
		}
		for(String channle : hotChannelList){
			System.out.println("channle:" + channle);
		}
		
		final Broadcast<List<String>> hotChannelListBroadcast = sc.broadcast(hotChannelList);
		
		 
		JavaRDD<String> filtedRDD = logRDD.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				List<String> hostChannels = hotChannelListBroadcast.value();
				String channel = v1.split("\t")[4];
				String userId = v1.split("\t")[2];
				return hostChannels.contains(channel) && !"null".equals(userId);
			}
		});
		
		JavaPairRDD<String, String> channel2UserRDD = filtedRDD.mapToPair(new PairFunction<String, String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String v1) throws Exception {
				String[] splited = v1.split("\t");
				String channel = splited[4];
				String userId = splited[2];
				return new Tuple2<String, String>(channel,userId);
			}
		});
		
		channel2UserRDD.groupByKey().foreach(new VoidFunction<Tuple2<String,Iterable<String>>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<String>> tuple) throws Exception {
				String channel = tuple._1;
				Iterator<String> iterator = tuple._2.iterator();
				Map<String, Integer> userNumMap = new HashMap<>();
				while(iterator.hasNext()){
					String userId = iterator.next();
					Integer count = userNumMap.get(userId);
					if(count == null){
						count = 1;
					}else{
						count ++;
					}
					userNumMap.put(userId, count);
				}
				
				List<SortObj> lists = new ArrayList<>();
				Set<String> keys = userNumMap.keySet();
				for(String key : keys){
					lists.add(new SortObj(key, userNumMap.get(key)));
				}
				
				Collections.sort(lists,new Comparator<SortObj>() {

					@Override
					public int compare(SortObj O1, SortObj O2) {
						// TODO Auto-generated method stub
						return O2.getValue() - O1.getValue();
					}
				});
				
				System.out.println("HOT_CHANNEL:"+channel);
				for(int i = 0 ; i < 3 ; i++){
					SortObj sortObj = lists.get(i);
					System.out.println(sortObj.getKey()+"=="+sortObj.getValue());
				}
			}
		});
		
	}

	private static void uvAnalyzeOptz(JavaRDD<String> logRDD, final Broadcast<String> broadcast) {
		JavaRDD<String> filteredLogRDD = logRDD.filter(new Function<String, Boolean>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				String actionParam = broadcast.value();
				String action = v1.split("\t")[5];
				String userId = v1.split("\t")[2];
				return actionParam.equals(action) && "null".equals(userId);
			}
		});
		
		JavaPairRDD<String, String> up2LogRDD = filteredLogRDD.mapToPair(new PairFunction<String, String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String val) throws Exception {
				String[] splited = val.split("\t");
				String userId = splited[2];
				String pageId = splited[3];
				 
				return new Tuple2<String, String>(userId + "_" + pageId,null);
			}
		});
		
		JavaPairRDD<String, Iterable<String>> groupUp2LogRDD = up2LogRDD.groupByKey();
		
		Map<String, Object> countByKey = groupUp2LogRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
				String pu = tuple._1;
				String[] splited = pu.split("_");
				String pageId = splited[1];
				return new Tuple2<String, String>(pageId,null);
			}
		}).countByKey();
		
		Set<String> keySet = countByKey.keySet();
		for (String key : keySet) {
			System.out.println("PAGEID:"+key+"\tUV_COUNT："+countByKey.get(key));
		}
		
		
	}

	private static void uvAnalyze(JavaRDD<String> logRDD, final Broadcast<String> broadcast) {
		JavaRDD<String> filteredLogRDD = logRDD.filter(new Function<String, Boolean>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;
	
				@Override
				public Boolean call(String v1) throws Exception {
					String actionParam = broadcast.value();
					String action = v1.split("\t")[5];
					return actionParam.equals(action);
				}
			});
	
		JavaPairRDD<String, String> pairLogRDD = filteredLogRDD.mapToPair(new PairFunction<String, String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String val) throws Exception {
				String pageId = val.split("\t")[3];
				String userId = val.split("\t")[2];
				return new Tuple2<String, String>(pageId,userId);
			}
		});
		
		pairLogRDD.groupByKey().foreach(new VoidFunction<Tuple2<String,Iterable<String>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<String>> tuple) throws Exception {
				String pageId = tuple._1;
				Iterator<String> iterator = tuple._2.iterator();
				Set<String> userSets = new HashSet<>();
				while (iterator.hasNext()) {
					String userId = iterator.next();
					
					
					
					userSets.add(userId);
				}
				System.out.println("PAGEID:"+pageId+"\t UV_COUNT:"+userSets.size());
			}
		}); 
		
		
		
	}

	private static void pvAnalyze(JavaRDD<String> logRDD, final Broadcast<String> broadcast) {
	JavaRDD<String> filteredLogRDD = logRDD.filter(new Function<String, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				String actionParam = broadcast.value();
				String action = v1.split("\t")[5];
				return actionParam.equals(action);
			}
		});
		
		JavaPairRDD<String, String> pairLogRDD = filteredLogRDD.mapToPair(new PairFunction<String, String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(String val) throws Exception {
				String pageId = val.split("\t")[3];
				return new Tuple2<String, String>(pageId,null);
			}
		});
		
		 pairLogRDD.groupByKey().foreach(new VoidFunction<Tuple2<String,Iterable<String>>>() {
			 
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<String>> tuple) throws Exception {
				String pageId = tuple._1;
				Iterator<String> iterator = tuple._2.iterator();
				long count = 0L;
				while (iterator.hasNext()) {
					iterator.next();
					count++;
				}
				System.out.println("PAGEID:"+pageId+"\t PV_COUNT:"+count);
			}
		}); 
		
		Map<String, Object> map = pairLogRDD.countByKey();
		
		Set<String> keySet = map.keySet();
		for (String pageId : keySet) {
			System.out.println("PAGEID:"+pageId+"\t PV_COUNT:"+map.get(pageId));
		}
		
		/**
		 * 按照pv值排序  然后取pv最高的前3名
		 */
		
	}
}
