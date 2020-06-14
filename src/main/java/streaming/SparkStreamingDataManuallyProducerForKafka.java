package main.java.streaming;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
 
public class SparkStreamingDataManuallyProducerForKafka extends Thread
{
	
    static String[] channelNames = new  String[]{
    	"Spark","Scala","Kafka","Flink","Hadoop","Storm",
    	"Hive","Impala","HBase","ML"
    };

    static String[] actionNames = new String[]{"View", "Register"};

    private String topic; //发送给Kafka的数据的类别
    private Producer<Integer, String> producerForKafka;
    
    private static String dateToday;
    private static Random random;

    public SparkStreamingDataManuallyProducerForKafka(String topic){
    	dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        this.topic = topic;
        random = new Random();
        Properties conf = new Properties();
        conf.put("metadata.broker.list","hadoop1:9092,hadoop2:9092,hadoop3:9092");
        conf.put("serializer.class",  StringEncoder.class.getName());
        producerForKafka = new Producer<Integer, String>(new ProducerConfig(conf)) ;
    }
    
    
    @Override
    public void run() {
    	int counter = 0;
    	while(true){
    		counter++;
			String userLog = userlogs();
//			System.out.println("product:"+userLog+"   ");
			producerForKafka.send(new KeyedMessage<Integer, String>(topic, userLog));
			if(0 == counter%500){
				counter = 0;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			}
    	}
    

    public static void main( String[] args )
    {
    	new SparkStreamingDataManuallyProducerForKafka("Abgelababy").start();
    }

	

	private static String userlogs() {
		
		StringBuffer userLogBuffer = new StringBuffer("");
		int[] unregisteredUsers = new int[]{1, 2, 3, 4, 5, 6, 7, 8};
		long timestamp = new Date().getTime();
			Long userID = 0L;
			long pageID = 0L;
			
			//随机生成的用户ID 
			if(unregisteredUsers[random.nextInt(8)] == 1) {
				userID = null;
			} else {
				userID = (long) random.nextInt((int) 2000);
			}
			
			
			//随机生成的页面ID
			pageID =  random.nextInt((int) 2000);
			
			//随机生成Channel
			String channel = channelNames[random.nextInt(10)];
			
			//随机生成action行为
			String action = actionNames[random.nextInt(2)];
			
			
			userLogBuffer.append(dateToday)
						.append("\t")
						.append(timestamp)
						.append("\t")
						.append(userID)
						.append("\t")
						.append(pageID)
						.append("\t")
						.append(channel)
						.append("\t")
						.append(action);
//						.append("\n");	
			
				
		return userLogBuffer.toString();
		
	}

}
