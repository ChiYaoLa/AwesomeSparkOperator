package main.java.puv;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

//import org.apache.cassandra.cli.CliParser.newColumnFamily_return;



public class GenerateDataPUV {

	public static void main(String[] args) {
		String path = "d:/demo/userLog";
		writeLog2File(path, 10000);
	}

	static String[] channelNames = new String[] { "Spark", "Scala", "Kafka", "Flink", "Hadoop", "Storm", "Hive", "Impala", "HBase", "ML" };

	static String[] actionNames = new String[] { "View", "Register" };

	private static String dateToday;
	private static Random random = new Random();

	/**
	 * 日志格式：
	 * 	date timestamp userID pageID channel action
	 * @return
	 */
	private static String userlogs() {
		dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		StringBuffer userLogBuffer = new StringBuffer("");
		int[] unregisteredUsers = new int[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		long timestamp = new Date().getTime();
		Long userID = 0L;
		long pageID = 0L;

		// 随机生成的用户ID
		if (unregisteredUsers[random.nextInt(8)] == 1) {
			userID = null;
		} else {
			userID = (long) random.nextInt((int) 20);
		}

		// 随机生成的页面ID
		pageID = random.nextInt((int) 100);

		// 随机生成Channel
		String channel = channelNames[random.nextInt(10)];

		// 随机生成action行为
		String action = actionNames[random.nextInt(2)];

		userLogBuffer.append(dateToday).append("\t").append(timestamp).append("\t").append(userID).append("\t").append(pageID).append("\t").append(channel).append("\t").append(action);
		return userLogBuffer.toString();
	}

	public static void writeLog2File(String path, int logNum) {
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)));
			for (int i = 0; i < logNum; i++) {
				String userlogs = userlogs();
				System.out.println(userlogs);
				bw.write(userlogs+ "\n");
			}
			bw.flush();
			bw.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
