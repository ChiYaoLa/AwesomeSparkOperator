package main.java.sql.createdf;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.columnar.INT;

/**
 * 使用反射的方式将RDD转换成为DataFrame
 * 1、自定义的类必须是public
 * 2、自定义的类必须是可序列化的
 * 3、RDD转成DataFrame的时候，他会根据自定义类中的字段名进行排序。
 * @author zfg
 */

public class RDD2DataFrameByReflection {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("RDD2DataFrameByReflection");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlcontext = new SQLContext(sc);
		
		JavaRDD<String> lines = sc.textFile("Peoples.txt");
		
		JavaRDD<Person> personsRdd = lines.map(new Function<String, Person>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Person call(String line) throws Exception {
				String[] split = line.split(",");
				Person p = new Person();
				p.setId(Integer.valueOf(split[0].trim()));
				p.setName(split[1]);
				p.setAge(Integer.valueOf(split[2].trim()));
				return p;
			}
			
		});
		
		//传入进去Person.class的时候，sqlContext是通过反射的方式创建DataFrame
		//在底层通过反射的方式或得Person的所有field，结合RDD本身，就生成了DataFrame
		DataFrame df = sqlcontext.createDataFrame(personsRdd, Person.class);
	 
		//命名table的名字为person
		df.registerTempTable("personTable");
		
		
		DataFrame resultDataFrame = sqlcontext.sql("select * from personTable where age > 7");
		
		
		resultDataFrame.show();
		
		
	 	JavaRDD<Row> rrdd = resultDataFrame.javaRDD();
	 	
		JavaRDD<Person> pRdd = rrdd.map(new Function<Row, Person>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Person call(Row row) throws Exception {
				int age = row.getInt(0);
				int id = row.getInt(1);
				String name = row.getString(2);
//				row.getAs("name|age|id")
				Person person = new Person(id, name, age);
				return person;
			}
		});
		
		
		pRdd.foreach(new VoidFunction<Person>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Person p) throws Exception {
				System.out.println(p.toString());
			}
		}); 
		
		 
	}
}
 