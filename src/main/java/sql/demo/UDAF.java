package main.java.sql.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UDAF {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("UDAF").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(sc);

		List<String> list = new ArrayList<String>();

		list.add("yarn");
		list.add("Marry");
		list.add("Jack");
		list.add("Tom");
		list.add("Tom");

		/**
		 * SELECT name,udaf(name)  FROM nameTable GROUP BY name;
		 */
		JavaRDD<String> nameRdd = sc.parallelize(list);
		JavaRDD<Row> rowRdd = nameRdd.map(new Function<String, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(String name) throws Exception {
				return RowFactory.create(name);
			}
		});

		ArrayList<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		StructType structType = DataTypes.createStructType(structFields);

		DataFrame nameDF = sqlContext.createDataFrame(rowRdd, structType);

		nameDF.registerTempTable("nameTable");

		sqlContext.udf().register("stringCount", new UserDefinedAggregateFunction() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			// 指定输入数据的字段与类型
			@Override
			public StructType inputSchema() {
				return DataTypes.createStructType(
						Arrays.asList(DataTypes.createStructField("str", DataTypes.StringType, true)));
			}

			/**
			 * 初始化 可以认为是，你自己在内部指定一个初始的值
			 */
			@Override
			public void initialize(MutableAggregationBuffer buffer) {
				buffer.update(0, 0);
			}

			 // 最终函数返回值的类型
			@Override
			public DataType dataType() {
				return DataTypes.StringType;
			}

			// 最后返回一个最终的聚合值 要和dataType的类型一一对应
			@Override
			public Object evaluate(Row row) {
				return row.getInt(0)+"~";
			}

			@Override
			public boolean deterministic() {
				return true;
			}

			// 聚合操作时，所处理的数据的类型
			@Override
			public StructType bufferSchema() {
				return DataTypes.createStructType(
						Arrays.asList(DataTypes.createStructField("bf", DataTypes.IntegerType, true)));
			}

			/**
			 * 更新 可以认为是，一个一个地将组内的字段值传递进来 实现拼接的逻辑
			 *  buffer.getInt(0)获取的是上一次聚合后的值
			 * 相当于map端的combiner，combiner就是对每一个map task的处理结果进行一次小聚合 
			 *    大聚和发生在reduce端
			 */
			@Override
			public void update(MutableAggregationBuffer buffer, Row arg1) {
				buffer.update(0, buffer.getInt(0) + 1);
			}

			/**
			 * 合并 update操作，可能是针对一个分组内的部分数据，在某个节点上发生的 但是可能一个分组内的数据，会分布在多个节点上处理
			 * 此时就要用merge操作，将各个节点上分布式拼接好的串，合并起来
			 * buffer1.getInt(0) : 大聚和的时候 上一次聚合后的值       
			 * buffer2.getInt(0) : 这次计算传入进来的update的结果
			 */
			@Override
			public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
				buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0));
			}

		});

		sqlContext.sql("SELECT name,stringCount(name) from nameTable group by name").show();

	}
}
