package a_erotima;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
//to count how many are the cars presented in our database
public class Cars_Count {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("JavaStat1").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> file = jsc.textFile(System.getProperty("user.dir") + "/src/database.csv");

		String header = file.first();

		JavaRDD<String> data = file.filter(i -> !i.equals(header));

		JavaPairRDD<String, Integer> cars = data.mapToPair(s -> {
			String[] foo = s.split(",");
			return new Tuple2<>(foo[0], Integer.parseInt(foo[3]));
		}).distinct();
		System.out.println("The number of cars is " + cars.groupByKey().count());
		jsc.close();
	}

}
