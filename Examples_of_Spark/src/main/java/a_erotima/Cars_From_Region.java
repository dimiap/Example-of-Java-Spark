package a_erotima;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
//to count from how many areas the cars in our database drove by
public class Cars_From_Region {
	private static final Pattern NEW_LINE = Pattern.compile("\\\\u0085");

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("JavaStat2").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> file = jsc.textFile(System.getProperty("user.dir") + "/src/database.csv");

		String header = file.first();

		JavaRDD<String> data = file.filter(i -> !i.equals(header));

		JavaRDD<String> lines = data.flatMap(s1 -> Arrays.asList(NEW_LINE.split(s1)).iterator());

		JavaPairRDD<String, Integer> cars = lines.mapToPair(s -> {
			String[] foo = s.split(",");
			return new Tuple2<>(foo[0], 1);
		});
		JavaPairRDD<String, Integer> result = cars.reduceByKey((x, y) -> (x + y));

		result.foreach(x -> System.out.println(x));

		jsc.close();

	}

}
