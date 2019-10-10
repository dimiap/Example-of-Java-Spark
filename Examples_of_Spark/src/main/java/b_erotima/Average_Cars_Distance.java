package b_erotima;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
//to show the average distance a car drove 
public class Average_Cars_Distance {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("AverageDistance").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		JavaRDD<String> file = jsc.textFile(System.getProperty("user.dir") + "/src/database.csv");

		String header = file.first();

		JavaRDD<String> datab = file.filter(i -> !i.equals(header));

		JavaPairRDD<String, Double> average = datab.mapToPair(s -> {
			String[] foo = s.split(",");
			return new Tuple2<>(foo[0], Double.parseDouble(foo[2]));
		});

		JavaPairRDD<String, Tuple2<Double, Double>> valueCount = average
				.mapValues(s -> new Tuple2<Double, Double>(s, (double) 1));

		JavaPairRDD<String, Tuple2<Double, Double>> reducedCount = valueCount.reduceByKey(
				(tuple1, tuple2) -> new Tuple2<Double, Double>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));

		JavaPairRDD<String, Double> averagePair = reducedCount.mapToPair(getAverageByKey);

		averagePair.foreach(data -> {
			System.out.println("Car=" + data._1() + "\t" + " Average=" + data._2());
		});

		jsc.close();

	}

	private static PairFunction<Tuple2<String, Tuple2<Double, Double>>, String, Double> getAverageByKey = (tuple) -> {
		Tuple2<Double, Double> val = tuple._2;
		double total = val._1;
		double count = val._2;
		Tuple2<String, Double> averagePair = new Tuple2<String, Double>(tuple._1, total / count);
		return averagePair;
	};

}
