import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class CharCount {

	public static void main(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);

		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"wordCount").setMaster("local"));
		JavaRDD<String> lines = sc.textFile(args[0]);
		int popularityLimit = Integer.parseInt(args[2]);

		JavaPairRDD<String, Integer> counts = lines
				.flatMap(line -> Arrays.asList(line.split(" ")))
				.mapToPair(w -> new Tuple2<>(w, 1))
				.reduceByKey((x, y) -> x + y)
				.filter(tuple -> tuple._2 > popularityLimit)
				.flatMapToPair(
						wordCount -> {
							HashMap<String, Integer> letterCountMap = new HashMap<>();
							for (char ch : wordCount._1.toCharArray()) {
								String c = Character.toString(ch).toLowerCase();
								if (letterCountMap.containsKey(c)) {
									letterCountMap.put(c, letterCountMap.get(c)
											+ wordCount._2);
								} else {
									letterCountMap.put(c, wordCount._2);
								}
							}
							List<Tuple2<String, Integer>> letterCountList = new ArrayList<>();
							for (Map.Entry<String, Integer> entry : letterCountMap
									.entrySet()) {
								letterCountList.add(new Tuple2<>(
										entry.getKey(), entry.getValue()));
							}
							return letterCountList;
						}).sortByKey();

		counts.saveAsTextFile(args[1]);
		sc.close();
	}
}