import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SparkNameFinder {

    private static final List<String> NAMES = Arrays.asList(
            "James", "John", "Robert", "Michael", "William", "David", "Richard", "Charles", "Joseph", "Thomas",
            "Christopher", "Daniel", "Paul", "Mark", "Donald", "George", "Kenneth", "Steven", "Edward", "Brian",
            "Ronald", "Anthony", "Kevin", "Jason", "Matthew", "Gary", "Timothy", "Jose", "Larry", "Jeffrey",
            "Frank", "Scott", "Eric", "Stephen", "Andrew", "Raymond", "Gregory", "Joshua", "Jerry", "Dennis",
            "Walter", "Patrick", "Peter", "Harold", "Douglas", "Henry", "Carl", "Arthur", "Ryan", "Roger"
    );

    public static void main(String[] args) {
        // Create a Spark configuration and context
        SparkConf conf = new SparkConf().setAppName("Spark Name Finder").setMaster("local[*]");
        JavaSparkContext spark = new JavaSparkContext(conf);

        // Broadcast the list of names
        Broadcast<List<String>> broadcastNames = spark.broadcast(NAMES);

        // Read the input file
        JavaRDD<String> lines = spark.textFile("src\\main\\resources\\big.txt");
        AtomicInteger lineOffset = new AtomicInteger();

        // Process each line to find matches
        JavaPairRDD<String, String> matches = lines.flatMapToPair(line -> {
            List<Tuple2<String, String>> results = new ArrayList<>();

            for (String name : broadcastNames.value()) {
                Pattern pattern = Pattern.compile("\\b" + name + "\\b");
                Matcher matcher = pattern.matcher(line);
                while (matcher.find()) {
                    String location = "[lineOffset=" + lineOffset + ", charOffset=" + matcher.start() + "]";
                    results.add(new Tuple2<>(name, location));
                }
            }
            lineOffset.incrementAndGet();
            return results.iterator();
        });

        // Aggregate the results
        JavaPairRDD<String, Iterable<String>> groupedMatches = matches.groupByKey();

        // Collect and print the results
        Map<String, Iterable<String>> result = groupedMatches.collectAsMap();
        result.forEach((name, locations) -> System.out.println(name + " --> " + locations));

        // Stop the Spark context
        spark.stop();
    }
}