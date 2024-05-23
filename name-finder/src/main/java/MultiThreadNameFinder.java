import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MultiThreadNameFinder {

    private static final String FILE_PATH = "src\\main\\resources\\big.txt";
    public static final List<String> NAMES = Arrays.asList(
            "James", "John", "Robert", "Michael", "William", "David", "Richard", "Charles", "Joseph", "Thomas",
            "Christopher", "Daniel", "Paul", "Mark", "Donald", "George", "Kenneth", "Steven", "Edward", "Brian",
            "Ronald", "Anthony", "Kevin", "Jason", "Matthew", "Gary", "Timothy", "Jose", "Larry", "Jeffrey",
            "Frank", "Scott", "Eric", "Stephen", "Andrew", "Raymond", "Gregory", "Joshua", "Jerry", "Dennis",
            "Walter", "Patrick", "Peter", "Harold", "Douglas", "Henry", "Carl", "Arthur", "Ryan", "Roger"
    );
    private static final int CHUNK_SIZE = 1000;

    //Main module -> Reads input data and invoke matcher and aggregator module.
    public static void main(String[] args) throws Exception {
        List<Future<Map<String, List<String>>>> futures = new ArrayList<>();
        int nThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
        BufferedReader reader = Files.newBufferedReader(Paths.get(FILE_PATH));

        StringBuilder chunkBuilder = new StringBuilder();
        int lineOffset = 1;
        String line;

        while ((line = reader.readLine()) != null) {
            chunkBuilder.append(line).append("\n");
            if (++lineOffset % CHUNK_SIZE == 0) {
                String chunk = chunkBuilder.toString();
                int finalLineOffset = lineOffset;
                futures.add(executorService.submit(() -> NameMatcher.findMatches(chunk, finalLineOffset - CHUNK_SIZE)));
                chunkBuilder.setLength(0);
            }
        }

        if (chunkBuilder.length() > 0) {
            String chunk = chunkBuilder.toString();
            int finalLineOffset1 = lineOffset;
            int finalLineOffset2 = lineOffset;
            futures.add(executorService.submit(() -> NameMatcher.findMatches(chunk, finalLineOffset1 - (finalLineOffset2 % CHUNK_SIZE))));
        }

        executorService.shutdown();
        Aggregator aggregator = new Aggregator();

        for (Future<Map<String, List<String>>> future : futures) {
            aggregator.aggregate(future.get());
        }

        aggregator.printResults();
    }
}
//Matcher module -> Find matches with input data
class NameMatcher {
    public static Map<String, List<String>> findMatches(String text, int startingLineOffset) {
        Map<String, List<String>> results = new HashMap<>();
        String[] lines = text.split("\n");

        for (int i = 0; i < lines.length; i++) {
            for (String name : MultiThreadNameFinder.NAMES) {
                Matcher matcher = Pattern.compile("\\b" + name + "\\b").matcher(lines[i]);
                while (matcher.find()) {
                    results.computeIfAbsent(name, k -> new ArrayList<>()).add(
                            "[lineOffset=" + startingLineOffset + i + ", charOffset=" + matcher.start() + "]"
                    );
                }
            }
        }

        return results;
    }
}

//Aggregator module - Aggregate findings to a Map.
class Aggregator {
    private final Map<String, List<String>> aggregatedResults = new HashMap<>();

    public synchronized void aggregate(Map<String, List<String>> results) {
        results.forEach((name, locations) ->
                aggregatedResults.computeIfAbsent(name, k -> new ArrayList<>()).addAll(locations)
        );
    }

    public void printResults() {
        aggregatedResults.forEach((name, locations) -> {
            System.out.println(name + " --> " + locations);
        });
    }
}