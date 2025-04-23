import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import smile.regression.RandomForest;
import smile.data.*;
import smile.data.formula.Formula;
import smile.data.vector.*;

import java.time.Duration;
import java.util.*;

public class AmazonReviewConsumer {
    public static void main(String[] args) {
        // Kafka config
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
       props.put("group.id", "ml-group-" + UUID.randomUUID().toString());

        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test22")); // correct topic need to change topic for every dataset 

        // Data lists
        List<Double> starRatings = new ArrayList<>();
        List<Double> helpfulVotes = new ArrayList<>();
        List<Double> totalVotes = new ArrayList<>();
        List<String> verified = new ArrayList<>();

        int count = 0;
        int emptyPolls = 0;

        // Consume records
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                emptyPolls++;
                if (emptyPolls >= 6) break; // 30 sec no data
                continue;
            }
            emptyPolls = 0;

            for (ConsumerRecord<String, String> record : records) {
                String[] cols = record.value().split("\t");
                if (cols.length >= 14) {
                    try {
                        double rating = Double.parseDouble(cols[7]); // star_rating
                        if (rating < 1 || rating > 5) continue; // skip invalid rating

                        double helpful = Double.parseDouble(cols[8]);
                        double total = Double.parseDouble(cols[9]);
                        String verifiedPurchase = cols[11];

                        starRatings.add(rating);
                        helpfulVotes.add(helpful);
                        totalVotes.add(total);
                        verified.add(verifiedPurchase.equalsIgnoreCase("Y") ? "1.0" : "0.0");

                        count++;
                    } catch (Exception e) {
                        System.out.println("Skipped due to parsing error: " + Arrays.toString(cols));
                    }
                }
            }
        }

        System.out.println("Total Records Consumed: " + count);
        consumer.close();

        if (count < 5) {
            System.out.println("Not enough data to train/test. Minimum 5 rows required.");
            return;
        }

        // Create Smile DataFrame
        System.out.println("Sizes => rating: " + starRatings.size() +
    ", helpful: " + helpfulVotes.size() +
    ", total: " + totalVotes.size() +
    ", verified: " + verified.size());

int minSize = Collections.min(Arrays.asList(
    starRatings.size(), helpfulVotes.size(), totalVotes.size(), verified.size()
));

starRatings = starRatings.subList(0, minSize);
helpfulVotes = helpfulVotes.subList(0, minSize);
totalVotes = totalVotes.subList(0, minSize);
verified = verified.subList(0, minSize);

double[] ratings = starRatings.stream().mapToDouble(Double::doubleValue).toArray();
double[] helpful = helpfulVotes.stream().mapToDouble(Double::doubleValue).toArray();
double[] total = totalVotes.stream().mapToDouble(Double::doubleValue).toArray();
double[] verifiedIndexed = verified.stream().mapToDouble(Double::parseDouble).toArray();


        DataFrame df = DataFrame.of(
                DoubleVector.of("star_rating", ratings),
                DoubleVector.of("helpful_votes", helpful),
                DoubleVector.of("total_votes", total),
                DoubleVector.of("verified_purchase", verifiedIndexed)
        );

        System.out.println("Created DataFrame with " + df.nrows() + " rows");

        // Manual train-test split
        int totalRows = df.nrows();
        int trainSize = Math.max(1, (int) (totalRows * 0.8));
        int testSize = totalRows - trainSize;

        int[] trainIndices = new int[trainSize];
        int[] testIndices = new int[testSize];

        for (int i = 0; i < trainSize; i++) trainIndices[i] = i;
        for (int i = trainSize; i < totalRows; i++) testIndices[i - trainSize] = i;

        System.out.println("Splitting: Train = " + trainSize + ", Test = " + testSize);
        DataFrame train = df.slice(0, trainSize);
        DataFrame test = df.slice(trainSize, df.nrows());



        // Train RandomForest model
        Formula formula = Formula.lhs("star_rating");
        RandomForest model = RandomForest.fit(formula, train);

        // Predict and evaluate
        double[] predicted = model.predict(test);
        double[] actual = test.column("star_rating").toDoubleArray();

        double mae = meanAbsoluteError(actual, predicted);
        double accuracy = 100 - ((mae / 5.0) * 100);

        System.out.printf("Mean Absolute Error (MAE): %.4f\n", mae);
        System.out.printf("Estimated Accuracy: %.2f%%\n", accuracy);

        // Optional: print sample predictions
        for (int i = 0; i < Math.min(5, predicted.length); i++) {
            System.out.printf("Sample %d — Actual: %.1f, Predicted: %.2f%n", i + 1, actual[i], predicted[i]);
        }
    }

    public static double meanAbsoluteError(double[] actual, double[] predicted) {
        double sum = 0.0;
        for (int i = 0; i < actual.length; i++) {
            sum += Math.abs(actual[i] - predicted[i]);
        }
        return sum / actual.length;
    }
}