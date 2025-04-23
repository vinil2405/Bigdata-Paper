import org.apache.kafka.clients.producer.*;
import java.io.*;
import java.util.Properties;

public class AmazonReviewProducer {

    public static void main(String[] args) throws IOException {
        // Set up Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Performance tuning for large files
        props.put("acks", "1");              // Wait for leader only
        props.put("linger.ms", 5);           // Wait a bit for batching
        props.put("batch.size", 65536);      // Larger batches

        // Create Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // File path and topic name
        String filePath = "c:\\kafka\\amazon_reviews_us_Shoes_v1_00.tsv";  // change filepath for everydataset
        String topicName = "test22";

        // File reading setup
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line = reader.readLine(); // skip header

        int count = 0; // to count total records sent

        while ((line = reader.readLine()) != null) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, null, line);
            producer.send(record);
            count++;

            // Optional: flush every 1000 records to optimize throughput
            if (count % 1000 == 0) {
                producer.flush();
            }
        }

        producer.flush(); // final flush
        producer.close();
        reader.close();

        // Print summary
        System.out.println(" Finished sending records.");
        System.out.println("Total records sent: " + count);
    }
}