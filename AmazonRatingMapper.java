import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AmazonRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] cols = line.split("\t");

        if (cols.length >= 12 && !cols[0].equals("marketplace")) { // skip header
            try {
                double star = Double.parseDouble(cols[7]);
                double helpful = Double.parseDouble(cols[8]);
                double total = Double.parseDouble(cols[9]);
                String verified = cols[11].equalsIgnoreCase("Y") ? "1" : "0";

                // emit: key = "data", value = star,helpful,total,verified
                context.write(new Text("data"), new Text(star + "," + helpful + "," + total + "," + verified));
            } catch (Exception e) {
                // skip bad rows
            }
        }
    }
} 
