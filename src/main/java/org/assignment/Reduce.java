package org.assignment;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class Reduce extends Reducer<Text, Text, Text, IntWritable> {
    /*@Override
    protected void reduce(Text country, Iterable<Text> customerIDs, Context context) throws IOException, InterruptedException {
        HashSet<Text> customerIDset = new HashSet<>();
        customerIDs.forEach(customerIDset::add);

        IntWritable size = new IntWritable(customerIDset.size());
        context.write(country, size);
    }*/

    @Override
    protected void reduce(Text country, Iterable<Text> customerIDs, Context context) throws IOException, InterruptedException {
        HashSet<Text> customerIDset = new HashSet<>();
        for (Text customerid : customerIDs) {
            customerIDset.add(customerid);
        }

        int size = customerIDset.size();
        IntWritable sizeIW = new IntWritable(size);
        context.write(country, sizeIW);
    }
}
