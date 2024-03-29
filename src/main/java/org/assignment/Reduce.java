package org.assignment;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class Reduce extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text country, Iterable<Text> customerIDs, Context context) throws IOException, InterruptedException {
        HashSet<String> customerIDset = new HashSet<>();
        for (Text customerid : customerIDs) {
            if(!StringUtils.isNotBlank(customerid.toString()))
                continue;
            customerIDset.add(customerid.toString());
        }

        int size = customerIDset.size();
        IntWritable sizeIW = new IntWritable(size);
        context.write(country, sizeIW);
    }
}
