package org.assignment;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

public class Map extends Mapper<LongWritable, Text, Text, Text>  {
    private final static int INVOICE = 0;
    private final static int STOCKCODE = 1;
    private final static int DESCRIPTION = 2;
    private final static int QUANTITY = 3;
    private final static int INVOICEDATE = 4;
    private final static int PRICE = 5;
    private final static int CUSTOMERID = 6;
    private final static int COUNTRY = 7;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("Key is " + key);
        System.out.println("Value is " + value);
        System.out.println("Context is " + context);

        String record = value.toString();
        CSVReader csvRecord = new CSVReader(new StringReader(record));
        List<String[]> csvParsedLine;
        try {
            csvParsedLine = csvRecord.readAll();
        } catch (CsvException e) {
            throw new RuntimeException(e);
        }
        String[] fields = csvParsedLine.get(0);

        String invoice = fields[INVOICE];
        String stockcode = fields[STOCKCODE];
        String description = fields[DESCRIPTION];
        String quantity = fields[QUANTITY];
        String invoicedate = fields[INVOICEDATE];
        String price = fields[PRICE];
        String customerid = fields[CUSTOMERID];
        String country = fields[COUNTRY];

        System.out.println(invoice + " | " + stockcode + " | " + description + " | " + quantity +
                " | " + invoicedate + " | " + price + " | " + customerid + " | " + country);

        if (country.equals(context.getConfiguration().get("country")))
           context.write(new Text(country), new Text(customerid));

    }


}
