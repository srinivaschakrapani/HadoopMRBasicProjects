package com.srini.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapForNumberSum extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] numbers = line.split(" ");
        System.out.println("*** Starting Mapper **** ");
        for (String num : numbers) {
            Text outputKey = new Text("sum");
            LongWritable outputValue = new LongWritable(Long.valueOf(num));
            System.out.println("*** Before calling Reducer **** ");
            context.write(outputKey, outputValue);
        }
    }
}
