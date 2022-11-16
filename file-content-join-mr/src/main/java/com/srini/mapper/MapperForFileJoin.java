package com.srini.mapper;

import com.srini.customtypes.JoinWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperForFileJoin extends Mapper<LongWritable, Text, Text, JoinWritable> {
    String ref_file_name = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        ref_file_name = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] emp_details = line.split(",");
        System.out.println("*** Starting Mapper **** ");
        System.out.println("*** Before calling Reducer **** ");
        context.write(new Text(emp_details[0]), new JoinWritable(new Text(emp_details[1]), new Text(ref_file_name)));
    }
}
