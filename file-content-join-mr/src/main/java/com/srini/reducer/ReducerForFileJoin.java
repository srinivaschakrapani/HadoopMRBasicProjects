package com.srini.reducer;

import com.srini.customtypes.JoinWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.Iterator;

public class ReducerForFileJoin extends Reducer<Text, JoinWritable, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<JoinWritable> values, Reducer<Text, JoinWritable, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        String name = null;
        String dept = null;
        for (JoinWritable x : values) {
            if (x.getMrFileName().toString().equals("emp.txt")) {
                name = x.getMrValue().toString();
            }
            if (x.getMrFileName().toString().equals("dept.txt")) {
                dept = x.getMrValue().toString();
            }
        }
        StringBuilder stb = new StringBuilder(key.toString()).append(name).append(",").append(dept);
        context.write(NullWritable.get(), new Text(stb.toString()));
    }
}
