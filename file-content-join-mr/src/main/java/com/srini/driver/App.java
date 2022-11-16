package com.srini.driver;

import com.srini.customtypes.JoinWritable;
import com.srini.mapper.MapperForFileJoin;
import com.srini.reducer.ReducerForFileJoin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class App {
    public static void main(String [] args) throws Exception
    {
        System.out.println("*** Starting NumberSum **** ");
        Configuration c=new Configuration();
        String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output=new Path(files[1]);
        Job j=new Job(c,"filejoinJob");
        j.setJarByClass(App.class);
        j.setMapperClass(MapperForFileJoin.class);
        j.setReducerClass(ReducerForFileJoin.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(JoinWritable.class);
        j.setOutputKeyClass(NullWritable.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true)?0:1);
        System.out.println("*** Completed execution of NumberSum **** ");
    }
}
