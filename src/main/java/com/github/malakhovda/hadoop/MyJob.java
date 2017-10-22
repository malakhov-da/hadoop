/*
 * Copyright 2017 Dmitriy Malakhov.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.malakhovda.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author Dmitriy Malakhov
 */
public class MyJob {
    public static String TEMP = "w:/tmp/";
    public static String INPUT = "w:/input";
    public static String OUTPUT = "w:/output";
    
    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(), value);
        }
        
    }
    
    public static class ReducerImpl extends Reducer<Text, Text, NullWritable, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }
        
    }
    
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        conf.set("mapreduce.jobtracker.staging.root.dir", TEMP);
        Job job = Job.getInstance(conf, "MyJob");
        job.setJarByClass(MyJob.class);
        job.setMapperClass(MapperImpl.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ReducerImpl.class);
        FileInputFormat.addInputPath(job, new Path(INPUT));
        fs.delete(new Path(OUTPUT), true);
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
        job.waitForCompletion(true);
    }
}
