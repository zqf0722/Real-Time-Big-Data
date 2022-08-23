package com.mycompany.app;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GoogleAPI {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PageRank <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(GoogleAPI.class);
        job.setJobName("Google API");

        job.setMapperClass(GoogleAPIMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);


        String file1 = "Brightkite_totalCheckins.txt";
        String file2 = "Gowalla_totalCheckins.txt";

        String path1 = args[0] + file1;
        String path2 = args[0] + file2;


        FileInputFormat.addInputPath(job, new Path(path1));
        FileInputFormat.addInputPath(job, new Path(path2));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}