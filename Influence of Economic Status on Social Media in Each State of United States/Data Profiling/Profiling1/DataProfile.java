package com.mycompany.app;

import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.MapReduceBase;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class DataProfile {



    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private ArrayList<ArrayList<String>> state_matrix = new ArrayList<ArrayList<String>>();

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String file_name = conf.get("file_name");
            Path pt = new Path(file_name);
            try
            {
                FileSystem fis= FileSystem.get(new Configuration());
                BufferedReader br= new BufferedReader(new InputStreamReader(fis.open(pt)));
                String line;
                line = br.readLine();
                while (line != null) {
                    ArrayList<String> this_line = new ArrayList<String>();
                    line = line.replace("\n","");
                    String[] tokens = line.split(",");
                    for(int j = 0; j<tokens.length;j++){
                        this_line.add(tokens[j]);
                    }
                    state_matrix.add(this_line);
                    line = br.readLine();
                }
            }
            catch(IOException e)
            {
                ;
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{

            String line = value.toString();
            line = line.replace("\t"," ").replace("\n"," ");
            String[] tokens = line.split("\\s+");
            if(tokens.length<5){
                return;
            }
            String latitude = tokens[2];
            String longitude = tokens[3];
            String check_in_time = tokens[1].substring(0,10);
            int lat = (int) Float.parseFloat(latitude);
            int longi = (int) Float.parseFloat(longitude);
            int left_most = -125;
            int right_most = -66;
            int top_most = 49;
            int down_most = 26;
            if(lat>=26&&lat<=49) {
                if (longi >= -125 && longi <= -66) {
                    String state = state_matrix.get(lat-down_most).get(longi-left_most);
                    if (!state.equals("X")) {
                        context.write(new Text(state), new Text(check_in_time));
                    }
                }
            }
         }
	   }


    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: DataProfile <input path> <state_matrix> <output path>");
            System.exit(-1);
        }


        Job job = Job.getInstance();
        job.setJarByClass(com.mycompany.app.DataProfile.class);
        job.setJobName("Data Profile qz2038");

        job.setMapperClass(Map.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        job.getConfiguration().set("file_name", args[1]);

        String path1 = args[0] + "Brightkite_totalCheckins.txt";
        String path2 = args[0] + "Gowalla_totalCheckins.txt";

        FileInputFormat.addInputPath(job, new Path(path1));
        FileInputFormat.addInputPath(job, new Path(path2));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}