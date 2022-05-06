import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Count {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Count <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(Count.class);
        job.setJobName("Check-in Count");


        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);

        for(int i = 0; i<6;i++){
            String path = "0000" + String.valueOf(i);
            path = args[0] + path;
            FileInputFormat.addInputPath(job, new Path(path));
        }
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.getConfiguration().set("mapred.textoutputformat.separator", ",");
        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}