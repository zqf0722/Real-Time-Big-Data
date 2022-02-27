import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PageRank <input path> <output path>");
            System.exit(-1);
        }


        String original = args[1]+"/tempInput";
        String tempOutputPath = "";
        String tempInputPath = "";


        for(int i=0;i<3;i++){
            Job job = Job.getInstance();
            job.setJarByClass(PageRank.class);
            job.setJobName("PageRank");

            job.setNumReduceTasks(1);

            Configuration conf = job.getConfiguration();
            conf.set("mapred.textoutputformat.separator", " ");

            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            if(i == 0){
                FileInputFormat.addInputPath(job, new Path(args[0]));
                tempOutputPath = original + String.valueOf(i);
                FileOutputFormat.setOutputPath(job, new Path(tempOutputPath));
            }else if(i == 1){
                tempInputPath = tempOutputPath+"/part-r-00000";
                FileInputFormat.addInputPath(job, new Path(tempInputPath));
                tempOutputPath = original + String.valueOf(i);
                FileOutputFormat.setOutputPath(job, new Path(tempOutputPath));
            }else{
                tempInputPath = tempOutputPath+"/part-r-00000";
                FileInputFormat.addInputPath(job, new Path(tempInputPath));
                String finalOutputPath = args[1] + "/finalOutput";
                FileOutputFormat.setOutputPath(job, new Path(finalOutputPath));
            }
            job.waitForCompletion(true);
        }

        System.exit(0);
    }
}