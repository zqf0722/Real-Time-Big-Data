import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{

        String line = value.toString();
        String[] tokens = line.replace("\n", " ").split("\\t+");
        if(tokens.length==0) return;
        String key_out = tokens[0];
        context.write(new Text(key_out), new LongWritable(1));
    }
}

