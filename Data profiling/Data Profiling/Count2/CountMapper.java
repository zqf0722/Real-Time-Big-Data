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
        String state = tokens[0];
        String time = tokens[1];
        String yyyymm = time.substring(0, 4) + time.substring(5, 7);
        String key_out = state + "," + yyyymm;
        context.write(new Text(key_out), new LongWritable(1));
    }
}

