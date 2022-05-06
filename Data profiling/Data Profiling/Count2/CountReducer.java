import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException{

        Long count = 0L;
        for(LongWritable value: values){
            count += value.get();
        }
        context.write(key, new LongWritable(count));
    }
}