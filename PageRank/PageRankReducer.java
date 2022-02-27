import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException{

        String out = "";
        float outValue = 0;
        float res = 0;
        for(Text value:values){
            String line = value.toString();
            line = line.replace("\t"," ").replace("\n"," ");
            String[] tokens = line.split("\\s+");
            if(tokens.length == 2){
                try{
                    res = Float.parseFloat(tokens[1]);
                }catch(NumberFormatException e){
                    //not float
                    for(String token: tokens){
                        out = out + ' ' + token;
                    }
                    continue;
                }
                outValue += res;
            }else{
                for(String token: tokens){
                    out = out + ' ' + token;
                }
            }
        }
        out = out + ' ' + String.valueOf(outValue);
        context.write(key, new Text(out));
    }
}