import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{

        String line = value.toString();
        line = line.replace("\t"," ").replace("\n"," ");
        String[] tokens = line.split("\\s+");
        int numberOfOut = tokens.length-2;
        float pr = Float.parseFloat(tokens[numberOfOut+1]);
        String outLinks="";
        String node = "";
        for(int i = 0;i<tokens.length;i++){
            if(i==0){
                node = tokens[i];
            }else if(i==numberOfOut+1){
                context.write(new Text(node), new Text(outLinks));
            }else{
                String outlink = tokens[i];
                outLinks = outLinks+"\t"+outlink;
                String toReducer = outlink + "\t"+ String.valueOf(pr/numberOfOut);
                context.write(new Text(outlink), new Text(toReducer));
            }
        }
    }
}

