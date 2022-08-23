package com.mycompany.app;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.*;


public class GoogleAPIMapper extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{

        String line = value.toString();
        line = line.replace("\t"," ").replace("\n"," ");
        String[] tokens = line.split("\\s+");
        String latitude = tokens[2];
        String longitude = tokens[3];
        String check_in_time = tokens[1].substring(0, 10);
        String API_key = "AIzaSyBkhKlor1ZfV3EcGvyhjHmBfqDXDjvUxio";
        String request = "https://maps.googleapis.com/maps/api/geocode/json?latlng=" +
                latitude + "," + longitude + "&key=" + API_key;
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpget = new HttpGet(request);
        CloseableHttpResponse response;
        response = httpclient.execute(httpget);
        String result = EntityUtils.toString(response.getEntity());
        try {
            JSONObject responseObj = new JSONObject(result);
            JSONArray results = (JSONArray) responseObj.get("results");
            JSONObject resultObj = results.getJSONObject(0);
            String formatted_address = resultObj.getString("formatted_address");
            int len = formatted_address.length();
            if(formatted_address.substring(len-3, len).equals("USA")){
                // means it happens in the USA
                // Extract only the State
                String output = formatted_address.substring(len-13, len-11);
                context.write(new Text(output), new Text(check_in_time));
            }

        } catch (JSONException e) {
            ;
        }
    }
}

