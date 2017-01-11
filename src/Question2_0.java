
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.math.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

public class Question2_0 {
	
	protected static final String K_TAGS_NAME = "k";
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		// I think that it is not matching when it has just one tag (without comma)
		protected static final String TAGS_PATTERN = "([a-zA-Z0-9]+,\\s*)+";
		
		// http://stackoverflow.com/a/31408260
		protected static final String LAT_LONG_PATTERN = "^(\\+|-)?(?:90(?:(?:\\.0{1,6})?)|(?:[0-9]|[1-8][0-9])(?:(?:\\.[0-9]{1,6})?))$";
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			Pattern tagsPattern = Pattern.compile(TAGS_PATTERN, Pattern.UNICODE_CHARACTER_CLASS);
			Pattern latLongPattern = Pattern.compile(LAT_LONG_PATTERN);
			String text, tags = ""; 
			Float lon = (float) 0, lat = (float) 0;
			Matcher matcher;
		
			for (String s: value.toString().split("\\t+")) {
				text = s.toString();
				
				/* match tags */
				matcher = tagsPattern.matcher(text);
				if (matcher.find()){
					tags = java.net.URLDecoder.decode(text, "UTF-8");
					
					continue;
				}
				
				matcher = latLongPattern.matcher(text);
				if ((lon == 0 || lat == 0) && matcher.find()) {
					// Longitude is always the first one
					if (lon == 0) {
						lon = Float.parseFloat(text);
						continue;
					}
					
					if (lat == 0) {
						lat = Float.parseFloat(text);
						continue;
					}
				}
			}
			
			Country country = Country.getCountryAt(lat, lon);
			if (!tags.isEmpty() && country != null)
				for (String s: tags.split(",")) 				
					context.write(new Text(country.toString()), new Text(s));				
			
//			System.out.println("Tags: "+tags.toString());
//			System.out.println("Lat: "+lat.toString()+" , Lon: "+lon.toString());
//			System.out.println("====================================================================");
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> hmap = new HashMap<String, Integer>();
			
			for (Text i: values){
			  int value; 
			  if ( hmap.get(i.toString()) != null)
				 value = hmap.get(i.toString()).intValue()+1;
			  else value = 1;
			  
			  hmap.put(i.toString(), value);			  
			  			 
			}
						
			Integer quantityTags = Integer.parseInt(context.getConfiguration().get(K_TAGS_NAME));
			
			MinMaxPriorityQueue<StringAndInt> max = MinMaxPriorityQueue.maximumSize(quantityTags).create();
			for (Map.Entry<String, Integer> entry : hmap.entrySet()) 				
				max.add(new StringAndInt(entry.getKey(), entry.getValue()));
			
			System.out.println("******* Country: "+key+" *********");
			
			StringAndInt s;
			while ((s = max.pollFirst()) != null) {
				context.write(key, new Text (s.getTag()));
				System.out.println("Tag: "+s.getTag()+" Count: "+s.getCount());
			}
			
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		conf.set(K_TAGS_NAME, otherArgs[2]);
		
		Job job = Job.getInstance(conf, "Question2_0");
		job.setJarByClass(Question2_0.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}