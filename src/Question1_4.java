import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Question1_4 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/* It is an empty line */
			if (value.toString().isEmpty()) {
				return;
			}
			
			Pattern pattern = Pattern.compile("([A-z]+)");
			String text;
			Matcher matcher;
			
			for(String s: value.toString().split("\\s+")) {
				matcher = pattern.matcher(s);
				
				if (matcher.find()) 
					text = matcher.group(0).toString();
				else
					text = s.toString();
				
				context.write(new Text(text), new IntWritable(1));
			}
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			
			for (IntWritable i: values){
				count += i.get();
			}
			
			context.write(key, new IntWritable(count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		
		Job job = Job.getInstance(conf, "Question1_4");
		job.setJarByClass(Question1_4.class);
		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		/* Adds combiner class. It will combine each equal key in one, reducing the redundancy */
		job.setCombinerClass(MyReducer.class);
		job.setNumReduceTasks(5);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
