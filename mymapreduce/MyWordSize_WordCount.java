package in.mymapreduce;

import java.io.IOException;
import java.util.StringTokenizer;


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



public class MyWordSize_WordCount {

	public static class Map extends Mapper<LongWritable,Text,IntWritable,Text>{
		
		private Text word = new Text();
		public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
		{
			
			StringTokenizer stringtokenizer = new StringTokenizer(value.toString());
			while(stringtokenizer.hasMoreTokens())
			{
				word.set(stringtokenizer.nextToken());
				context.write(new IntWritable(word.getLength()),word);
				
			}
			
		}
	}
	
	public static class Reduce extends Reducer<IntWritable,Text,IntWritable,IntWritable>
	{
		public void reduce(IntWritable key, Iterable<Text> value,Context context) throws IOException,InterruptedException
		{
			int sum=0;
			for(Text i:value)
			{
				sum+=1;
			}
			context.write(key,new IntWritable(sum));
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job job = new Job(conf,"myws_wc");
		
		job.setJarByClass(MyWordSize_WordCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		Path path = new Path(args[1]);
		path.getFileSystem(conf).delete(path);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		
		
		
	}

}
