package in.mymapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MyPatent {
	
	public static class Map extends Mapper<LongWritable,Text,IntWritable,FloatWritable>
	{
	
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException
		{
			IntWritable s=new IntWritable();
			FloatWritable s1=new FloatWritable();
			StringTokenizer stringtokenizer = new StringTokenizer(value.toString());
			while(stringtokenizer.hasMoreTokens())
			{
				s.set(Integer.parseInt(stringtokenizer.nextToken()));
				s1.set(Float.parseFloat(stringtokenizer.nextToken()));
				context.write(s, s1);
			}	
		}
	}
	
	public static class Reduce extends Reducer<IntWritable,FloatWritable,IntWritable,IntWritable>
	{
		public void reduce(IntWritable key,Iterable<FloatWritable> value, Context context ) throws IOException, InterruptedException
		{
			int sum=0;
			for(FloatWritable i:value)
			{
				sum++;
			}
			context.write(key, new IntWritable(sum));
		}
	}
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = new Job(conf,"mp");
		job.setJarByClass(MyPatent.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		Path path= new Path(args[1]);
		path.getFileSystem(conf).delete(path);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
