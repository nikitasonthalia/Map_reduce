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

public class Max_Temp {
	
	public static class Map extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			
			StringTokenizer tokenizer=new StringTokenizer(value.toString());
			IntWritable word = new IntWritable();
			IntWritable word1 = new IntWritable();
			while(tokenizer.hasMoreTokens())
			{
				word.set(Integer.parseInt(tokenizer.nextToken()));
				word1.set(Integer.parseInt(tokenizer.nextToken()));
				context.write(word, word1);
				
			}
		}
		
	}
	
	public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
	{
		public void reduce(IntWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException
		{
			int max=0;
			for(IntWritable i:value)
			{
				if(i.get()>max)
				{
					max=i.get();
				}
			}
			context.write(key, new IntWritable(max));
			
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job=new Job(conf,"mt");
		job.setJarByClass(Max_Temp.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		Path path= new Path(args[1]);
		path.getFileSystem(conf).delete(path);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		
		
	}

}
