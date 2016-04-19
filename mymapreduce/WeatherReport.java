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

public class WeatherReport {

	public static class Map extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String date= line.substring(6, 14).trim();
			Float max=Float.parseFloat(line.substring(39, 45).trim());
			Float min=Float.parseFloat(line.substring(47, 53).trim());
			
			String year= date.substring(0, 4).trim().toString();
			String mm=date.substring(4, 6).trim().toString();
			String dd=date.substring(6, 8).trim().toString();
			
			date=mm+"-"+dd+"-"+year;
			
			if(max>40.0)
			{
				context.write(new Text(date), new Text("Hot day"));
			}
			if(min<10.0)
			{
				context.write(new Text(date), new Text("Cold day"));
			}
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"WR");
		job.setJarByClass(WeatherReport.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		Path path=new Path(args[1]);
		
		path.getFileSystem(conf).delete(path);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		

	}

}
