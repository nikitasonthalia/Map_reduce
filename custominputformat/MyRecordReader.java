package in.custominputformat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;



public class MyRecordReader extends RecordReader<MyKey,MyValue> {
	
	MyKey key;
	MyValue value;
	
	LineRecordReader l=new LineRecordReader();

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		l.close();
	}

	@Override
	public MyKey getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public MyValue getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return l.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		l.initialize(arg0, arg1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		boolean gotNextKeyValue=l.nextKeyValue();
		
		if(gotNextKeyValue)
		{
			if(key==null)
			{
				key=new MyKey();
			}
			if(value==null)
			{
				value=new MyValue();
			}
			Text s = (Text) l.getCurrentValue();
			String[] tokens=s.toString().split("\t");
			key.setSensiorType(new Text(tokens[0]));
			key.setTimeStamp(new Text(tokens[1]));
			key.setStatus(new Text(tokens[2]));
			value.setValue1(new Text(tokens[3]));
			value.setValue2(new Text(tokens[4]));
		}
		else {
			key = null;
			value = null;
		}
		return gotNextKeyValue;
		
		
		
	}
	

}
