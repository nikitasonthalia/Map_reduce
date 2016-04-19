package in.custominputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MyValue implements Writable {
	
	Text value1,value2;

	
	public MyValue() {
		// TODO Auto-generated constructor stub
		value1=new Text();
		value2=new Text();
	}
	
	public MyValue(Text value1,Text value2)
	{
		this.value1=value1;
		this.value2=value2;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		value1.readFields(arg0);
		value2.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		value1.write(arg0);
		value2.write(arg0);
	}

	
	

}
