package in.custominputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.io.WritableComparable;

public class MyKey implements WritableComparable {
	
	Text sensiorType, timeStamp, status;
	
	public Text getSensiorType() {
		return sensiorType;
	}

	public void setSensiorType(Text sensiorType) {
		this.sensiorType = sensiorType;
	}

	public Text getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Text timeStamp) {
		this.timeStamp = timeStamp;
	}

	public Text getStatus() {
		return status;
	}

	public void setStatus(Text status) {
		this.status = status;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

}
