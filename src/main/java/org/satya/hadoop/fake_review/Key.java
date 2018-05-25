package org.satya.hadoop.fake_review;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;
import com.google.common.collect.ComparisonChain;
 
public class Key implements WritableComparable<Key> {
	private String x;
	private String y; 
	public String getX() { 
		return x; 
	}
	public void setX(String x) { 
		this.x = x; 
	}
	public String getY() { 
		return y; 
	}
	public void setY(String y) {
		this.y = y; 
	} 
	public Key(String x, String y) { 	
		this.x = x;
		this.y = y;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(x);	
		out.writeUTF(y);
	} 	
	public void readFields(DataInput in) throws IOException { 
		x = in.readUTF();
		y = in.readUTF();	
	}
	public Key(){
	} 
	@Override	 
	public int compareTo(Key o) { 
		// TODO Auto-generated method stub
		return ComparisonChain.start().compare(this.x,o.x).compare(this.y,o.y).result();
	} 
	public boolean equals(Object o1) { 
		if (!(o1 instanceof Key)) {
			return false;
		}
		Key other = (Key)o1;
		return this.x == other.x && this.y == other.y;	
	} 
	@Override 
	public String toString() { 
		return x+":"+y; 
	} 
}