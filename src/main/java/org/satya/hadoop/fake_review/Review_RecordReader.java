package org.satya.hadoop.fake_review;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
public class Review_RecordReader extends RecordReader<Key,Text> {
	private Key key ;
	private Text value ;
	private LineRecordReader reader = new LineRecordReader();
	
	@Override 
	public void close() throws IOException { 
		// TODO Auto-generated method stub
		reader.close();
	} 
	@Override 
	public Key getCurrentKey() throws IOException, InterruptedException { 
		// TODO Auto-generated method stub
		return key;
	} 
	@Override 
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	} 
	@Override 
	public float getProgress() throws IOException, InterruptedException { 
		// TODO Auto-generated method stub
		return reader.getProgress();
	} 
	@Override 
	public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
		reader.initialize(is, tac);
	} 
	@Override 
	public boolean nextKeyValue() { 
		
		Text line = null;
		String[] tokens = null;
		String product_name = null;
		String product_id = null;
		String review = null;

		try {
			boolean gotNextKeyValue = reader.nextKeyValue();
			// TODO Auto-generated method stub
			if(gotNextKeyValue){
				if(key==null){
					key = new Key();
				}
				if(value == null){	
					value = new Text();
				}
				
				do {
					line = reader.getCurrentValue();
					if(line == null) {
						return false;
					}
					if(line.toString().matches("^#{5}(.*)")){
						tokens = line.toString().split("^#{5}") ;
						product_id = tokens[1];
					}
					if(!reader.nextKeyValue()){
						System.err.println("ERROR while parsing for the product_id");
					}
				}while(product_id == null);
				do {
					line = reader.getCurrentValue();
					tokens = line.toString().split(":");
					if(tokens[0].equals("[productName]")) {
						product_name = tokens[1];
					}
					if(!reader.nextKeyValue()){
						System.err.println("ERROR while parsing for the product_name");
					}
				}while(product_name == null);
				do {
					line = reader.getCurrentValue();
					tokens = line.toString().split(":");
					if(tokens[0].equals("[fullText]")) {
						review =  tokens[1];
					}
					if(!reader.nextKeyValue()){
						System.err.println("ERROR while parsing for the review");
					}
				}while(review == null);
				key.setX(product_id);
				key.setY(product_name);
				value.set(new Text(review));
				
			}
			else {
				key = null;
				value = null;
			}			
			return gotNextKeyValue;
		}
		catch(Exception e){
			System.err.println("ERROR occured in Record_Reader");
			e.printStackTrace();
		}
		return false;
		
	}
 
}
