package org.satya.hadoop.fake_review;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Reducer; 


public class Detector_Reducer extends Reducer<Key, DoubleWritable, Text, Text> {
	public static double calAverageRating(String name){ 
	       try{
	    	   URL stream = Detector_Mapper.class.getResource("/amazon_mp3_output.txt");
	    	   @SuppressWarnings("resource")
	    	   BufferedReader csv =  new BufferedReader(new FileReader(stream.getFile()));
	    	   String line ="";
	    	   while((line = csv.readLine()) != null){
	        	  String[] data = line.split(":");    	         
	        	  if(data[0].equals(name)){
	    	  			return Double.parseDouble(data[1]);      
	        	  }
	                   
	          }
	          csv.close();
	       }
	       catch(Exception e){e.printStackTrace();
	       }
		return -1; 
	}	   
	/*
	 * Method to calculate the threshold value.
	 * Returns a positive integer.
	 */
	public static double threshold(double x, double y){
		double value = 0.0;
		value = x-y;
		if(value<0){
			return -value;
		}
		else{
			return value;
		}
		
	}
	public  void reduce(Key key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
        Double avg_rating = 0.0;
        Double cal_rating = 0.0;
        String review_id[] = key.toString().split(":");        
        for (DoubleWritable val : values) {
         
        	cal_rating += val.get();
        	break;
         
        }
        cal_rating = -(cal_rating);
        avg_rating = calAverageRating(review_id[1]);
		if(!(threshold(avg_rating,cal_rating) < 0.5)){		//Threshold value.
			//ds.deleteReview(review_id);
			context.write(new Text("For Review : " + review_id[0] + "\n"),new Text("Product Name : " + review_id[1] +"\nCalculated Sentiment Score of the review : " + cal_rating + "\n" + "Average Rating  of the product : " + avg_rating + "\n" + "Fake Review" ));  // used to collect the data in the form of Text and Text        
		}
		else{
			context.write(new Text("For Review : " + review_id[0] + "\n" ),new Text("Product Name : " + review_id[1] +"\nCalculated Sentiment Score of the review : " + cal_rating + "\n" + "Average Rating  of the product : " + avg_rating + "\n" + "Genuine Review\n" ));  // used to collect the data in the form of Text and Text 			
		}
	} 
}  