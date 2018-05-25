package org.satya.hadoop.fake_review;
import java.io.IOException;
import java.net.URL;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;



public class Detector_Mapper  extends Mapper<Key, Text, Key,DoubleWritable> {  
	private static double cal_rating;
	
	
	
	
	/*(non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 * 
	 * Mapper Logic. 
	 * input to the mapper <offset in the file, line at that offset>
	 * output from the mapper <review number, calculated Sentiment Score> 
	 * 
	 */

	public void map(Key key, Text value, Context context) throws IOException {
		// TODO Auto-generated method stub
		SWN3 samp = new SWN3();
		MaxentTagger tagger = null;
		URL stream = Detector_Mapper.class.getResource("/left3words-wsj-0-18.tagger");
		tagger = new MaxentTagger(stream.getFile());
		String review = value.toString();  // used to convert text class to string 
		try{
			review = review.toLowerCase();
            String tagged = tagger.tagString(review);
            cal_rating = samp.classifyreview(tagged);
            cal_rating = ((((cal_rating)*2)/5)-1); //normalization of score of all words.
        	context.write(key, new DoubleWritable(cal_rating));
		}
		catch(Exception e){
			System.out.println("ERROR occurred in Mapper Class");
        	e.printStackTrace();
        }
		
		
	}
} 