package org.satya.hadoop.fake_review;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
public class Review_InputFormat extends FileInputFormat<Key,Text> {
	public RecordReader<Key,Text> createRecordReader(InputSplit arg0,TaskAttemptContext arg1) throws IOException,InterruptedException {
		return new Review_RecordReader();
	}
}