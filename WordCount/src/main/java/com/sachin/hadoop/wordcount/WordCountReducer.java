package com.sachin.hadoop.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		int noOfFrequency = 0;
		for (IntWritable occurance : values) {
			noOfFrequency += occurance.get();
		}
		context.write(key, new IntWritable(noOfFrequency));
	}

}