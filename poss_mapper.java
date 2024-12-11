package pos_index_project;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class poss_mapper extends Mapper<LongWritable, Text, Text, Text> {

	Text term = new Text();
	Text  position = new Text();
	
	protected void map(LongWritable  key, Text value, Context context) throws IOException, InterruptedException{
		
		 FileSplit fileSplit = (FileSplit) context.getInputSplit();
	        String fileName = fileSplit.getPath().getName();
	     String[] tokens = value.toString().split("\\W+");
	     
	     for (int pos = 0; pos < tokens.length; pos++){
	    	 if (!tokens[pos].isEmpty()) { 
	    		 term.set(tokens[pos].toLowerCase()); 
	    		 position.set(fileName + ":" + pos); 
	             context.write(term, position);
	     }
	}
	}
}

