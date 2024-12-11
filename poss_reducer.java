package pos_index_project;

import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class poss_reducer extends Reducer<Text, Text, Text, Text>  {

	Text result = new Text();
	
	
	protected void reduce(Text key , Iterable<Text> values , Context context) throws IOException, InterruptedException{
		
		 HashMap<String , HashMap<String , ArrayList<Integer>>> filePositions = new HashMap<>();
		 
		for(Text value : values){
			String[] splitter = value.toString().split(":");
			String term = key.toString();
			String filename = splitter[0];
			int position = Integer.parseInt(splitter[1]);
			filename = filename.replace(".txt", "");
			
			if (!filePositions.containsKey(term)) {
			    filePositions.put(term, new HashMap<String , ArrayList<Integer>>());
			}
			HashMap<String , ArrayList<Integer>> fileMap =filePositions.get(term);
			
			if (!fileMap.containsKey(filename)) {
		       
		        fileMap.put(filename, new ArrayList<Integer>());
		    }
			 ArrayList<Integer> positions = fileMap.get(filename);

			    positions.add(position);
		}
		
		int documentFrequency = filePositions.get(key.toString()).size();
		
		StringBuilder sb = new StringBuilder();
		
		sb.append(documentFrequency).append("; ");
		
		 for (Map.Entry<String, HashMap<String, ArrayList<Integer>>> entry : filePositions.entrySet()) {
		        HashMap<String, ArrayList<Integer>> fileValue = entry.getValue();
		        
		        for (Map.Entry<String, ArrayList<Integer>> fileEntry : fileValue.entrySet()) {
		            String filename = fileEntry.getKey();
		            ArrayList<Integer> positions = fileEntry.getValue();
		            
	                  
	                  sb.append(filename)
	                  .append(":")
	                  .append(positions.toString().replace("[", "(").replace("]", ")"))
	                  .append("; ");
                
                int start = sb.indexOf("[");
                if (start != -1) {
                    sb.replace(start, start + 1, "(");
                }

                // Replace ']' with ')'
                int end = sb.indexOf("]");
                if (end != -1) {
                    sb.replace(end, end + 1, ")");
                
        }
       
            }
        
            
	}
        
        result.set(sb.toString().trim());
        context.write(key, result);
	
}
}
