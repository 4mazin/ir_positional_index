package pos_index_project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PositionalIndexDriver <input path> <output path>");
            System.exit(-1);
        }
	
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Positional Index");
        job.setJarByClass(Driver.class);
        job.setMapperClass(poss_mapper.class);
        job.setReducerClass(poss_reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
}
}