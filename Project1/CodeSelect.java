package Project1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;

public class CodeSelect {
	public static class Map extends Mapper<LongWritable, Text,Text, Text> {

      public void map(LongWritable key, Text value, Context context) {
        String[] tokens = value.toString().split(",");
        int countrycode = Integer.parseInt(tokens[3]);
        if (countrycode <= 6 && countrycode >= 2) {
        	try {
        		context.write(value, null);
        	} catch(Exception e) {
        	}
        }
      }
    }

  public static void main(String[] args) throws Exception {
    Job job = new Job();
    job.setJobName("CodeSelect");
    job.setJarByClass(CodeSelect.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(Map.class);
    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}
