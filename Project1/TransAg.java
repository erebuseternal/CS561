package Project1;

import java.lang.Iterable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TransAg {

	public static class Map extends Mapper<LongWritable,
    Text, Text, Text> {
      private Text id = new Text();
      private Text trans_data = new Text();

      public void map(LongWritable key, Text value, Context context) {
        String[] tokens = value.toString().split(",");
        id.set(tokens[1]);
        // we know the transaction count for one transaction is one
        // and we know that our transaction total is the third entry in a transaction
        // tuple
        String data = "1," + tokens[2];
        trans_data.set(data);
        try {
        	context.write(id, trans_data);
        } catch(Exception e) {
        	
        }
      }
    }

  public static class Reduce extends Reducer<Text, Text,
    Text, Text> {
      private Text trans_data = new Text();

      public void reduce(Text key, Iterable<Text> values, Context context) {
    	// this will contain the total number of transactions
        int sum = 0;
        // this will contain the sum of transaction totals
        float trans_sum = 0;
        String new_value = "";
        for (Text value : values) {
          String[] tokens = value.toString().split(",");
          int number = Integer.parseInt(tokens[0]);
          float trans_value = Float.parseFloat(tokens[1]);
          sum += number;
          trans_sum += trans_value;
        }
        new_value = Integer.toString(sum) + ',' + Float.toString(trans_sum);
        trans_data.set(new_value);
        try {
        	context.write(key, trans_data);
        } catch(Exception e) {
        	
        }
      }
    }

    public static void main(String[] args) throws Exception{
      Job job = new Job();
      job.setJarByClass(TransAg.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setMapperClass(Map.class);
      job.setReducerClass(Reduce.class);
      // if we set the third argument to 1 a combiner will be used
      if (Integer.parseInt(args[2]) == 1) {
        job.setCombinerClass(Reduce.class);
      }
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
    }
}
