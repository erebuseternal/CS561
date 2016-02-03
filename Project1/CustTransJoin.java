package Project1;

import java.lang.Iterable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CustTransJoin {
	public static class CustomerMap extends Mapper<LongWritable, Text, Text, Text> {
      private Text id = new Text();

      public void map(LongWritable key, Text value, Context context) {
    	// split the value into tokens
        String[] tokens = value.toString().split(",");
        // we set the customer id as our id (which will be our output key) 
        id.set(tokens[0]);
        // these are what they look like
        String name = tokens[1];
        String salary = tokens[4];
        // we create our output value with a prefix of C so the reducer knows where
        // it came from
        Text cust_info = new Text("C" + name + "," + salary);
        try {
        	context.write(id, cust_info);
        } catch(Exception e) {
        	
        }
      }
    }

  public static class TransMap extends Mapper<LongWritable, Text, Text, Text> {
      private Text id = new Text();

      public void map(LongWritable key, Text value, Context context) {
    	// split the value into tokens
        String[] tokens = value.toString().split(",");
        // we set the customer id as our id (which will be our output key) 
        id.set(tokens[1]);
        // num_items is the number of items in the transaction
        String num_items = tokens[3];
        // total is the transaction total
        String total = tokens[2];
        // we use a prefix of T and then we add as our first value 1 because 
        // that is the number of transactions captured so far
        Text trans_info = new Text("T" + "1" + "," + total + "," + num_items);
        try {
        	context.write(id, trans_info);
        } catch(Exception e) {
        	
        }
      }
    }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
	  
	  private Text output = new Text();

      public void reduce(Text key, Iterable<Text> values, Context context) {
    	// we set this to an extremely large number so it will be immediately 
    	// overridden 
        int min_num_items = 1000000000;
        // this will capture the number of items per record in values
        int num_items = 0;
        // this will capture the transaction total sum for the all values for our key
        float key_total = 0;
        // this will capture the transaction total (sum) for a specific value
        float total = 0;
        // this will capture the number of transactions for the key
        int sum = 0;
        // this will capture the number of transactions for a specific value
        int count = 0;
        // these are used for the combiner. If a combiner only gets transaction inputs
        // then we want it to output a transaction output. Equivalent behavior for 
        // only customer inputs. If we find both, then we write out our joined 
        // tuple. NOTE I will be using a combiner so I can do aggregation on the 
        // transaction values before hand so there is less data shuffling
        boolean found_C = false;
        boolean found_T = false;
        // captures what you would think
        String name = "";
        String salary = "";
        for (Text value : values) {
          // we turn the value into a string
          String val = value.toString();
          // we check to see if it is a transaction value
          if (val.charAt(0) == 'T') {
        	found_T = true;
            String[] tokens = val.substring(1).split(",");
            count = Integer.parseInt(tokens[0]);
            total = Float.parseFloat(tokens[1]);
            num_items = Integer.parseInt(tokens[2]);
            // we use this to find the minimum number of transaction items for this
            // set of transaction values
            if (num_items < min_num_items) {
              min_num_items = num_items;
            }
            // we increase the count on transactions
            sum += count;
            // and we increase the transaction total for this key
            key_total += total;
          }
          // or a customer value
          else if (val.charAt(0) == 'C') {
            found_C = true;
            String[] tokens = val.substring(1).split(",");
            name = tokens[0];
            salary = tokens[1];
        } }
        // now we can put everything together
        // if we found both, we create a joined value
        if (found_C && found_T) {
        	output.set(name + "," + salary + "," + Integer.toString(sum) + "," + Float.toString(key_total) + "," + Integer.toString(min_num_items));
        }
        // if we only found C we create a customer value
        else if (found_C && !found_T) {
        	output.set("C" + name + "," + salary);
        }
        // if we only found T we create a transaction value
        else {
        	output.set("T" + Integer.toString(sum) + "," + Float.toString(key_total) + "," + Integer.toString(num_items));
        }
        try {
        	context.write(key, output);
        } catch(Exception e) {
        	
        }
      }
    } 

    public static void main(String[] args) throws Exception {
      Job job = new Job();
      job.setJarByClass(CustTransJoin.class);
      job.setReducerClass(Reduce.class);
      job.setCombinerClass(Reduce.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransMap.class);
      MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMap.class);
      FileOutputFormat.setOutputPath(job, new Path(args[2]));

      job.waitForCompletion(true);
    }
}
