
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.lang.Math;
import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.Set;

public class SpatialJoin {

	/**
	 * @param args
	 */
	
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(SpatialJoin.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PointMap.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RectangleMap.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);
	}
	
	public static class PointMap extends Mapper<LongWritable, Text, Text, Text> {
		int x_min = 0;
		int x_max = 10000;
		int y_min = 0;
		int y_max = 10000;
		
		public void setBounds(int left_bound, int right_bound, int bottom_bound, int top_bound) {
			x_min = left_bound;
			x_max = right_bound;
			y_min = bottom_bound;
			y_max = top_bound;
		}
		
		// this method takes x,y coordinates and returns the x and y index of the partition to which these belong as a Integer array
		public Integer[] getPartition(double x, double y) {
			// first we prepare x and y
			int flr_x = (int) Math.floor(x);
			int flr_y = (int) Math.floor(y);
						
			// next we get the closest (lower) multiple to five and twenty respectively
			int mod_x = flr_x % 5;
			int new_x = flr_x - mod_x;
			int mod_y = flr_y % 20;
			int new_y = flr_y - mod_y;
						
			// for consistency, we need to check to make sure that new_x and new_y are not y_min or y_max
			// in which case we need to decrement them by 5 or 20 respectively so they are not found in the 
			// partition that starts at y_max or x_max which doesn't exist
			if (new_x == x_max) {
				// this will put it in the appropriate partition
				new_x = new_x - 5;
			}
			if (new_y == y_max) {
				new_y = new_y + 20;
			}
						
			// now we get the x and y indexes (indexes of the partition this point is in)
			int x_index = (int)((new_x - x_min) / 5);
			int y_index = (int)((new_y - y_min) / 20);
			
			return new Integer[] {new Integer(x_index), new Integer(y_index)};
		}
		
		/* now we are going to image that we have broken up the space set by the bounds 
		 into rectangles of height 20 and width 5. (Where rectangles can overlap at the top
		 and right of our space. This means we have divided our space up into the maximum size of 
		 the rectangles in our rectangles data set. Now in another mapper we are going to send 
		 each rectangle in our data to the rectangle in our space that holds the former's top left
		 corner. (or for rectangle's outside of the bound we will send it to the dividing rectangle 
		 that is above the one containing the lower right hand corner. The reason for this will become 
		 clear in a moment.) Now for the rectangles that are placed by top left corner they might 
		 extend into the 'partitioning' rectangles directly below them, to the right, or both. Therefore 
		 in order to send each point to all of the partitions that might contain the left hand corner of 
		 a rectangle containing them, we have to attach the point's value to the partition to which 
		 it belongs as well as the ones directly above, to the left, and both (if they exist). This way
		 if one of those have the top left corner of a rectangle that extends to the partition to 
		 which the point actually belongs, that point will be considered for the rectangle in the reducer.*/
		
		/*
		 * To turn each partition into a key (to be used for the reducer) we will say that the bottom 
		 * partition is 0,0 the one to its right is 0,1 the one directly above it 1,1 and so forth.
		 * Therefore for each of these points we are going to find its partition (and thus key) by 
		 * getting its x value to the nearest lower multiple of five and its y value to the nearest lower 
		 * multiple of 20. Then take each of these and divide by 5 and 20 respectively to get the two 
		 * values of our key. Then we will subtract one from each individually and then both 
		 * to get the other possible keys. We will make sure these keys are in our bounds and then send 
		 * them off!
		 */
		
		public void map(LongWritable key, Text value, Context context) {
			// first we split the value into its parts 
			// I expect the value to be in the form x,y
			String[] tokens = value.toString().split(",");
			double x = Double.parseDouble(tokens[0]);
			double y = Double.parseDouble(tokens[1]);
			
			// next we make sure these values are in the bounds
			if (x < x_min || x > x_max) {
				return;
			}
			if (y < y_min || y > y_max) {
				return;
			}
			
			// now we get the partition this point lies in
			Integer[] indexes = getPartition(x,y);
			
			// now we get the x and y indexes (indexes of the partition this point is in)
			Integer x_index = indexes[0];
			Integer y_index = indexes[1];
			// we write out our first key pair
			Text out_key = new Text(x_index.toString() + "," + y_index.toString());
			try {
				context.write(out_key, value);
			} catch(Exception e) {
			}
			
			// next we will check that the partitions above, to the left, and above-left exist
			// if they do we will assign this point to those partitions as well.
			
			// we check for the partition above
			// note it is less than rather than less than or equal to, because equal to would 
			// mean the point initially laid in the partition that started at y_max
			if ((y_index + 1) * 20 + y_min < y_max) {
				// we generate the key
				out_key = new Text(x_index.toString() + "," + new Integer(y_index + 1).toString());
				// we send the key-pair
				try {
					context.write(out_key, value);
				} catch(Exception e) {
				}
			}
			
			// now we check for the partition to the left 
			if ((x_index - 1) * 5  + x_min >= x_min) {
				// we generate the key
				out_key = new Text(new Integer(x_index - 1).toString() + "," + y_index.toString());
				// we send the key-pair
				try {
					context.write(out_key, value);
				} catch(Exception e) {
				}
				
				// next we check to see if left is also okay so we can get up and to the left
				if ((y_index + 1) * 20 + y_min < y_max) {
					// we generate the key
					out_key = new Text(new Integer(x_index - 1).toString() + "," + new Integer(y_index + 1).toString());
					// we send the key-pair
					try {
						context.write(out_key, value);
					} catch(Exception e) {
					}
				}
			}
		}

	}
	
	public static class RectangleMap extends Mapper<LongWritable, Text, Text, Text> {
		int x_min = 0;
		int x_max = 10000;
		int y_min = 0;
		int y_max = 10000;
		
		public void setBounds(int left_bound, int right_bound, int bottom_bound, int top_bound) {
			x_min = left_bound;
			x_max = right_bound;
			y_min = bottom_bound;
			y_max = top_bound;
		}
		
		// this method takes x,y coordinates and returns the x and y index of the partition to which these belong as a Integer array
		public Integer[] getPartition(double x, double y) {
			// first we prepare x and y
			int flr_x = (int) Math.floor(x);
			int flr_y = (int) Math.floor(y);
								
			// next we get the closest (lower) multiple to five and twenty respectively
			int mod_x = flr_x % 5;
			int new_x = flr_x - mod_x;
			int mod_y = flr_y % 20;
			int new_y = flr_y - mod_y;
								
			// for consistency, we need to check to make sure that new_x and new_y are not y_min or y_max
			// in which case we need to decrement them by 5 or 20 respectively so they are not found in the 
			// partition that starts at y_max or x_max which doesn't exist
			if (new_x == x_max) {
				// this will put it in the appropriate partition
				new_x = new_x - 5;
			}
			if (new_y == y_max) {
				new_y = new_y + 20;
			}
								
			// now we get the x and y indexes (indexes of the partition this point is in)
			int x_index = (int)((new_x - x_min) / 5);
			int y_index = (int)((new_y - y_min) / 20);
					
			return new Integer[] {new Integer(x_index), new Integer(y_index)};
		}
		
		public void map(LongWritable key, Text value, Context context) {
			// refer to the map for points for a full explanation.
			/*
			 * We are going to do the following:
			 * 	check that either our rectangles upper left corner or lower right corner is in the bounds set for us
			 * 	if it is in the upper corner, we are going to find the nearest lower multiple of 5 and 20
			 *  for the x and y values of that corner and then divide by 5 and 20 respectively to get the 
			 *  index in x and y of the partitioning rectangle in which this upper corner exists. 
			 * 
			 *  If the lower corner is the one in our bounds, we actually put it in the partition above 
			 *  the partition the corner is in, so that we get the points of this partition as well as the lower 
			 *  one in which the corner actually exists (because of how we distributed our points). If that 
			 *  upper partition does not actually exist, then we assign the rectangle to the partition in 
			 *  which the lower right hand corner was found.
			 *  
			 *  Finally we have to check the other two corners. If the upper right corner is in our bounds we just 
			 *  assign it to the partition it is found in
			 *  
			 *  If the lower left hand corner is the one in the bounds, we assign it to the partition above, 
			 *  if that partition exists, or otherwise to the partition it belongs
			 */
			
			// so first we get the upper left hand corner's coordinates 
			// I expect the value to be in the form x,y,w,h,name
			String[] tokens = value.toString().split(",");
			double x = Double.parseDouble(tokens[0]);
			double y = Double.parseDouble(tokens[1]);
			double w = Double.parseDouble(tokens[2]);
			double h = Double.parseDouble(tokens[3]);
			
			// next we look to see if this corner is in our bounds
			if ((x >= x_min && x <= x_max) && (y >= y_min && y <= y_max)) {
				// we get the partition it is in 
				Integer[] indexes = getPartition(x,y);
				
				// now we get the x and y indexes
				Integer x_index = indexes[0];
				Integer y_index = indexes[1];
				
				// now we go ahead and assign the key and value
				Text out_key = new Text(x_index.toString() + "," + y_index.toString());
				try {
					context.write(out_key, value);
				} catch(Exception e) {
				}
			} // we look at the lower right corner
			else if ((x + w >= x_min && x + w <= x_max) && (y - h >= y_min && y - h <= y_max)) {
				// we get the partition it is in 
				Integer[] indexes = getPartition(x + w,y - h);
				
				// now we get the x and y indexes
				Integer x_index = indexes[0];
				Integer y_index = indexes[1];
				
				// we will to assign it to the partition above if it exists
				if ((y_index + 1) * 20 + y_min < y_max) {
					y_index = y_index + 1;
				}
				
				// now we go ahead and assign the key and value
				Text out_key = new Text(new Integer(x_index).toString() + "," + new Integer(y_index).toString());
				try {
					context.write(out_key, value);
				} catch(Exception e) {
				}
			} // we look at the upper right corner
			else if ((x + w >= x_min && x + w <= x_max) && (y >= y_min && y <= y_max)) {
				// we get the partition it is in 
				Integer[] indexes = getPartition(x + w,y);
				
				// now we get the x and y indexes
				Integer x_index = indexes[0];
				Integer y_index = indexes[1];
				
				// now we go ahead and assign the key and value
				Text out_key = new Text(new Integer(x_index).toString() + "," + new Integer(y_index).toString());
				try {
					context.write(out_key, value);
				} catch(Exception e) {
				}
			} // finally lower left 
			else if ((x >= x_min && x <= x_max) && (y - h >= y_min && y - h <= y_max)) {
				// we get the partition it is in 
				Integer[] indexes = getPartition(x,y - h);
				
				// now we get the x and y indexes
				Integer x_index = indexes[0];
				Integer y_index = indexes[1];
				
				// we need to assign it to the partition above if that partition exists
				if ((y_index + 1) * 20 + y_min < y_max) {
					y_index = y_index + 1;
				}
				
				// now we go ahead and assign the key and value
				Text out_key = new Text(new Integer(x_index).toString() + "," + new Integer(y_index).toString());
				try {
					context.write(out_key, value);
				} catch(Exception e) {
				}
			}
		}
	}
	
	/*
	 * So now we have sent each point to each partition that could hold the top left corner 
	 * of a rectangle that holds it. Therefore the each list that a reducer gets will have 
	 * several rectangles and the points from all partitions that rectangle could cover.
	 * So now we have a simple join ahead of us!
	 * 
	 * First we are going to need to separate out the points and the rectangles.
	 * Then once we have done that, we will need to go through the points and for 
	 * every rectangle they are contained in, we need to add that point's x and y values
	 * as a string of the form x,y to a string for each of those rectangles of the form
	 * coordinates_1;coordinates_2;...
	 * string will be in a map where they is the rectangle name and the value is this string
	 * once we have finished our loop, we will write out the key value pairs.
	 */
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		Text out_key = new Text();
		Text out_val = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) {
			// FIRST WE SORT RECTANGLES FROM POINTS
			Map<String,String> rectangle_map = new HashMap<String,String>();
			Map<String, double[]> rectangles = new HashMap<String, double[]>();
			LinkedList<String> points = new LinkedList<String>();
			for (Text value : values) {
				// note I am assuming the rectangle's name contains a 'r'
				String val = value.toString();
				if (val.indexOf('r') != -1) {
					String[] tokens = val.split(",");
					double x = Double.parseDouble(tokens[0]);
					double y = Double.parseDouble(tokens[1]);
					double w = Double.parseDouble(tokens[2]);
					double h = Double.parseDouble(tokens[3]);
					String name = tokens[4];
					rectangles.put(name, new double[]{x,y,w,h});
					rectangle_map.put(name, "");
				} else {
					points.add(val);
				}
			}
			
			double[] bounds = new double[1];
			double x_min = 0;
			double x_max = 0;
			double y_min = 0;
			double y_max = 0;
			double x = 0;
			double y = 0;
			// now we assign points to rectangles
			Set<String> names = rectangles.keySet();
			// we loop through the rectangles
			for (String name : names) {
				bounds = rectangles.get(name);
				x_min = bounds[0];
				x_max = bounds[0] + bounds[2];
				y_min = bounds[1] - bounds[3];
				y_max = bounds[1];
				// now we go through each of the points and find if it is contained in the current rectangle
				for (String point : points) {
					String[] tokens = point.split(",");
					x = Double.parseDouble(tokens[0]);
					y = Double.parseDouble(tokens[1]);
					// now we see if this point is in the current rectangle
					if ((x <= x_max && x >= x_min) && (y <= y_max && y >= y_min)) {
						// and if it is we add ;x,y to the end of that rectangles string
						rectangle_map.put(name, rectangle_map.get(name) + ";" + point);
					}
				}
				// finally we shave off the first semicolon from the string
				rectangle_map.put(name, rectangle_map.get(name).substring(1));
			}
			
			
			// now we send of the key value pairs
			for (String name : names) {
				out_key = new Text(name);
				out_val = new Text(rectangle_map.get(name));
				try {
					context.write(out_key, out_val);
				} catch(Exception e) {
				}
			}
		}
		
	}

}
