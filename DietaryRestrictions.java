package DietaryRestrictions;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;



public class DietaryRestrictions {

	public static class MapClass1 
	extends Mapper<Object, Text, Text, IntWritable>{

		private Text feel = new Text();
		private Text feelFood = new Text();
		private final static IntWritable one = new IntWritable(1);
		private ArrayList<String> foods = new ArrayList<String>();

		// key is filename, useless, value is file content, useful (duh)
		// iterates through all files in directory for us, v nice

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException{

			// Turn whole text into StringTokenizer
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			// Save each token in array of strings
			ArrayList<String> tokens = new ArrayList<String>();
			while(itr.hasMoreTokens())
			{
				tokens.add(itr.nextToken());
			}

			// Test to see if food
			if(tokens.get(0).equals("food")){
				for(int i = 0; i < tokens.size(); i++)
				{
					foods.add(tokens.get(i));
				}
				// Clear all tokens
				tokens.clear();
			}

			// Test to see if feel
			else if(tokens.get(0).equals("feel")){
				// Iterate through foods
				for(int i = 0; i < foods.size(); i+=4){
					// See if this feel is within 12 hours
					if(within12Hours(foods.get(i+2), tokens.get(2), foods.get(i+3), tokens.get(3))){
						// New association! Output (food + feel, 1)
						feel.set(tokens.get(1));
						String pair = feel.toString() + " " + foods.get(i+1);
						feelFood.set(pair);
						context.write(feelFood, one);
					} // end if 12 hours
				} //end for
				// Clear the tokens before moving on
				tokens.clear();
			} //end if feel
		} // END MAP1 FUNCTION
	} // END MAP1 CLASS

	public static class IntSumReducer1 
	extends Reducer<Text, IntWritable, Text, IntWritable>{

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class MapClass2 
	extends Mapper<Object, Text, Text, Text>{

		private Text feel = new Text("null");
		private Text foodNum = new Text();

		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			
			// Create new StringTokenizer from line and save tokens in ArrayList
			StringTokenizer itr = new StringTokenizer(value.toString());
			ArrayList<String> tokens = new ArrayList<String>();
			while(itr.hasMoreTokens()){
				tokens.add(itr.nextToken());
			}
			
			feel.set(tokens.get(0));
			foodNum.set(tokens.get(1) + " " + tokens.get(2));
			
			context.write(feel, foodNum);
			
			tokens.clear();
		}
	}

	public static class ReduceClass2 
	extends Reducer<Text, Text, Text, Text>{
	// Create 5 different temp values
			int i = 0;

			public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
				Text winner1 = new Text();
				Text winner2 = new Text();
				Text winner3 = new Text();
				Text winner4 = new Text();
				Text winner5 = new Text();
				Text temp = new Text();
				
				for(Text val : value)
				{
					if(i == 0){							//first iteration, set winner1 to val
						winner1.set(val);
					}
					else if(i == 1){					//2nd iteration, set winner2 to val, compare to winner1
						
						winner2.set(val);
						
						if(hasMoreOccurrences(val.toString(), winner1.toString()))
						{
							swap(winner1,winner2);
						}
					}
					else if(i == 2){					//3rd iteration, set winner3 to val, compare to winner1, compare to winner2
						winner3.set(val);
						
						if(hasMoreOccurrences(val.toString(), winner1.toString()))
						{
							swap(winner1,winner3);
						}
						
						else if(hasMoreOccurrences(val.toString(), winner2.toString()))
						{
							swap(winner2,winner3);
						}
					}
					else if(i == 3){
						winner4.set(val);
						
						if(hasMoreOccurrences(val.toString(), winner1.toString()))
						{
							swap(winner1,winner4);
						}
						
						else if(hasMoreOccurrences(val.toString(), winner2.toString()))
						{
							swap(winner2,winner4);
						}
						
						else if(hasMoreOccurrences(val.toString(), winner3.toString()))
						{
							swap(winner3,winner4);
						}
					}
					else if(i == 4){
						winner5.set(val);
						
						if(hasMoreOccurrences(val.toString(), winner1.toString()))
						{
							swap(winner1,winner5);
						}
						
						else if(hasMoreOccurrences(val.toString(), winner2.toString()))
						{
							swap(winner2,winner5);
						}
						
						else if(hasMoreOccurrences(val.toString(), winner3.toString()))
						{
							swap(winner3,winner5);
						}
						
						else if(hasMoreOccurrences(val.toString(), winner4.toString()))
						{
							swap(winner4,winner5);
						}
					}		
					else{																//else we are out of first 5: compare to 5 winners in descending order
						if(hasMoreOccurrences(val.toString(), winner1.toString()))
						{
							winner5.set(winner4);
							winner4.set(winner3);
							winner3.set(winner2);
							winner2.set(winner1);
							winner1.set(val);
						}
						else if(hasMoreOccurrences(val.toString(), winner2.toString()))
						{
							winner5.set(winner4);
							winner4.set(winner3);
							winner3.set(winner2);
							winner2.set(val);
						}
						else if(hasMoreOccurrences(val.toString(), winner3.toString()))
						{
							winner5.set(winner4);
							winner4.set(winner3);
							winner3.set(val);
						}
						else if(hasMoreOccurrences(val.toString(), winner4.toString()))
						{
							winner5.set(winner4);
							winner4.set(val);
						}
						else if(hasMoreOccurrences(val.toString(), winner5.toString()))
						{
							winner5.set(val);
						}
						
					}//end of else
					i++;
				}//end of for(val: value)
				
				System.out.println("1: " + winner1 + ", 2: " + winner2 +", 3: " + winner3 + ", 4: " + winner4 + ", 5: "+ winner5);
				
				context.write(key, winner1);
				context.write(key, winner2);
				context.write(key, winner3);
				context.write(key, winner4);
				context.write(key, winner5);
				i = 0;
		}
	}

	public static boolean within12Hours(String day1, String day2, String time1, String time2){
		final long TWELVEHRS = 43200000;

		boolean toReturn = false;

		String d1String = day1 +" " + time1;
		String d2String = day2 +" " + time2;

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm");

		try {
			Date date1 = sdf.parse(d1String);
			Date date2 = sdf.parse(d2String);

			long d1Millis = date1.getTime();
			long d2Millis = date2.getTime();
			long timeDif = d2Millis - d1Millis;

			if(timeDif <= TWELVEHRS){
				toReturn = true;
			}
			else{
				toReturn = false;
			}

		} catch (ParseException e) {
			e.printStackTrace();
		}

		return toReturn;
	}
	
	public static boolean hasMoreOccurrences(String food1, String food2){
		// Temp bool val
		boolean toReturn = false;
		
		String[] split1 = food1.split(" ");
		String[] split2 = food2.split(" ");
		
		int occurrences1 = Integer.parseInt(split1[1]);
		int occurrences2 = Integer.parseInt(split2[1]);
		
		if(occurrences1 > occurrences2){
			toReturn = true;
		}
		else{
			toReturn = false;
		}
		
		return toReturn;
	}
	
	public static void swap(Text val1, Text val2)
	{
		Text temp = new Text();
		
		temp.set(val1);
		val1.set(val2);
		val2.set(temp);
		
	}

	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "dietary restrictions");
		job.setJarByClass(DietaryRestrictions.class);
		job.setMapperClass(MapClass1.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/Users/brianfry/Downloads/input"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/brianfry/Desktop/mapredResults"));
		
		job.waitForCompletion(true);
		
		Job job2 = Job.getInstance(conf, "dietary restrictions");
    job2.setJarByClass(DietaryRestrictions.class);
    job2.setMapperClass(MapClass2.class);
    job2.setReducerClass(ReduceClass2.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path("/Users/brianfry/Desktop/mapredResults"));
    FileOutputFormat.setOutputPath(job2, new Path("/Users/brianfry/Desktop/FINAL"));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		System.out.println("Job Completed");
	}
}
