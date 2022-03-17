package projet;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class matMult {
	
	public class matMultMapper extends  Mapper<Text, Text, Text, Text>
	{
	  private String matrixName = "";
	  private int    stepNumber = 0;
	
	  public void map(Text key, Text value, Context context, Reporter reporter) throws IOException, InterruptedException
	  {
		  System.out.println(key); 
	      System.out.println(value);
	    if (stepNumber == 0)
	    {
	      String[] values = value.toString().trim().split(" ");
	      for (int column = 0; column < values.length; column++)
	      {
	        if (matrixName.equalsIgnoreCase("A"))
	        {
	          context.write(new Text(Integer.toString(column)), new Text(matrixName + "," + key.toString() + "," + values[column]));
	        }
	        else
	        {
	          context.write(key, new Text(matrixName + "," + Integer.toString(column) + "," + values[column]));
	        }
	      }
	    }
	    else
	    {
	      String[] tokens = value.toString().split(",");
	      String newKey = tokens[0] + "," + tokens[1];
	      String newValue = tokens[2];
	
	      context.write(new Text(newKey), new Text(newValue));
	    }
	  }
	};
	
	public class matMultReducer extends Reducer<Text, Text, Text, Text>
	{
	
	  private int stepNumber = 0;
	
	  public void reduce(Text key, Iterator<Text> values, Context context, Reporter reporter)
	      throws IOException, NumberFormatException, InterruptedException
	  {
	    if (stepNumber == 0)
	    {
	      ArrayList<String> valuesGroupedByA = new ArrayList<String>();
	      ArrayList<String> valuesGroupedByB = new ArrayList<String>();
	
	      while (values.hasNext())
	      {
	        String value = values.next().toString();
	        (value.startsWith("A") ? valuesGroupedByA : valuesGroupedByB).add(value);
	      }
	
	      Iterator<String> iteratorA = valuesGroupedByA.iterator();
	      while (iteratorA.hasNext())
	      {
	        String[] tokensA = iteratorA.next().split(",");
	
	        Iterator<String> iteratorB = valuesGroupedByB.iterator();
	        while (iteratorB.hasNext())
	        {
	          String[] tokensB = iteratorB.next().split(",");
	
	          context.write(
	              key,
	              new Text(tokensA[1] + "," + tokensB[1] + "," + (Double.parseDouble(tokensA[2]) * Double.parseDouble(tokensB[2]))));
	        }
	      }
	    }
	    else
	    {
	      Double newValue = new Double(0);
	      while (values.hasNext())
	      {
	        newValue += Double.parseDouble(values.next().toString());
	      }
	      System.out.println(key); 
	      System.out.println(new Text(newValue.toString()));
	      context.write(key, new Text(newValue.toString()));
	    }
	  }
	};
	

	public static void main(String[] args) throws Exception {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		
		Configuration conf = new Configuration();
		String input = "input";
		String output1 = "matMult";	 
		Job job1 = Job.getInstance(conf, "matMult");
		job1.setJarByClass(matMult.class);
		job1.setMapperClass(matMultMapper.class);
		job1.setReducerClass(matMultReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(input));
		FileOutputFormat.setOutputPath(job1, new Path(output1));
	};


};