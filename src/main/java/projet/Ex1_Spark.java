package projet;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import scala.Tuple2;


public class Ex1_Spark {
	 public static void main( String[] args )
	    {
			Logger.getLogger("org").setLevel(Level.WARN);
			Logger.getLogger("akka").setLevel(Level.WARN);
		 
	    	String inputFile = "integers/integers.txt";
	    	// Create a Java Spark Context
	    	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("INF583 Projet");
	    	JavaSparkContext sc = new JavaSparkContext(conf);
	    	
	    	// Load our input data.
	    	JavaRDD<String> input = sc.textFile(inputFile);
	    	
	    	//Exercise 1: Compute the largest integer
	    	JavaRDD<Integer> ints = input.map(s -> {return Integer.parseInt(s);});
	    	
	    	Integer maxint = ints.reduce((a,b) -> Math.max(a,b));
	    			
	    	System.out.println(maxint);
	    	
	    	
	    	// Exercise 2 : Compute the mean of the integers
	    	
	    	JavaRDD<Tuple2<Integer, Integer>> integers = input.map(s -> new Tuple2<>(Integer.parseInt(s), 1));
	    	    	
	     	
	     	// We sum the ratings and the values of 1
	     	Tuple2<Integer, Integer> sumInts = integers.reduce((a, b)-> new Tuple2<>(a._1 + b._1, a._2 + b._2));
	     	
	     	//We compute the average using the sum and the count of the integers	
	     	float mean = (float) sumInts._1 / (float) sumInts._2;
	     	System.out.println(mean);
	     	
	     	
	     	// Exercise 3 : Compute the set of integers
	     	// Exercise 4 : Compute the number of distinct integers
	     	
	     	JavaPairRDD<Integer, Integer> int3 = input.mapToPair(s -> new Tuple2<>(Integer.parseInt(s), 1));
	     	
	     	JavaPairRDD<Integer, Integer> setInts = int3.reduceByKey((a, b) -> a + b );
	     	
	     	setInts.keys().saveAsTextFile("exercise3");
	     	setInts.saveAsTextFile("exercise4");
	     	
	     	
	     	
	     	
	    	
	    }
}
