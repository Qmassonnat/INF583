package projet;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import scala.Tuple2;
import scala.Serializable;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import java.util.Properties;



public class Ex1_Streaming {
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		
	Logger.getLogger("org").setLevel(Level.WARN);
	Logger.getLogger("akka").setLevel(Level.WARN);

	SparkConf sparkConf = new SparkConf () . setMaster ("local[*]") . setAppName ("Ex1_Streaming") ;
	JavaStreamingContext jssc = new JavaStreamingContext ( sparkConf , Durations.seconds(1) ) ;
	JavaSparkContext jsc = new JavaSparkContext(jssc.ssc().sc()) ;
	FileReader fr = new FileReader ( new File ("integers/integers.txt") ) ;

	BufferedReader br = new BufferedReader ( fr ) ;
	String line ;
	int count = 0;
	ArrayList < String > batch = new ArrayList < String >() ;
	Queue < JavaRDD < String > > rdds = new LinkedList < >() ;
	while (( line = br . readLine () ) != null ) {
	count +=1;
	if ( count == 10)
	{
	JavaRDD < String > rdd = jsc . parallelize ( batch ) ;
	rdds . add ( rdd ) ;
	batch = new ArrayList < String >() ;
	count = 0;
	}
	batch . add ( line ) ;
	}
	JavaRDD < String > rdd = jsc . parallelize ( batch ) ;
	rdds . add ( rdd ) ;
	JavaDStream < String > stream = jssc . queueStream ( rdds , true ) ;

	//Exercise 1: Compute the largest integer
	
	JavaDStream<Integer> ints = stream.map(a -> (Integer.parseInt(a)));
	JavaDStream<Integer> maxint = ints.reduce((a,b) -> Math.max(a,b));
	
	maxint.print();
	
	// Exercise 2 : Compute the mean of the integers
	
	JavaDStream<Tuple2<Integer, Integer>> integers = stream.map(s -> new Tuple2<>(Integer.parseInt(s), 1));
	    	
 	
 	// We sum the ratings and the values of 1
 	JavaDStream<Tuple2<Integer, Integer>> sumInts = integers.reduce((a, b)-> new Tuple2<>(a._1 + b._1, a._2 + b._2));
 	
 	//We compute the average using the sum and the count of the integers
 	JavaDStream<Double> mean = sumInts.map(a -> new Double((float)a._1 / (float)a._2));
 	mean.print();

    jssc.start();
    
    
	
    try {jssc.awaitTermination();} catch (InterruptedException e) {e.printStackTrace();}
	 }
}
