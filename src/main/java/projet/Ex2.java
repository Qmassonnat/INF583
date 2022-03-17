package projet;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import scala.Tuple2;
import scala.Tuple3;


public class Ex2 {
	
	 public static void main( String[] args )
	    {
			Logger.getLogger("org").setLevel(Level.WARN);
			Logger.getLogger("akka").setLevel(Level.WARN);
		 
	    	String inputFile = "integers/matrix.txt";
	    	// Create a Java Spark Context
	    	SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("INF583 Projet");
	    	JavaSparkContext sc = new JavaSparkContext(conf);
	    	
	    	// Load our input data, with rows A, i, j, Aij
	    	JavaRDD<String> input = sc.textFile(inputFile);
	    	
	    	
	    	//Exercise 3 : Matrix Multiplication
	    	
	    	JavaPairRDD<Integer, Tuple3<String, Integer, Float>> mult = input.mapToPair((s) -> {
	    		String[] parts = s.split(",");
	    		if (parts[0].equals("A")) {
	    			Tuple3<String, Integer, Float> value = new Tuple3<>("A", Integer.parseInt(parts[1]), Float.parseFloat(parts[3]));
		    		return new Tuple2<> (Integer.parseInt(parts[2]), value);
	    		}
	    		else {
	    			Tuple3<String, Integer, Float> value = new Tuple3<>("B", Integer.parseInt(parts[2]), Float.parseFloat(parts[3]));
		    		return new Tuple2<> (Integer.parseInt(parts[1]), value);
	    		}
	    	});
	    	System.out.println("MAP1 ------------");
	    	mult.foreach(element -> {
				System.out.println(element._1 + ": " + element._2());
			});
	    	
	    	
	    	
	    	JavaPairRDD<Integer, Tuple3<String, Integer, Float>> partialMults = mult.reduceByKey((a, b) -> {
	    		System.out.println(a._1() + "  " + b._1());
	    		if (a._1().equals("A") &&  b._1().equals("B")) {
	    			System.out.println("a");
	    			return new Tuple3<> (a._2().toString(), b._2(), a._3() * b._3());
	    		}
	    		if (a._1().equals("B") &&  b._1().equals("A")) {
	    			return new Tuple3<> (b._2().toString(), a._2(), a._3() * b._3());
	    		}
	    		// if both values come from the same matrix we don't compute the product
	    		return new Tuple3<> (a._2().toString(), b._2(), new Float(0));
	    	});
	    	System.out.println("REDUCE1 ------------");
	    	partialMults.foreach(element -> {
				System.out.println(element._1 + ": " + element._2());

			});
	     	
	    	
	     	JavaPairRDD<Tuple2<Integer, Integer>, Float> temp = partialMults.mapToPair((a) -> {
	     		return new Tuple2<> (new Tuple2<>( Integer.parseInt(a._2()._1()), a._2()._2()), a._2()._3());
	     	});
	     	System.out.println("MAP2 ------------");
	     	temp.foreach(element -> {
				System.out.println(element._1 + ": " + element._2());

			});
	     	
	     	JavaPairRDD<Tuple2<Integer, Integer>, Float> product = temp.reduceByKey((a, b) -> a+b);
	     	System.out.println("REDUCE2 ------------");
	     	product.foreach(element -> {
				System.out.println(element._1 + ": " + element._2());

			});
	     	product.saveAsTextFile("matrixMult");
	    }
}
