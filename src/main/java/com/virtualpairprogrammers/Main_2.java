package com.virtualpairprogrammers;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

import scala.Tuple2;

import org.apache.hadoop.fs.*;

public class Main_2 {

	public static void main(String[] args) {
		
	    System.setProperty("hadoop.home.dir", "c:/hadoop");
	    
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf =new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
//		
//		//---------------------------------------------Task -1---------------------------------------------------//
	
		long startTime_1 = System.currentTimeMillis();
//			
		JavaRDD<String> reviewRddfile = sc.textFile("src/main/resources/reviews/reviews.txt");
//				
     	JavaRDD<String> reviews = reviewRddfile.flatMap(sentence -> Arrays.asList(sentence.split("<br /><br />")).iterator());
//		
	    JavaRDD<String> noblankreviews=reviews.map(review -> review.trim());
//
//		//System.out.println(noblankreviews.take(5));
//		
//		//reviews.collect().forEach(System.out::println); 
//		
		System.out.println("There are "+noblankreviews.count()+ " records in the review.txt file");
////		
		System.out.println("There are "+reviews.getNumPartitions()+ " partitions for this spark cluster");
////		
		long endTime_1  =  System.currentTimeMillis();
////		
		long exectime_1 =  startTime_1-endTime_1;
////		
		System.out.println("The TAT for Task-1 is :" +exectime_1+ " ms");
////		
////		
////		//-------------------------------------------Task -2---------------------------------------------------------//
////		
		long startTime_2 = System.currentTimeMillis();
////		
		//JavaRDD<String> removedSpecialCharRdd = noblankreviews.map(sentance -> sentance.replaceAll("[$&+,:;=?@#|'<>.-^*()%!]+", "_"));
////		
		JavaRDD<String> removedSpecialCharRdd = noblankreviews.map(sentance -> sentance.replaceAll("[^a-zA-Z]+", ""));
//				
//		System.out.println(removedSpecialCharRdd.take(100));
//		
////		//System.out.println(removedSpecialCharRdd);
////		
//		//reviewscleanedtrimmed.collect().forEach(System.out :: println);
////		
        long endTime_2  =  System.currentTimeMillis();
////		
	    long exectime_2 =  startTime_2-endTime_2;
////		
////		//System.out.println("The TAT for Task-2 is :" +exectime_2+ " ms");
////		
////		//-------------------------------------------Task -3------------------------------------------------------//
////		
////		
		  long startTime_3 = System.currentTimeMillis();
////		
          JavaRDD<String>  noNumericReviews = removedSpecialCharRdd.filter(sentence -> sentence.matches("^[a-zA-z]*$+"));
////         
          System.out.println("The number of reviews with no numeric character in them are :" +noNumericReviews.count());
////         
//        //System.out.println(noNumericReviews.take(100));
////          
          long endTime_3  =  System.currentTimeMillis();
////		
	      long exectime_3 =  startTime_3-endTime_3;
////		
////		//System.out.println("The TAT for Task-3 is :" +exectime_3+ " ms");
////		
////		 
////        //----------------------------------------------------Task -4---------------------------------------------------//
////         
		long startTime_4 = System.currentTimeMillis();
////		
        JavaRDD<String> reviewword = removedSpecialCharRdd.flatMap(review ->  Arrays.asList(review.split("_")).iterator());
//        
        JavaRDD<String> movie = reviewword.filter(word -> word.equals("movie"));
//        
        System.out.println("The wordcount for movie is : " +movie.count());
////         
        long endTime_4  =  System.currentTimeMillis();
//// 		
 		long exectime_4 =  startTime_4-endTime_4;
//// 		
//// 		  
////       //--------------------------------------------------Task -5---------------------------------------------------//
//// 		 
 		   long startTime_5 = System.currentTimeMillis();
//// 		
         JavaPairRDD<String, Long> pairRdd = removedSpecialCharRdd.mapToPair(review -> new Tuple2<String, Long>(review, (long) review.length()));
////       
         JavaPairRDD<Long, String> switchedpairRdd = pairRdd.mapToPair(row -> new Tuple2<Long, String>(row._2,row._1));
//         
         JavaPairRDD<Long, String> switchedpairRddcleaned=switchedpairRdd.filter(row -> (row._2).length()>0);
//        
         JavaPairRDD<Long, String> sorteddesc = switchedpairRddcleaned.sortByKey(false);
////         
         JavaPairRDD<Long, String> sortedasc = switchedpairRddcleaned.sortByKey(true);
////      
         sorteddesc.collect().forEach(System.out::println);
//         
         JavaRDD<Long> sorteddesckeys = sorteddesc.keys();
////         
         JavaRDD<Long> sortedasckeys = sortedasc.keys();
////         
         System.out.println("The Max length of the review : "+sorteddesckeys.first());
////         
         System.out.println("The Min length of the review is : "+sortedasckeys.first());
////        
         long endTime_5  =  System.currentTimeMillis();
////  		
 		 long exectime_5 =  startTime_5-endTime_5;
//// 		
//// 		     
////      //--------------------------------------------------Task -6-------------------------------------------------//
////         
//// 		 
 		 long startTime_6 = System.currentTimeMillis();
//		
//         removedSpecialCharRdd.saveAsTextFile("reviews_cleansed.txt.");
//		
         long endTime_6  =  System.currentTimeMillis();
//   		
 		 long exectime_6 =  startTime_6-endTime_6;
// 		
// 		 //System.out.println("The TAT for Task-5 is :" +exectime_6+ " ms");
//        
//			
// 	 //-------------------------------------------------------------------------------------------------------//
// 		 
 		 TreeMap<String,Long> TAT = new TreeMap<>();
// 		 
 		 TAT.put("Task_1",Math.abs(exectime_1));
 		 TAT.put("Task_2",Math.abs(exectime_2));
 		 TAT.put("Task_3",Math.abs(exectime_3));
 		 TAT.put("Task_4",Math.abs(exectime_4));
 		 TAT.put("Task_5",Math.abs(exectime_5));
 	 	 TAT.put("Task_6",Math.abs(exectime_6));
// 		 
// 	 	 System.out.println("The TAT for all the tasks is :" +TAT);
// 	 	 
 	 	 Scanner scanner = new Scanner(System.in);
 	 	 scanner.nextLine();
// 	 	 
 		 sc.close();
//		
		
		
	}

}
