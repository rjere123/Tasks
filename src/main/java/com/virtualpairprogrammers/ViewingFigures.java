package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);
		
		//Step-0
		JavaPairRDD<Integer,Integer> chaptercountRdd = chapterData.mapToPair(row -> new Tuple2<Integer,Integer>(row._2,1))
				.reduceByKey((value1,value2) -> value1 + value2 );
		//chaptercountRdd.collect().forEach(System.out::println);
		
		//Step-1
		viewData =viewData.distinct();
		//viewData.collect().forEach(System.out::println);
		
		//Step-2
		JavaPairRDD<Integer,Integer> switched = viewData.mapToPair(row -> new Tuple2<Integer,Integer>(row._2,row._1));
	    JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedRdd = switched.join(chapterData);
	    
	    //joinedRdd.collect().forEach(System.out::println);
	    
	    //Step-3
	    JavaPairRDD<Tuple2<Integer, Integer>, Long> step3 = joinedRdd.mapToPair(row -> new Tuple2<Tuple2<Integer,Integer>,
	    		Long>(new Tuple2<Integer,Integer>(row._2._1,row._2._2),1L));
	    JavaPairRDD<Tuple2<Integer, Integer>, Long> newstep3 = step3.reduceByKey((value1,value2) -> value1+value2);
	    
	    //newstep3.collect().forEach(System.out::println);
	    
	    //Step-5
	    JavaPairRDD<Integer, Long> step4 = newstep3.mapToPair(row -> new Tuple2<Integer,Long>(row._1._2,row._2));
	    
	    //step4.collect().forEach(System.out::println);
	    
	    //Step-6
	    JavaPairRDD<Integer, Tuple2<Long, Integer>> step6 = step4.join(chaptercountRdd);
	    
	    //step6.collect().forEach(System.out::println);
	    
	    
	    //Step-7
	    JavaPairRDD<Integer,Double > step7 = step6.mapValues(value -> (double)value._1/value._2);
	    
	    //step7.collect().forEach(System.out::println);
	    
	    //Step-8
	    JavaPairRDD<Integer, Integer> step8 = step7.mapValues(value ->{
	    	if(value > 0.9) return 10;
	    	if(value > 0.5) return 4;
	    	if(value > 0.25) return 2;
	    	return 0;
	    });
	    
	    //step8.collect().forEach(System.out::println);
	    
	    //Step-9
	    JavaPairRDD<Integer , Integer> step9 = step8.reduceByKey((value1,value2) -> (int)(value1) + (int)(value2));
	    
	    //JavaPairRDD<Object, Integer> switchedstep9 = step9.mapToPair(row -> new Tuple2<Object ,Integer>(row._2,row._1));
	    
	    //JavaPairRDD<Object, Integer> sorted = switchedstep9.sortByKey(false);
	    
	    //sorted.collect().forEach(System.out::println);
	    
	    //Step-10
	    
	    JavaPairRDD<Integer, Tuple2<String, Integer>> step10 = titlesData.join(step9);
	    
	    step10.collect().forEach(System.out::println);
	    
	    Scanner scanner = new Scanner(System.in);
	    scanner.nextLine();
		
		sc.close();
	}

	
	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
