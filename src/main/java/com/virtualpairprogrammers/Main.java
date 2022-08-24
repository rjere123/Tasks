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

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
			
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
			                                   .config("spark.sql.warehouse.dir","file:///c:/tmp/")
			                                   .getOrCreate();
			
	     
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/task_3/News_Final.csv");
		
		dataset.createOrReplaceTempView("news");
		
		Dataset<Row> news_cleaned = spark.sql("Select * FROM news "
				+ "                    WHERE "
				+ "                    LENGTH(Title)>12"
				+ "                    AND Topic IN('obama','economy','microsoft','palestine')");
		
		//news_cleaned.coalesce(1).write().option("header", true).format("csv").save("src/main/resources/task_3/News_cleaned.csv");
		
		//news_cleaned.show();
		
		Dataset<Row> id_1 = spark.sql("Select IDLink from news WHERE "
				+ "           LENGTH(Title)<=12");
		
		//id_1.coalesce(1).write().option("header", true).csv("src/main/resources/task_3/Rule_1_reject_log.csv");
		
		Dataset<Row> id_2 = spark.sql("Select IDLink from news WHERE "
				+ "           NOT Topic IN('obama','economy','microsoft','palestine')");
		
		//id_2.coalesce(1).write().option("header", true).csv("src/main/resources/task_3/Rule_2_reject_log.csv");
		
		news_cleaned.createOrReplaceTempView("news_cleaned");
		
		long count_news = dataset.count();
		long count_news_cleaned = news_cleaned.count();
		
		System.out.println("Total number of records in the original file  : " +count_news);
		System.out.println("Total number of records after applying business rules  : " +count_news_cleaned);
		
		Dataset<Row> scores_headlines = spark.sql("Select Topic, date_format(PublishDate,'y') as year,"
				+ "    ROUND(AVG(SentimentHeadline),0) as Headline_sentiment_score"
				+ "    from  news_cleaned "
				+ "    group by Topic,year"
				+ "    order by Headline_sentiment_score desc ");
		
		System.out.println("Year-wise average Sentiment score of the text in the news items' headline for each topic");
		
		scores_headlines.show(5);
		
		Dataset<Row> scores_titles = spark.sql("Select Topic, date_format(PublishDate,'y') as year,"
				+ "    ROUND(AVG(SentimentTitle),0) as Title_sentiment_score"
				+ "    from  news_cleaned "
				+ "    group by Topic,year"
				+ "    order by Title_sentiment_score desc ");
			
		System.out.println("Year-wise average Sentiment score of the text in the news items' title for each topic");
		
		scores_titles.show(5);
		
		Dataset<Row> total = spark.sql("Select Topic,ROUND(ABS((Facebook+LinkedIn+GooglePlus)/3),0) as avg_final_popularity_value from news_cleaned"); 
		
		total.createOrReplaceTempView("rank_table");
		
		Dataset<Row> top10items = spark.sql("Select RANK() over ( order by avg_final_popularity_value desc) as rank,* from rank_table ");
		
        System.out.println("Top 10 most popular items by average Final value of the news items' popularity on Facebook, GooglePlus, LinkedIn");
        
        top10items.show(10);
		
		Dataset<Row> facebook = spark.read().option("header", true).csv("src/main/resources/task_3/Facebook_Microsoft.csv");
		Dataset<Row> googleplus= spark.read().option("header", true).csv("src/main/resources/task_3/GooglePlus_Economy.csv");
		
		facebook.createOrReplaceTempView("facebook_table");
		googleplus.createOrReplaceTempView("googleplus_table");
				
		Dataset<Row> feedback_facebook = spark.sql("Select * from facebook_table JOIN news ON news.IDLink = facebook_table.IDLink "
				+ "WHERE date_format(news.PublishDate, 'y') = 2015 AND news.Topic = 'economy'");
		
		Dataset<Row> drop_news_id_1 = feedback_facebook.drop(col("news.IDLink"));
		
		//drop_news_id_1.show();
		
		Dataset<Row> feedback_googleplus = spark.sql("Select * from googleplus_table JOIN news ON news.IDLink = googleplus_table.IDLink where "
				+ "date_format(news.PublishDate, 'y') = 2015 AND news.Topic = 'economy'");
		
		Dataset<Row> drop_news_id_2 = feedback_googleplus.drop(col("news.IDLink"));
		 
		//drop_news_id_2.show();
		
		drop_news_id_1.createOrReplaceTempView("feedback_facebook");
		drop_news_id_2.createOrReplaceTempView("feedback_googleplus");
		
		Dataset<Row> final_feedback= spark.sql("Select * from feedback_googleplus UNION select * from feedback_facebook");
		
		//final_feedback.show();
		
		//final_feedback.coalesce(1).write().option("header", true).csv("src/main/resources/task_3/economy_social_feedback_2015.csv");
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		//dataset.createOrReplaceTempView("my_current_table");
		
//		Dataset<Row> results = spark.sql("Select level,date_format(datetime,'MMMM') as month ,date_format(datetime,'M') as monthnum "
//				+ "                                   from my_current_table "
//				+ "                                   group by level,month,monthnum"
//				+ "                                   order by monthnum,level");
//		
        
//		dataset = dataset.select(col("level"),
//				date_format(col("datetime"),"MMMM").alias("month"),
//				date_format(col("datetime"),"M").alias("monthnum"));
//		
//		
		//dataset = dataset.groupBy(col("level"),col("month"),col("monthnum")).count();
		//dataset = dataset.orderBy(col("level"),col("monthnum"));
		
//		Object[] months = new Object[] {"January","February","March","April","May","June","July","August","September","October","November","December"};
//		
//		List<Object>columns = Arrays.asList(months);
//		
//		dataset = dataset.groupBy("level").pivot("month",columns).count().na().fill(0);
//		
		//dataset = dataset.groupBy("subject").agg(max("score").cast(DataTypes.IntegerType).alias("max"),min("score").alias("min"));
		
//		dataset = dataset.groupBy("subject").pivot("year").agg(round(functions.avg(col("score")),2).alias("average"),
//				                                     round(functions.avg(col("score")),2).alias("Stddev"));
//		dataset.show(100);
//		
 		
 		
 		
 		
 		
 		
 		
 		
 		
 		
 		
 		
 		
 		
 		
 		
 		
		

		/*List<Row> inMemory = new ArrayList<>();
//			
			inMemory.add(RowFactory.create("WARN", "2020-12-28 04:15:30"));
			inMemory.add(RowFactory.create("FATAL","2020-10-20 05:15:30"));
			inMemory.add(RowFactory.create("FATAL","2016-10-15 08:15:30"));
			inMemory.add(RowFactory.create("WARN", "2015-11-16 04:16:30"));
			inMemory.add(RowFactory.create("TURN", "2014-10-15 04:15:28"));
			
		StructField[] fields= new StructField[] {	
//					
					new StructField("level",DataTypes.StringType,false,Metadata.empty()),
					new StructField("datetime",DataTypes.StringType,false,Metadata.empty())
			};
//					
//					
			StructType schema = new StructType(fields);
			Dataset<Row> dataset = spark.createDataFrame(inMemory , schema);
//		*/	
		
		
		
		
		//Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		//dataset.show();
		
		//long numberOfRows = dataset.count();
		//System.out.println("No of rows :" +numberOfRows);
		
		//Row firstRow = dataset.first();
		
		//String subject = firstRow.getAs("subject").toString();
		//System.out.println("The subject is :" +subject);
		
		//int year = Integer.parseInt(firstRow.getAs("year").toString());
		//System.out.println("The year is :" +year);
		
		//Dataset<Row> modernArtsFilter = dataset.filter(" subject = 'Modern Art' AND year >= 2006 ");
		
		//modernArtsFilter.show();
		
		//Dataset<Row> modernArtsresults = dataset.filter(row -> row.getAs("subject").equals("Modern Art") && Integer.parseInt(row.getAs("year"))>=2006);
		
	    //Column subject = col("subject");
	    //Column year = col("year");
	    
	    //Dataset<Row> modernArtsresults = dataset.filter(subject.equalTo("Modern Art")
	    //		                                                .and(year.geq(2007)));

		
		//Dataset<Row> modernArtsresults = dataset.filter(col("subject").equalTo("Modern Art")
                //.and(col("year").geq(2007)));
		
		//modernArtsresults.show();
		
		//dataset.createOrReplaceTempView("my_current_table");
		
		//Dataset<Row> results = spark.sql("Select subject,year from my_current_table where year =2008");
		
		//results.show();
		
		
		
	    spark.close();
		
		
	}
}
		
		
//		SparkConf conf =new SparkConf().setAppName("startingSpark").setMaster("local[*]");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//		
//		//---------------------------------------------Task -1---------------------------------------------------//
//	
//		long startTime_1 = System.currentTimeMillis();
//			
//		JavaRDD<String> reviewRddfile = sc.textFile("src/main/resources/reviews/reviews.txt");
//				
//		JavaRDD<String> reviews = reviewRddfile.flatMap(sentence -> Arrays.asList(sentence.split("<br /><br />")).iterator());
//		
//		JavaRDD<String> noblankreviews=reviews.map(review -> review.trim());
//
//		//System.out.println(noblankreviews.take(5));
//		
//		//reviews.collect().forEach(System.out::println); 
//		
//		System.out.println("There are "+noblankreviews.count()+ " records in the review.txt file");
////		
//		System.out.println("There are "+reviews.getNumPartitions()+ " partitions for this spark cluster");
////		
//		long endTime_1  =  System.currentTimeMillis();
////		
//		long exectime_1 =  startTime_1-endTime_1;
////		
////		System.out.println("The TAT for Task-1 is :" +exectime_1+ " ms");
////		
////		
////		//-------------------------------------------Task -2---------------------------------------------------------//
////		
//		long startTime_2 = System.currentTimeMillis();
////		
//		//JavaRDD<String> removedSpecialCharRdd = noblankreviews.map(sentance -> sentance.replaceAll("[$&+,:;=?@#|'<>.-^*()%!]+", "_"));
////		
//		JavaRDD<String> removedSpecialCharRdd = noblankreviews.map(sentance -> sentance.replaceAll("[^a-zA-Z]+", ""));
//				
//		//System.out.println(removedSpecialCharRdd.take(100));
//		
////		//System.out.println(removedSpecialCharRdd);
////		
//		//reviewscleanedtrimmed.collect().forEach(System.out :: println);
////		
//        long endTime_2  =  System.currentTimeMillis();
////		
//	    long exectime_2 =  startTime_2-endTime_2;
////		
////		//System.out.println("The TAT for Task-2 is :" +exectime_2+ " ms");
////		
////		//-------------------------------------------Task -3------------------------------------------------------//
////		
////		
//		long startTime_3 = System.currentTimeMillis();
////		
//        JavaRDD<String>  noNumericReviews = removedSpecialCharRdd.filter(sentence -> sentence.matches("^[a-zA-z]*$+"));
////         
//        //System.out.println("The number of reviews with no numeric character in them are :" +noNumericReviews.count());
////         
//        //System.out.println(noNumericReviews.take(100));
////          
//        long endTime_3  =  System.currentTimeMillis();
////		
//	    long exectime_3 =  startTime_3-endTime_3;
////		
////		//System.out.println("The TAT for Task-3 is :" +exectime_3+ " ms");
////		
////		 
////        //----------------------------------------------------Task -4---------------------------------------------------//
////         
//		long startTime_4 = System.currentTimeMillis();
////		
//        JavaRDD<String> reviewword = removedSpecialCharRdd.flatMap(review ->  Arrays.asList(review.split("_")).iterator());
//        
//        JavaRDD<String> movie = reviewword.filter(word -> word.equals("movie"));
//        
//        //System.out.println("The wordcount for movie is : " +movie.count());
////         
//        long endTime_4  =  System.currentTimeMillis();
//// 		
// 		long exectime_4 =  startTime_4-endTime_4;
//// 		
//// 		  
////       //--------------------------------------------------Task -5---------------------------------------------------//
//// 		 
// 		 long startTime_5 = System.currentTimeMillis();
//// 		
//         JavaPairRDD<String, Long> pairRdd = removedSpecialCharRdd.mapToPair(review -> new Tuple2<String, Long>(review, (long) review.length()));
////       
//         JavaPairRDD<Long, String> switchedpairRdd = pairRdd.mapToPair(row -> new Tuple2<Long, String>(row._2,row._1));
//         
//         JavaPairRDD<Long, String> switchedpairRddcleaned=switchedpairRdd.filter(row -> (row._2).length()>0);
//        
//         JavaPairRDD<Long, String> sorteddesc = switchedpairRddcleaned.sortByKey(false);
////         
//         JavaPairRDD<Long, String> sortedasc = switchedpairRddcleaned.sortByKey(true);
////      
//         //sorteddesc.collect().forEach(System.out::println);
//         
//         JavaRDD<Long> sorteddesckeys = sorteddesc.keys();
////         
//         JavaRDD<Long> sortedasckeys = sortedasc.keys();
////         
//         System.out.println("The Max length of the review : "+sorteddesckeys.first());
////         
//         System.out.println("The Min length of the review is : "+sortedasckeys.first());
////        
//         long endTime_5  =  System.currentTimeMillis();
////  		
// 		 long exectime_5 =  startTime_5-endTime_5;
//// 		
//// 		     
////      //--------------------------------------------------Task -6-------------------------------------------------//
////         
//// 		 
// 		 long startTime_6 = System.currentTimeMillis();
//		
//         removedSpecialCharRdd.saveAsTextFile("reviews_cleansed.txt.");
//		
//         long endTime_6  =  System.currentTimeMillis();
//   		
// 		 long exectime_6 =  startTime_6-endTime_6;
// 		
// 		 //System.out.println("The TAT for Task-5 is :" +exectime_6+ " ms");
//        
//			
// 	 //-------------------------------------------------------------------------------------------------------//
// 		 
// 		 TreeMap<String,Long> TAT = new TreeMap<>();
// 		 
// 		 TAT.put("Task_1",Math.abs(exectime_1));
// 		 TAT.put("Task_2",Math.abs(exectime_2));
// 		 TAT.put("Task_3",Math.abs(exectime_3));
// 		 TAT.put("Task_4",Math.abs(exectime_4));
// 		 TAT.put("Task_5",Math.abs(exectime_5));
// 	 	 TAT.put("Task_6",Math.abs(exectime_6));
// 		 
// 	 	 System.out.println("The TAT for all the tasks is :" +TAT);
// 	 	 
// 	 	 Scanner scanner = new Scanner(System.in);
// 	 	 scanner.nextLine();
// 	 	 
// 		 sc.close();
//		
//		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		

		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		/*
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN : Tuesday 4 september 0405");
		inputData.add("ERROR : Tuesday 4 september 0408");
		inputData.add("FATAL:Wednesday 8 september 0409");
		inputData.add("ERROR : Wednesday 7 september 1632");
		inputData.add("WARN : Tuesday 4 september 1844");
		*/
	
		
		
		
		
		
		
		
//		
		
		
		
		
		
		
		
		
		/*
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		
		JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentance -> sentance.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
		
		JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentance -> sentance.trim().length() > 0);
		
		JavaRDD<String> onlyWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
		
		JavaRDD<String> removedBlankwords = onlyWords.filter(word -> word.trim().length() > 0);
		
		JavaRDD<String> interestingWords = removedBlankwords.filter(word -> Util.isNotBoring(word));
		
		JavaPairRDD<String, Long> pairRdd = interestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));
		
		JavaPairRDD<String,Long> totals = pairRdd.reduceByKey((value1,value2) -> value1+value2);
		
		JavaPairRDD<Long,String> switched = totals.mapToPair(tuple -> new Tuple2<Long,String>(tuple._2 ,tuple._1));
		
		JavaPairRDD<Long,String> sorted = switched.sortByKey(false);
		
		//sorted = sorted.coalesce(1);
		
		//sorted.collect().forEach(System.out :: println);
		
		List<Tuple2<Long,String>> result = sorted.take(10);
		result.forEach(System.out :: println);
		
	/*	initialRdd
		.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
		.collect().forEach(System.out :: println);
	*/
		
		//JavaRDD<String> sentences = sc.parallelize(inputData);
		/*
		sc.parallelize(inputData)
		.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
		.filter(word -> word.length() > 1 )
		.collect().forEach(System.out :: println);
		*/
		//JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
		
		//JavaRDD<String> filteredWords = words.filter(word -> word.length() > 1 );
		
		//filteredWords.collect().forEach(System.out :: println);
		
		//sc.close();
		
	//}

//}








//sqrtRdd.foreach( System.out::println );

//System.out.println(myRdd.count());

//Integer result = myRdd.reduce((value1,value2) -> (value1+value2));
//sqrtRdd.foreach( value -> System.out.println(value));//
/*
sqrtRdd.map(value -> 1 );
JavaRDD<Integer> singleIntegerCount = myRdd.map(value -> 1);
Integer count =  singleIntegerCount.reduce((value1,value2) -> (value1+value2));
*/


//JavaRDD<IntegertoSquareRoot> sqrtRdd= origanalIntegers.map(value -> new IntegertoSquareRoot(value));

//Tuple2<Integer,Double> myValue = new Tuple2<>(9,3.0);

//JavaRDD<Tuple2<Integer,Double>> sqrtRdd= origanalIntegers.map(value -> new Tuple2<>(value , Math.sqrt(value)));

//IntegertoSquareRoot iws = new IntegertoSquareRoot(9);//

/*
 
JavaPairRDD<String,String> pairRdd = originalLogMessages.mapToPair(rawValue -> {
         
	String[] columns = rawValue.split(":");
	String level = columns[0];
	String date = columns[1];
	
	return new Tuple2<>(level,date);
			
});

sc.parallelize(inputData)
.mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0],1L))
.reduceByKey((value1,value2) -> value1 + value2)
.foreach(tuple -> System.out.println("The key "+ " " +tuple._1+ " " +"has"+" " +tuple._2+ " " + "instances"));

JavaPairRDD<String,Long> pairRdd = originalLogMessages.mapToPair(rawValue -> {
    
	String[] columns = rawValue.split(":");
	String level = columns[0];
	String date = columns[1];
	
	return new Tuple2<>(level,1L);
			
});

JavaPairRDD<String,Long> sumsRdd = pairRdd.reduceByKey((value1,value2) -> value1 + value2);

sumsRdd.foreach(tuple -> System.out.println("The key "+ " " +tuple._1+ " " +"has"+" " +tuple._2+ " " + "instances"));
*/
