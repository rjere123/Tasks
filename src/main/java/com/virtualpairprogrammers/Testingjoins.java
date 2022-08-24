package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class Testingjoins {

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf =new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Tuple2<Integer,Integer>> visitsRaw = new ArrayList<>();
		
		visitsRaw.add(new Tuple2<>(4,10));
		visitsRaw.add(new Tuple2<>(6,18));
		visitsRaw.add(new Tuple2<>(8,12));
		visitsRaw.add(new Tuple2<>(10,14));
		
		List<Tuple2<Integer,String>> usersRaw = new ArrayList<>();
		
		usersRaw.add(new Tuple2<>(1,"John"));
		usersRaw.add(new Tuple2<>(4,"Sam"));
		usersRaw.add(new Tuple2<>(6,"Dorie"));
		usersRaw.add(new Tuple2<>(8,"Mike"));
		usersRaw.add(new Tuple2<>(8,"Peter"));
		usersRaw.add(new Tuple2<>(9,"MJ"));
	
		JavaPairRDD<Integer,Integer> visits = sc.parallelizePairs(visitsRaw);
		JavaPairRDD<Integer,String>  users = sc.parallelizePairs(usersRaw);
		
		//JavaPairRDD<Integer,Tuple2<Integer,String>> joined = visits.join(users);
		
		//JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRdd = visits.leftOuterJoin(users);
		
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinedRdd = visits.rightOuterJoin(users);
		
		joinedRdd.collect().forEach(it -> System.out.println(" User " +it._2._2+ " has " +it._2._1.orElse(0)+ " matches"));
	}

}
