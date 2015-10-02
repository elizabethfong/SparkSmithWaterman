package com.sw ;

import java.io.File ;

import java.util.ArrayList ;

import org.apache.spark.SparkConf ;
import org.apache.spark.api.java.JavaRDD ;
import org.apache.spark.api.java.JavaSparkContext ;

import org.apache.spark.api.java.function.Function ;

@SuppressWarnings( "serial" )
public class Debugging 
{
	// FILE VARIABLES
	final static String REF_240 = "/home/ubuntu/project/testRef/vertebrate_mammalian.415.rna.fna" ;
	final static String REF_121 = "/home/ubuntu/project/testRef/vertebrate_mammalian.432.rna.fna" ;
	final static String REF_85 = "/home/ubuntu/project/testRef/vertebrate_mammalian.107.rna.fna" ;
	final static String REF_70 = "/home/ubuntu/project/testRef/vertebrate_mammalian.123.rna.fna" ;
	final static String REF_50 = "/home/ubuntu/project/testRef/vertebrate_mammalian.436.rna.fna" ;
	final static String REF_30K = "/home/ubuntu/project/testRef/vertebrate_mammalian.129.rna.fna" ;
	
	final static String IN_240 = "/home/ubuntu/project/testIn/test1.fa" ;
	final static String IN_121 = "/home/ubuntu/project/testIn/test2.fa" ;
	final static String IN_85 = "/home/ubuntu/project/testIn/test5.fa" ;
	final static String IN_70 = "/home/ubuntu/project/testIn/test4.fa" ;
	final static String IN_50 = "/home/ubuntu/project/testIn/test3.fa" ;
	final static String IN_30K = "/home/ubuntu/project/testIn/test6.fa" ;
	
	final static String DELIMITER = ">gi" ;
	
	public static void main( String[] args )
	{
		long startTime = System.currentTimeMillis() ;
		
		final String refFile = REF_240 ;
		//final String in = IN_240 ;
		
		JavaSparkContext sc = new JavaSparkContext( new SparkConf() ) ;
		
		ArrayList<String[]> refSeq = new InOutOps.GetRefSeqs().call( new File(refFile) , DELIMITER ) ;
		JavaRDD<String[]> paraRDD = sc.parallelize(refSeq) ;
		JavaRDD<Integer> resultRDD = paraRDD.map( new CountSize() ) ;
		Integer sum = resultRDD.reduce( (a,b) -> a + b ) ;
		
		System.out.println( " sum = " + sum ) ;
		sc.close();
		
		long endTime = System.currentTimeMillis() ;
		long diff = endTime - startTime ;
		System.out.println( "exec time = " + diff ) ;
	}
	
	private static class CountSize implements Function<String[],Integer>
	{
		public Integer call( String[] ref )
		{
			return new Integer( ref[1].length() ) ;
		}
	}
}
