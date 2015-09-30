package com.sw ;

import java.io.BufferedWriter ;
import java.io.File ;
import java.io.FileNotFoundException ;
import java.io.FileWriter ;
import java.io.IOException ;
import java.io.Serializable ;

import java.lang.StringBuilder ;

import java.util.ArrayList ;
import java.util.Comparator ;
import java.util.List ;
import java.util.Scanner ;

import org.apache.spark.SparkConf ;
import org.apache.spark.api.java.JavaPairRDD ;
import org.apache.spark.api.java.JavaRDD ;
import org.apache.spark.api.java.JavaSparkContext ;

import org.apache.spark.api.java.function.Function2 ;
import org.apache.spark.api.java.function.Function3 ;
import org.apache.spark.api.java.function.PairFunction ;
import org.apache.spark.api.java.function.VoidFunction ;


import scala.Tuple2 ;
import scala.Tuple3 ;
import scala.Tuple5 ;

@SuppressWarnings( "serial" )
public class GeneAlignment 
{
	public static void main( String[] args )
	{
		// CONSTANTS
		final String FILE_REF = "/home/ubuntu/project/testRef/vertebrate_mammalian.436.rna.fna" ;
		final String FILE_IN = "/home/ubuntu/project/testIn/test3.fa" ;
		
		final int[] ALIGN_SCORES = {5,-3,-4} ;	// {match,mismatch,gap}
		final char[] ALIGN_TYPES = {'a','i','d','-'} ;
		final String DELIMITER = ">gi" ;
		
		final String APP_NAME = "Smith-Waterman in Spark" ;
		
		// timekeeper
		long startTime = System.currentTimeMillis() ;
		
		// init spark
		SparkConf config = new SparkConf() ;
		config.setAppName( APP_NAME ) ;
		JavaSparkContext sc = new JavaSparkContext( config ) ;
		
		// get reference sequences, reads
		ArrayList<String[]> refSeqs = new GetRefSeq().call( new File(FILE_REF) , DELIMITER ) ;
		ArrayList<String> reads = new GetReads().call( new File(FILE_IN) , DELIMITER ) ;
		
		int[] sizes = { refSeqs.size() , reads.size() } ;
		
		// parallelise reads
		Tuple3<JavaSparkContext,int[],char[]> info = new Tuple3<JavaSparkContext,int[],char[]>( sc , ALIGN_SCORES , ALIGN_TYPES ) ;
		Tuple2<Integer,List<Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>>> result = new Distribute().call( refSeqs , reads , info ) ;
		
		info = null ;
		refSeqs = null ;
		sc.close() ;
		
		// timekeeper
		long endTime = System.currentTimeMillis() ;
		long execTime = endTime - startTime ;
		
		// print result
		Tuple5<Long,int[],ArrayList<String>,Integer,List<Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>>> toPrint = 
				new Tuple5<Long,int[],ArrayList<String>,Integer,List<Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>>>( execTime , sizes , reads , result._1() , result._2() ) ;
		
		reads = null ;
		result = null ;
		
		new PrintResult().call(toPrint) ; 
	}
	
	
	private static class PrintResult implements VoidFunction<Tuple5<Long,int[],ArrayList<String>,Integer,List<Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>>>>
	{
		public void call( Tuple5<Long,int[],ArrayList<String>,Integer,List<Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>>> tuple )
		{
			final String OUTPUT_FILE = "results.txt" ;
			final String NEWLINE = System.lineSeparator() ;
			
			StringBuilder str = new StringBuilder() ;
			
			// data from tuple
			long executionTime = tuple._1().longValue() ;
			int[] sizes = tuple._2() ;
			ArrayList<String> reads = tuple._3() ;
			Integer maxScore = tuple._4() ;
			List<Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>> seqs = tuple._5() ;
			tuple = null ;
			
			// print sizes
			str.append( "Num Reference Sequences = " + sizes[0] + NEWLINE ) ;
			str.append( "Num Reads = " + sizes[1] + NEWLINE ) ;
			str.append( NEWLINE ) ;
			
			// print input
			str.append( "Input:" + NEWLINE ) ;
			for( String read : reads )
			{
				str.append( read ) ;
				str.append( NEWLINE ) ;
			}
			str.append( NEWLINE ) ;
			reads = null ;
			
			// print execution time
			str.append( "Execution Time = " + executionTime + NEWLINE ) ;
			str.append( NEWLINE ) ;
			
			// print max score
			str.append( "Alignment Score = " + maxScore ) ;
			str.append( NEWLINE ) ;
			maxScore = null ;
			
			// print sequences and binding sites
			for( Tuple2<String[],ArrayList<Tuple2<String[],Integer>>> seq : seqs )
			{
				// tuple information
				String[] refSeq = seq._1() ;
				ArrayList<Tuple2<String[],Integer>> sites = seq._2() ;
				sites.sort( new BindingSiteSorter() ) ;
				
				// print ref seq info
				str.append( refSeq[0] + NEWLINE ) ;
				str.append( refSeq[1] + NEWLINE ) ;
				str.append( NEWLINE ) ;
				
				// print binding sites
				for( Tuple2<String[],Integer> site : sites )
				{
					str.append( "Index = " + site._2() + NEWLINE ) ;
					str.append( site._1()[0] + NEWLINE ) ;
					str.append( site._1()[1] + NEWLINE ) ;
					str.append( NEWLINE ) ;
				}
				
				str.append( NEWLINE ) ;
			}
			seqs = null ;
			
			// print to results file
			try
			{
				File file = new File( OUTPUT_FILE ) ;
				
				if( ! file.exists() )
					file.createNewFile() ;
				
				BufferedWriter writer = new BufferedWriter( new FileWriter(file.getAbsoluteFile(),false) ) ;
				
				writer.write( str.toString() ) ;
				writer.close() ;
			}
			catch( IOException ioe )
			{
				System.out.println( "IOException on writing to file" ) ;
				ioe.printStackTrace() ;
			}
		}
	}
	
	private static class BindingSiteSorter implements Comparator<Tuple2<String[],Integer>> , Serializable
	{
		public int compare( Tuple2<String[],Integer> tuple1 , Tuple2<String[],Integer> tuple2 )
		{
			return tuple1._2().intValue() - tuple2._2().intValue() ;
		}
	}
	
	/* --- Distribution ---------------------------------------------------- */
	
	/**
	 * Distribute Reference Dataset
	 * 
	 * @param refSeqs
	 * @param reads
	 * @param sc JavaSparkContext
	 * 
	 * @return the result string
	 */
	private static class Distribute 
			  implements Function3< ArrayList<String[]> , ArrayList<String> , Tuple3<JavaSparkContext,int[],char[]> , Tuple2<Integer,List<Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>>> >
	{
		public Tuple2<Integer,List<Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>>> call( ArrayList<String[]> refSeqs , ArrayList<String> reads , Tuple3<JavaSparkContext,int[],char[]> info )
		{
			// from info
			JavaSparkContext sc = info._1() ;
			Tuple2<int[],char[]> alignInfo = new Tuple2<int[],char[]>( info._2() , info._3() ) ;
			info = null ;
			
			// combine ref and reads to one array list (distributing ref set)
			ArrayList<Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>>> list = new ArrayList<Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>>>() ;
			
			for( String[] ref : refSeqs )
				list.add( new Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>>(ref,reads,alignInfo) ) ;
			
			refSeqs = null ;
			reads = null ;
			
			// map the reference set
			JavaRDD<Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>>> refRDD = sc.parallelize(list) ;
			list = null ;
			
			// apply sw function
			JavaPairRDD<Integer,Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>> swRDD = refRDD.mapToPair( new MapRef() ) ;
			
			// extract info and reduce -> hopefully actions
			Integer max = swRDD.keys().max( new IntComparator() ) ;
			List<Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>> maxSeq = swRDD.lookup(max) ;
			swRDD = null ;
			
			// return!
			return new Tuple2<Integer,List<Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>>>( max , maxSeq ) ;
		}
	}
	
	private static class MapRef 
			  implements PairFunction< Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>> , Integer, Tuple2<String[],ArrayList<Tuple2<String[],Integer>>> >
	{
		public Tuple2<Integer,Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>> call( Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>> tuple )
		{
			// vars from tuple
			String[] ref = tuple._1() ;
			ArrayList<String> reads = tuple._2() ;
			int[] alignScores = tuple._3()._1() ;
			char[] alignTypes = tuple._3()._2() ;
			tuple = null ;
			
			// run sw algorithm
			int totalScore = 0 ;
			ArrayList<Tuple2<String[],Integer>> bindingPts = new ArrayList<Tuple2<String[],Integer>>() ;
			
			for( String read : reads )
			{
				String[] seqs = { ref[1] , read } ;
				Tuple2<Integer,ArrayList<Tuple2<String[],Integer>>> result = new SmithWaterman.OptAlignments().call( seqs , alignScores , alignTypes ) ;
				
				totalScore += result._1().intValue() ;
				bindingPts.addAll( result._2() ) ;
			}
			
			reads = null ;
			alignScores = null ;
			alignTypes = null ;
			
			// return
			Integer key = new Integer(totalScore) ;
			Tuple2<String[],ArrayList<Tuple2<String[],Integer>>> value = new Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>( ref , bindingPts ) ;
			
			ref = null ;
			bindingPts = null ;
			
			return new Tuple2<Integer,Tuple2<String[],ArrayList<Tuple2<String[],Integer>>>>( key , value ) ;
		}
	}
	
	private static class IntComparator implements Comparator<Integer> , Serializable
	{
		public int compare( Integer val1 , Integer val2 )
		{
			return val1.intValue() - val2.intValue() ;
		}
	}
	
	
	/* --- File Reading ---------------------------------------------------- */
	
	private static class GetReads implements Function2<File,String,ArrayList<String>>
	{
		public ArrayList<String> call( File file , String delim )
		{
			// reads
			ArrayList<String> reads = new ArrayList<String>() ;
			
			try
			{
				Scanner scanner = new Scanner( file ) ;
				
				// first line - may contain metadata
				String line = scanner.nextLine().trim() ;
				
				if( ! new IsMetadata().call(line,delim).booleanValue() )
					reads.add( line ) ;
				
				while( scanner.hasNextLine() )
					reads.add( scanner.nextLine().trim() ) ;
				
				scanner.close() ;
			}
			catch( FileNotFoundException fnfe )
			{
				System.out.println( "Input file not found." ) ;
				System.exit(0) ;
			}
			
			return reads ;
		}
	}
	
	private static class GetRefSeq implements Function2<File,String,ArrayList<String[]>>
	{
		public ArrayList<String[]> call( File file , String delim )
		{
			ArrayList<String[]> sequences = new ArrayList<String[]>() ;
			
			try
			{
				Scanner scanner = new Scanner( file ) ;
				
				String[] ref = null ;
				StringBuilder seq = null ;
				
				// each line
				while( scanner.hasNextLine() )
				{
					String line = scanner.nextLine() ;
					
					if( new IsMetadata().call(line,delim).booleanValue() )
					{
						if( ref != null )
						{
							ref[1] = seq.toString() ;
							sequences.add(ref) ;
							
							seq = null ;
							ref = null ;
						}
						
						ref = new String[2] ;
						ref[0] = line ;
						seq = new StringBuilder() ;
					}
					else
					{
						seq.append( line ) ;
					}
				}
				
				// last line
				ref[1] = seq.toString() ;
				sequences.add(ref) ;
				
				seq = null ;
				ref = null ;
				
				scanner.close() ;
			}
			catch( FileNotFoundException fnfe )
			{
				System.out.println( "Reference file not found" ) ;
				System.exit(0) ;
			}
			
			return sequences ;
		}
	}
	
	private static class IsMetadata implements Function2<String,String,Boolean>
	{
		public Boolean call( String str , String delim )
		{
			if( str.length() >= delim.length() && str.substring(0,delim.length()).equals(delim) )
				return true ;
			else
				return false ;
		}
	}
}
