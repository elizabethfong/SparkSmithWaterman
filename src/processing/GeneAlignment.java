package processing ;

//spark default imports
import org.apache.spark.api.java.JavaSparkContext ;		// entry pt for spark
import org.apache.spark.api.java.JavaRDD ;		// resilient distributed database
import org.apache.spark.SparkConf ;		// spark application config

import org.apache.spark.api.java.function.Function ;
import org.apache.spark.api.java.function.Function2 ;

import java.io.Serializable ;

import java.util.ArrayList ;
import java.util.Comparator ;

import scala.Tuple2 ;

public class GeneAlignment
{
	private static final String NEWLINE = System.lineSeparator() ;
	
	private static final String APP_NAME = "GeneAlignment" ;
	
	//private static final String DIR_FASTA = "/home/ubuntu/project/fasta" ;
	//private static final String DIR_REF = "/home/ubuntu/project/ncbi-refseq-rna" ;
	private static final String DIR_FASTA = "/home/ubuntu/project/testIn" ;
	private static final String DIR_REF = "/home/ubuntu/project/testRef" ;
	
	private static final String DELIMITER = ">gi" ;
	
	
	public static void main( String[] args )
	{
		// timekeeper
		long startTime = System.currentTimeMillis() ;
		
		// set spark application configurations
		SparkConf config = new SparkConf() ;
		config.setAppName( APP_NAME ) ;
		
		// entry pt for Spark - only 1 may exist
		JavaSparkContext sc = new JavaSparkContext( config ) ;
		
		
		// read fasta input!
		DirectoryCrawler inputDir = new DirectoryCrawler( DIR_FASTA ) ;
		DirectoryCrawler refDir = new DirectoryCrawler( DIR_REF ) ;
		
		// distribute reads instead - use less RAM?
		if( inputDir.hasNext() && refDir.hasNext() )
		{
			Fasta fasta = new Fasta( inputDir.next() ) ;
			RefSeqParser parser = new RefSeqParser( refDir.next() , DELIMITER ) ;
			
			int max = 0 ;
			ArrayList<RefSeq> opt = new ArrayList<RefSeq>() ;
			
			while( parser.hasNext() )
			{
				RefSeq ref = parser.next() ;
				ArrayList<Tuple2<String,String>> list = combine( ref , fasta ) ;
				
				// map the reads
				JavaRDD<Tuple2<String,String>> readsRDD = sc.parallelize(list) ;
				
				// apply sw function
				JavaRDD<Integer> swRDD = readsRDD.map( new MapReads() ) ;
				
				// reduce -> add all scores to get max alignment score
				int totalScore = swRDD.reduce( new TotalScore() ) ;
				
				// get alignments with max score
				if( totalScore > max )
				{
					max = totalScore ;
					opt.clear() ;
					opt.add(ref) ;
				}
				else if( totalScore == max )
				{
					opt.add(ref) ;
				}
			}
			
			// output
			System.out.println( "Max Score = " + max ) ;
			
			for( RefSeq r : opt )
			{
				System.out.println( r.getMetadata() ) ;
			}
		}
		
		// timekeeper
		long endTime = System.currentTimeMillis() ;
		long duration = endTime - startTime ;
		System.out.println( "Total Execution Time = " + duration + "ms" ) ;
		
		sc.close() ;
	}
	
	
	private static ArrayList<Tuple2<String,String>> combine( RefSeq seq , Fasta in )
	{
		String ref = seq.getSequence() ;
		ArrayList<String> reads = in.getReads() ;
		
		ArrayList<Tuple2<String,String>> list = new ArrayList<Tuple2<String,String>>() ;
		
		for( String read : reads )
		{
			list.add( new Tuple2<String,String>(ref,read) ) ;
		}
		
		return list ;
	}
	
	
	
	private static ArrayList<Tuple2<RefSeq,Fasta>> combine( ArrayList<RefSeq> seq , Fasta in )
	{
		ArrayList<Tuple2<RefSeq,Fasta>> list = new ArrayList<Tuple2<RefSeq,Fasta>>() ;
		
		for( RefSeq ref : seq )
		{
			list.add( new Tuple2<RefSeq,Fasta>(ref,in) );
		}
		
		return list ;
	}
	
	
	
	
	@SuppressWarnings( "serial" )
	private static class MapReads implements Function< Tuple2<String,String> , Integer >
	{
		@Override
		public Integer call( Tuple2<String,String> tuple )
		{
			return new SmithWaterman( tuple._1 , tuple._2 ).alignmentScore() ;
		}
	}
	
	@SuppressWarnings("serial")
	private static class MapRefSeq implements Function< Tuple2<RefSeq,Fasta> , Integer >
	{
		@Override
		public Integer call( Tuple2<RefSeq,Fasta> tuple )
		{
			int total = 0 ;
			
			String refSeq = tuple._1.getSequence() ;
			ArrayList<String> reads = tuple._2.getReads() ;
			
			for( String read : reads )
			{
				total += new SmithWaterman(refSeq,read).alignmentScore() ;
			}
			
			return new Integer(total) ;
		}
	}
	
	@SuppressWarnings( "serial" )
	private static class TotalScore implements Function2<Integer,Integer,Integer>
	{
		@Override
		public Integer call( Integer v1 , Integer v2 )
		{
			return v1 + v2 ;
		}
	}
	
	
	@SuppressWarnings("serial")
	private static class IntComparator implements Comparator<Integer>, Serializable
	{
		public int compare( Integer x , Integer y )
		{
			return x.intValue() - y.intValue() ;
		}
	}
}
