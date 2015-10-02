package com.sw ;

import java.io.Serializable ;

import java.util.ArrayList ;
import java.util.Comparator ;

import org.apache.spark.api.java.function.VoidFunction ;

import scala.Tuple2 ;
import scala.Tuple3 ;



@SuppressWarnings( "serial" ) 
public class Alignment 
{
	// CONSTANTS
	private static final String OUTPUT_EXT = ".txt" ;
	
	private static final int[] ALIGN_SCORES = {5,-3,-4} ;	// {match,mismatch,gap}
	private static final char[] ALIGN_TYPES = {'a','i','d','-'} ;
	private static final String DELIMITER = ">gi" ;
	
	private static final String APP_NAME = "Smith-Waterman in Spark" ;
	
	
	public static void main( String[] args )
	{
		// TODO main - code here
	}
	
	
	/**
	 * DISTRIBUTE ALGORITHM
	 * 
	 * @param args ( RefDir , InDir , OutputDir , outputFileName )
	 */
	public static class DistributeAlgorithm implements VoidFunction<String[]>
	{
		public void call( String[] args )
		{
			// VARIABLES
			DirectoryCrawler inDir = new DirectoryCrawler( args[1] ) ;
			DirectoryCrawler refDir ;
			
			int inputNum = 0;
			
			
			// RUN!!
			while( inDir.hasNext() )
			{	
				inputNum ++ ;
				
				ArrayList<String> reads = new InOutOps.GetReads().call( inDir.next() , DELIMITER ) ;
				
				int numReads = reads.size() ;
				int numRefs = 0 ;
				long execTime = System.currentTimeMillis() ;
				
				refDir = new DirectoryCrawler( args[0] ) ;
				
				
				// MAX - Bookkeeping
				int max = 0 ;
				ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> opt = new ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>>() ;
				
				
				// RUN!!
				while( refDir.hasNext() )
				{
					ArrayList<String[]> refSeqs = new InOutOps.GetRefSeqs().call( refDir.next() , DELIMITER ) ;
					numRefs += refSeqs.size() ;
					
					// COMPARISON
					for( String[] ref : refSeqs )
					{
						int total = 0 ;
						ArrayList<Tuple2<Integer,String[]>> matchSites = new ArrayList<Tuple2<Integer,String[]>>() ;
						
						for( String read : reads )
						{
							// sw
							String[] seq = { ref[1] , read } ;
							Tuple2<Integer,ArrayList<Tuple2<Integer,String[]>>> result = new DistributedSW.OptAlignments().call( seq , ALIGN_SCORES , ALIGN_TYPES ) ;
							
							// combine
							total += result._1().intValue() ;
							matchSites.addAll( result._2() ) ;
						}
						
						// get max ref
						if( total > max )
						{
							max = total ;
							
							opt.clear() ;
							matchSites.sort( new DistributedSW.MatchSiteComp() ) ; 
							opt.add( new Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>(ref,matchSites) ) ;
						}
						else if( total == max )
						{
							matchSites.sort( new DistributedSW.MatchSiteComp() ) ;
							opt.add( new Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>(ref,matchSites) ) ;
						}
						
					}
				}
				
				
				// print to file
				opt.sort( new OptSeqsComp() ) ;
				execTime = System.currentTimeMillis() - execTime ;
				
				int[] nums = { numRefs , numReads } ;
				Tuple3<int[],Integer,Long> tuple = new Tuple3<int[],Integer,Long>( nums , max , execTime ) ;
				
				String printStr = new InOutOps.GetOutputStr().call( reads , tuple , opt ) ;
				String filepath = args[2] + "/" + args[3] + inputNum + OUTPUT_EXT ;
				
				new InOutOps.PrintStrToFile().call( filepath , printStr ) ;
			}
		}
	}
	
	// DISTRIBUTE REFERENCE - TODO
	
	
	// DISTRIBUTE READS - TODO
	
	
	// NO DISTRIBUTION - TODO
	public static class NoDistribution
	{
		
	}
	
	/* --- UTILITY --------------------------------------------------------- */
	
	private static class OptSeqsComp implements Comparator<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> , Serializable
	{
		public int compare( Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>> t1 , Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>> t2 )
		{
			return t1._1()[0].compareTo( t2._1()[0] ) ;
		}
	}
}
