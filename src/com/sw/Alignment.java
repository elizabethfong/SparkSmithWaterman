package com.sw ;

import java.io.Serializable ;

import java.util.ArrayList ;
import java.util.Comparator ;

import org.apache.spark.api.java.function.VoidFunction ;

import scala.Tuple2 ;



@SuppressWarnings( "serial" ) 
public class Alignment 
{
	// CONSTANTS
	private static final String FILE_REF = "/home/ubuntu/project/testRef/vertebrate_mammalian.107.rna.fna" ;
	private static final String FILE_IN = "/home/ubuntu/project/testIn/test5.fa" ;
	
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
	 * @param args ( RefDir , InDir , OutputDir , outputFileName , Iterations , TimeFile )
	 */
	public static class DistributeAlgorithm implements VoidFunction<String[]>
	{
		public void call( String[] args )
		{
			/*
			DirectoryCrawler inDir = new DirectoryCrawler( args[1] ) ;
			
			while( inDir.hasNext() )
			{
				DirectoryCrawler refDir = DirectoryCrawler( )
				
				while( ref)
					*/
					// MAX - Bookkeeping
					int max = 0 ;
					ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> opt = new ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>>() ;
				
					ArrayList<String[]> refSeqs = new InOutOps.GetRefSeqs().call( FILE_REF , DELIMITER ) ;
					ArrayList<String> reads = new InOutOps.GetReads().call( FILE_IN , DELIMITER ) ;
									
					
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
					
					
					// print max to file
					opt.sort( new OptSeqsComp() ) ;
				/*}
			}*/
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
