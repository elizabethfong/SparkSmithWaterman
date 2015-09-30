package com.sw ;

import java.util.ArrayList ;

import org.apache.spark.SparkConf ;
import org.apache.spark.api.java.JavaRDD ;
import org.apache.spark.api.java.JavaSparkContext ;

import org.apache.spark.api.java.function.Function ;
import org.apache.spark.api.java.function.Function2 ;
import org.apache.spark.api.java.function.Function3 ;

import scala.Tuple2 ;
import scala.Tuple3 ;
import scala.Tuple4 ;
import scala.Tuple5 ;

@SuppressWarnings( "serial" ) 
public class DistributedSW 
{
	public static class OptAlignments
		implements Function3< String[] , int[] , char[] , Tuple2<Integer,ArrayList<Tuple2<String[],Integer>>> >
	{
		public Tuple2<Integer,ArrayList<Tuple2<String[],Integer>>> call( String[] seq , int[] alignScores , char[] alignTypes )
		{
			// VARIABLES
			String refSeq = seq[0] ;	// j
			String inSeq = seq[1] ;		// i
			
			int[][] scores = new int[inSeq.length()+1][refSeq.length()+1] ;
			char[][] aligns = new char[inSeq.length()+1][refSeq.length()+1] ;
			
			
			
			// score matrix
			
			
			return null ;
		}
	}
	
	private static class ScoreMatrix 
			  implements Function3< Tuple2<int[][],char[][]> , String[] , Tuple2<int[],char[]> , 
	 								Tuple4<int[][],char[][],ArrayList<int[]>,Integer> >
	{
		public Tuple4<int[][],char[][],ArrayList<int[]>,Integer> call( Tuple2<int[][],char[][]> matrices , 
																	   String[] seq , 
																	   Tuple2<int[],char[]> alignInfo )
		{
			// VARIABLES
			String refSeq = seq[0] ;	// j 	- necessary to split?
			String inSeq = seq[1] ;		// i	- necessary to split?
			
			int[][] scores = matrices._1() ;
			char[][] aligns = matrices._2() ;
			
			int[] alignScores = alignInfo._1() ;	// necessary to split?
			char[] alignTypes = alignInfo._2() ;	// necessary to split?
			
			JavaSparkContext sc = new JavaSparkContext( new SparkConf() ) ;
			
			// init matrices
			for( int i = 0 ; i < scores.length ; i++ )
			{
				for( int j = 0 ; j < scores[i].length ; j++ )
				{
					scores[i][j] = 0 ;
					aligns[i][j] = alignTypes[3] ;
				}
			}
			
			
			// DISTRIBUTED FILLING UP MATRIX
			int[] defaultScores = {-1,-1,-1} ;
			
			ArrayList<Tuple5<int[],int[],char[],int[],char[]>> list = new ArrayList<Tuple5<int[],int[],char[],int[],char[]>>(1) ;
			ArrayList<Tuple5<int[],int[],char[],int[],char[]>> children = new ArrayList<Tuple5<int[],int[],char[],int[],char[]>>(2) ;
			ArrayList<Tuple5<int[],int[],char[],int[],char[]>> grandchildren = new ArrayList<Tuple5<int[],int[],char[],int[],char[]>>(3) ;
			
			// { (i,j) , (NW,N,W) , (base_i,base_j) } -> first cell: (1,1) , (0,0,0)
			int[] coordinates = {1,1} ;
			int[] surrScores = {0,0,0} ;
			char[] bases = new GetBases().call( coordinates , seq ) ;
			
			Tuple2<int[],char[]> val1 = new Tuple2<int[],char[]>( surrScores , bases ) ;
			
			list.add( new GetCellMapData().call(coordinates,val1,alignInfo) ) ;
			
			while( list != null )
			{
				// parallelise
				JavaRDD<Tuple5<int[],int[],char[],int[],char[]>> distRDD = sc.parallelize(list) ;
				
				// map -> calculate score based on input
				JavaRDD<Tuple4<int[],Integer,Character,boolean[]>> scoreRDD = distRDD.map( new GetCellScore() ) ;
				
				// reduce -> distribute scores to children & grandchildren
				
				// calculate the next grandchild array length
			}
			
			
			sc.close() ;
			return null ;
		}
	}
	
	// return: coordinates, scores, bases, alignScores, alignTypes
	private static class GetCellMapData implements Function3< int[] , Tuple2<int[],char[]> , Tuple2<int[],char[]> , Tuple5<int[],int[],char[],int[],char[]> >
	{
		public Tuple5<int[],int[],char[],int[],char[]> call( int[] coordinates , Tuple2<int[],char[]> calcData , Tuple2<int[],char[]> alignInfo )
		{
			return new Tuple5<int[],int[],char[],int[],char[]>( coordinates , calcData._1() , calcData._2() , alignInfo._1() , alignInfo._2() ) ;
		}
	}
	
	private static class GetBases implements Function2< int[] , String[] , char[] >
	{
		public char[] call( int[] coordinates , String[] sequences )
		{
			char[] bases = { sequences[0].charAt(coordinates[1]) , sequences[1].charAt(coordinates[0]) } ;
			return bases ;
		}
	}
	
	
	/**
	 * 
	 * @param data ( coordinates, surroundingScores, bases, alignScores, alignTypes )
	 * @return ( coordinates , cellScore , cellAlignType , nextCells )
	 */
	private static class GetCellScore implements Function< Tuple5<int[],int[],char[],int[],char[]> , Tuple4<int[],Integer,Character,boolean[]> >
	{
		public Tuple4<int[],Integer,Character,boolean[]> call( Tuple5<int[],int[],char[],int[],char[]> data )
		{
			// do sth here
			
			
			
			return null ;
		}
	}
	
	
	
	public static void main( String[] args )
	{
		
	}
}
