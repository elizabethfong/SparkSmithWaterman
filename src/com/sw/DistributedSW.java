package com.sw ;

import java.io.Serializable ;

import java.util.ArrayList ;
import java.util.Comparator ;
import java.util.List ;

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
import scala.Tuple6 ;

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
			
			
			// SCORE MATRIX
			Tuple2<int[][],char[][]> matrices = new Tuple2<int[][],char[][]>( scores , aligns ) ;
			Tuple2<int[],char[]> alignInfo = new Tuple2<int[],char[]>( alignScores , alignTypes ) ;
			
			Tuple2<Integer,ArrayList<int[]>> result = new ScoreMatrix().call( matrices , seq , alignInfo ) ;
			
			int maxScore = result._1() ;
			ArrayList<int[]> maxCells = result._2() ;
			
			
			// GET OPT ALIGNMENTS
			
			
			
			// RETURN!!!
			return null ;
		}
	}
	
	
	// DISTRIBUTED FILLING UP MATRIX
	private static class ScoreMatrix 
			  implements Function3< Tuple2<int[][],char[][]> , String[] , Tuple2<int[],char[]> , Tuple2<Integer,ArrayList<int[]>> >
	{
		public Tuple2<Integer,ArrayList<int[]>> call( Tuple2<int[][],char[][]> matrices , String[] seq , Tuple2<int[],char[]> alignInfo )
		{
			// VARIABLES
			int[][] scores = matrices._1() ;
			char[][] aligns = matrices._2() ;
			
			JavaSparkContext sc = new JavaSparkContext( new SparkConf() ) ;
			
			// init matrices
			for( int i = 0 ; i < scores.length ; i++ )
			{
				for( int j = 0 ; j < scores[i].length ; j++ )
				{
					scores[i][j] = 0 ;
					aligns[i][j] = alignInfo._2()[3] ;
				}
			}
			
			
			// INIT
			int[] maxIndices = { seq[1].length()-1 , seq[0].length()-1 } ;	// i,j -> in,ref
			Tuple2<int[],String[]> arrInfo = new Tuple2<int[],String[]>( maxIndices , seq ) ;
			
			ArrayList<Tuple2<int[][],char[][]>> list ;
			ArrayList<Tuple2<int[][],char[][]>> children ;
			ArrayList<Tuple2<int[][],char[][]>> grandchildren ;

			// default init tuple
			int[] coordinates = {1,1} ;
			int[] surrScores = {0,0,0} ;
			char[] bases = new char[2] ;
			
			int[][] mapParam1 = { coordinates , surrScores , alignInfo._1() , maxIndices } ;
			char[][] mapParam2 = { bases , alignInfo._2() } ;
			
			Tuple2<int[][],char[][]> defaultTuple = new Tuple2<int[][],char[][]>( mapParam1 , mapParam2 ) ;
			
			// init lists
			list = new GetInitList().call( coordinates , defaultTuple , arrInfo ) ;
			
			int[] nextCoord = {2,1} ;
			children = new GetInitList().call( nextCoord , defaultTuple , arrInfo ) ;
			
			nextCoord = new GetNextStart().call( coordinates , maxIndices ) ;
			grandchildren = new GetInitList().call( nextCoord , defaultTuple, arrInfo ) ;
			
			
			// DISTRIBUTE!!!
			int maxScore = 0 ;
			ArrayList<int[]> maxCells = new ArrayList<int[]>() ;
			
			while( list != null )
			{
				// parallelise
				JavaRDD<Tuple2<int[][],char[][]>> distRDD = sc.parallelize(list) ;
				
				// map -> calculate score based on input
				JavaRDD<Tuple4<int[],Integer,Character,boolean[]>> scoreRDD = distRDD.map( new GetCellScore() ) ;
				
				
				// reduce -> broadcast scores to children & grandchildren
				List<Tuple4<int[],Integer,Character,boolean[]>> newScores = scoreRDD.collect() ;
				newScores.sort( new CellResultComp() ) ;
				
				new Broadcast().call( newScores , children , grandchildren ) ;
				
				
				// update matrices & max
				for( Tuple4<int[],Integer,Character,boolean[]> cell : newScores )
				{
					int[] coord = cell._1() ;
					int score = cell._2().intValue() ;
					
					// update matrices
					int i = coord[0] ;
					int j = coord[1] ;
					
					scores[i][j] = score ;
					aligns[i][j] = cell._3().charValue() ;
					
					// bookkeeping -> max
					if( score > maxScore )
					{
						maxScore = score ;
						
						maxCells.clear() ;
						maxCells.add(coord) ;
					}
					else if( score == maxScore )
					{
						maxCells.add(coord) ;
					}
				}
				
				
				// update lists
				int[] start = list.get(0)._1()[0] ;
				
				list = children ;
				children = grandchildren ;
				
				start = new GetNextStart().call( start , maxIndices ) ;
				grandchildren = new GetInitList().call( start , defaultTuple , arrInfo ) ;
			}
			
			
			// RETURN!!!
			sc.close() ;
			return new Tuple2<Integer,ArrayList<int[]>>( new Integer(maxScore) , maxCells ) ;
		}
	}
	
	private static class GetCellScore implements Function< Tuple2<int[][],char[][]> , Tuple4<int[],Integer,Character,boolean[]> >
	{
		public Tuple4<int[],Integer,Character,boolean[]> call( Tuple2<int[][],char[][]> data )
		{
			// PARAMETERS
			int[] coordinates = data._1()[0] ;
			int[] surrScores = data._1()[1] ;		// nw, n, w
			char[] bases = data._2()[0] ;
			int[] alignScores = data._1()[2] ;		// match, mismatch, gap
			char[] alignTypes = data._2()[1] ;		// alignment, insertion, deletion, none
			int[] maxIndices = data._1()[3] ;
			
			
			// GET MAX SCORE
			int max = 0 ;
			char type = alignTypes[3] ;
			
			// deletion
			int score = new InsDelScore().call(surrScores[2],alignScores[2]).intValue() ;
			if( score > max )
			{
				max = score ;
				type = alignTypes[2] ;
			}
			
			// insertion
			score = new InsDelScore().call(surrScores[1],alignScores[2]).intValue() ;
			if( score > max )
			{
				max = score ;
				type = alignTypes[1] ;
			}
			
			// alignment
			score = new AlignmentScore().call(surrScores[0],bases,alignScores) ;
			if( score > max )
			{
				max = score ;
				type = alignTypes[0] ;
			}
			
			
			// CALC WHICH CELLS TO SEND RESULT TO
			boolean[] nextCells = {true,true,true} ;	// se,s,e
			
			// at bottom row - i
			if( coordinates[0] == maxIndices[0] )
			{
				nextCells[1] = false ;
				nextCells[0] = false ;
			}
			
			// right edge - j
			if( coordinates[1] == maxIndices[1] )
			{
				nextCells[2] = false ;
				nextCells[0] = false ;
			}
			
			
			// RETURN
			return new Tuple4<int[],Integer,Character,boolean[]>( coordinates , new Integer(max) , new Character(type) , nextCells ) ;
		}
	}
	
	// does not return anything!
	private static class Broadcast implements Function3< List<Tuple4<int[],Integer,Character,boolean[]>> , 
			  											 ArrayList<Tuple2<int[][],char[][]>> , 
			  											 ArrayList<Tuple2<int[][],char[][]>> , Boolean >
	{
		public Boolean call( List<Tuple4<int[],Integer,Character,boolean[]>> cells , 
							 ArrayList<Tuple2<int[][],char[][]>> children , 
							 ArrayList<Tuple2<int[][],char[][]>> grandchildren )
		{
			for( Tuple4<int[],Integer,Character,boolean[]> cell : cells )
			{
				int[] coord = cell._1() ;
				int score = cell._2() ;
				boolean[] nextCells = cell._4() ;
				
				// se -> grandchild -> alignment
				if( nextCells[0] )
				{
					int gStartJ = grandchildren.get(0)._1()[0][1] ;
					int index = coord[1] + 1 - gStartJ ;
					
					grandchildren.get(index)._1()[1][0] = score ;
				}
				
				// s -> child -> insertion
				if( nextCells[1] )
				{
					int cStartJ = children.get(0)._1()[0][1] ;
					int index = coord[1] - cStartJ ;
					
					children.get(index)._1()[1][1] = score ;
				}
				
				// e -> child -> deletion
				if( nextCells[2] )
				{
					int cStartJ = children.get(0)._1()[0][1] ;
					int index = coord[1] + 1 - cStartJ ;
					
					children.get(index)._1()[1][2] = score ;
				}
			}
			
			return null ;
		}
	}
	
	
	// DISTRIBUTED GETTING OPT ALIGNMENTS
	private static class GetAlignments 
			  implements Function3< ArrayList<int[]> , Tuple2<int[][],char[][]> , Tuple2<String[],char[]> , ArrayList<Tuple2<String[],Integer>> >
	{
		public ArrayList<Tuple2<String[],Integer>> call( ArrayList<int[]> starts , Tuple2<int[][],char[][]> matrices , Tuple2<String[],char[]> otherData )
		{
			// VARIABLES
			int[][] scores = matrices._1() ;
			char[][] aligns = matrices._2() ;
			
			String refSeq = otherData._1()[0] ;
			String inSeq = otherData._1()[1] ;
			
			
			// DISTRIBUTE ALIGNMENT START CELLS
			
			
			return null ;
		}
	}
	
	
	/* --- UTIL ------------------------------------------------------------ */
	
	private static class GetBases implements Function2< int[] , String[] , char[] >
	{
		public char[] call( int[] coordinates , String[] sequences )
		{
			char[] bases = { sequences[0].charAt(coordinates[1]) , sequences[1].charAt(coordinates[0]) } ;
			return bases ;
		}
	}
	
	private static class InsDelScore implements Function2< Integer , Integer , Integer >
	{
		public Integer call( Integer surrScore , Integer gapScore )
		{
			return surrScore + gapScore ;
		}
	}
	
	private static class AlignmentScore implements Function3< Integer , char[] , int[] , Integer >
	{
		public Integer call( Integer surrScore , char[] bases , int[] alignScores )
		{
			if( Character.toUpperCase(bases[0]) == Character.toUpperCase(bases[1]) )
				return surrScore + alignScores[0] ;
			else
				return surrScore + alignScores[1] ;
		}
	}
	
	private static class CellResultComp implements Comparator<Tuple4<int[],Integer,Character,boolean[]>> , Serializable
	{
		public int compare( Tuple4<int[],Integer,Character,boolean[]> t1 , Tuple4<int[],Integer,Character,boolean[]> t2 )
		{
			return t1._1()[1] - t2._1()[1] ;
		}
	}
	
	private static class GetInitList 
			  implements Function3< int[] , Tuple2<int[][],char[][]> , Tuple2<int[],String[]> , ArrayList<Tuple2<int[][],char[][]>> >
	{
		public ArrayList<Tuple2<int[][],char[][]>> call( int[] start , Tuple2<int[][],char[][]> defaultTuple , Tuple2<int[],String[]> arrInfo )
		{
			// corner case: no grandchildren list
			if( start == null )
			{
				return null ;
			}
			
			
			// array info
			int[] maxIndices = arrInfo._1() ;
			String[] seq = arrInfo._2() ;
			
			
			// calculate coordinates of last cell in this list
			int[] end = { 1 , start[1]+start[0]-1 } ;
			
			if( end[1] > maxIndices[1] )
			{
				end[0] += end[1] - maxIndices[1] ;
				end[1] = maxIndices[1] ;
			}
			
			int len = end[1] - start[1] + 1 ;
			
			
			// init list
			ArrayList<Tuple2<int[][],char[][]>> list = new ArrayList<Tuple2<int[][],char[][]>>(len) ;
			
			for( int i = 0 ; i < list.size() ; i++ )
				list.add( new DeepCopyTuple().call(defaultTuple) ) ;
			
			
			// update coordinates and bases of each tuple
			for( int i = 0 ; i < list.size() ; i++ )
			{
				Tuple2<int[][],char[][]> tuple = list.get(i) ;
				
				int[] coordinates = { start[0]-i , start[1]-i } ;
				tuple._1()[0] = coordinates ;
				
				char[] bases = new GetBases().call( coordinates , seq ) ;
				tuple._2()[0] = bases ;
			}
			
			return list  ;
		}
	}
			  
	private static class DeepCopyTuple implements Function< Tuple2<int[][],char[][]> , Tuple2<int[][],char[][]> >
	{
		public Tuple2<int[][],char[][]> call( Tuple2<int[][],char[][]> tuple )
		{
			int[][] origIntArr = tuple._1() ;
			char[][] origCharArr = tuple._2() ;
			
			int[][] newIntArr = new int[origIntArr.length][] ;
			char[][] newCharArr = new char[origCharArr.length][] ;
			
			// clone int[][]
			for( int i = 0 ; i < origIntArr.length ; i++ )
			{
				int[] orig = origIntArr[i] ;
				int[] arr = new int[orig.length] ;
				
				for( int j = 0 ; j < arr.length ; j++ )
					arr[j] = orig[j] ;
				
				newIntArr[i] = arr ;
			}
			
			// clone char[][]
			for( int i = 0 ; i < origCharArr.length ; i++ )
			{
				char[] orig = origCharArr[i] ;
				char[] arr = new char[orig.length] ;
				
				for( int j = 0 ; j < arr.length ; j++ )
					arr[j] = orig[j] ;
				
				newCharArr[i] = arr ;
			}
			
			// return
			return new Tuple2<int[][],char[][]>( newIntArr , newCharArr ) ;
		}
	}
	
	private static class GetNextStart implements Function2< int[] , int[] , int[] >
	{
		public int[] call( int[] start , int[] maxIndices )
		{
			int nextI = start[0] + 2 ;
			int nextJ = start[1] ;
			
			if( nextI > maxIndices[0] )
			{
				nextJ += nextI - maxIndices[0] ;
				nextI = maxIndices[0] ;
			}
			
			// corner case: no grandchildren list
			if( nextJ > maxIndices[1] )
			{
				return null ;
			}
			
			int[] next = { nextI , nextJ } ;
			return next ;
		}
	}
	
	
	
	public static void main( String[] args )
	{
		
	}
}
