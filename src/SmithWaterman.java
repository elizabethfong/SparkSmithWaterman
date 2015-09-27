

import org.apache.spark.SparkConf ;

import org.apache.spark.api.java.JavaRDD ;
import org.apache.spark.api.java.JavaSparkContext ;

import org.apache.spark.api.java.function.Function ;
import org.apache.spark.api.java.function.Function2 ;
import org.apache.spark.api.java.function.Function3 ;

import scala.Tuple2 ;
import scala.Tuple4 ;

import java.lang.StringBuilder ;

import java.util.ArrayList ;
import java.util.Stack ;


@SuppressWarnings( "serial" ) 
public class SmithWaterman 
{
	
	/**
	 * 
	 * @param seq
	 * @param alignScore
	 * @param alignTypes
	 * 
	 * @return 
	 */
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
			
			
			// step 1: score matrix
			Tuple2<int[][],char[][]> matrices = new Tuple2<int[][],char[][]>( scores , aligns ) ;
			Tuple2<int[],char[]> alignInfo = new Tuple2<int[],char[]>( alignScores , alignTypes ) ;
			
			Tuple4<int[][],char[][],ArrayList<int[]>,Integer> result1 = 
					new ScoreMatrix().call( matrices , seq , alignInfo ) ;
			
			
			// step 2: backtrack from cells w/ max scores to find opt alignments
			matrices = new Tuple2<int[][],char[][]>( result1._1() , result1._2() ) ;
			ArrayList<int[]> maxCells = result1._3() ;
			Tuple2<String[],char[]> matrixInfo = new Tuple2<String[],char[]>( seq , alignTypes ) ;
			
			
				SmithWaterman.printMatrices(matrices._1(), matrices._2(), seq);
			
			
			ArrayList<Tuple2<String[],Integer>> opt = new ArrayList<Tuple2<String[],Integer>>(maxCells.size()) ;
			
			for( int[] cell : maxCells )
			{
				opt.add( new GetAlignment().call(cell,matrixInfo,matrices) ) ;
			}
			
			return new Tuple2<Integer,ArrayList<Tuple2<String[],Integer>>>( result1._4() , opt ) ;
		}
	}
	
	/**
	 * 
	 * @param matrices {scoreMatrix,alignTypeMatrix}
	 * @param seq {refSeq,inSeq}
	 * @param alignInfo { {Match,Mismatch,Gap} , {align,insert,delete} }
	 *
	 * @return cells with max score
	 */
	private static class ScoreMatrix 
			 implements Function3< Tuple2<int[][],char[][]> , String[] , Tuple2<int[],char[]> , 
			 					   Tuple4<int[][],char[][],ArrayList<int[]>,Integer> >
	{
		public Tuple4<int[][],char[][],ArrayList<int[]>,Integer> call( Tuple2<int[][],char[][]> matrices , 
																	   String[] seq , 
																	   Tuple2<int[],char[]> alignInfo )
		{
			// variables
			String refSeq = seq[0] ;	// j
			String inSeq = seq[1] ;		// i
			
			int[][] scores = matrices._1() ;
			char[][] aligns = matrices._2() ;
			
			
			// init matrices
			char noAlign = alignInfo._2[3] ;
			
			for( int i = 0 ; i < scores.length ; i++ )
			{
				for( int j = 0 ; j < scores[i].length ; j++ )
				{
					scores[i][j] = 0 ;
					aligns[i][j] = noAlign ;
				}
			}
			
			
			// keep track of cells with max score
			ArrayList<int[]> maxCells = new ArrayList<int[]>() ;
			int maxScore = 0 ;
			
			// score the matrix!
			for( int i = 1 ; i < scores.length ; i++ )
			{
				for( int j = 1 ; j < scores[i].length ; j++ )
				{
					// get cell score
					int[] cells = { scores[i-1][j-1] , scores[i-1][j] , scores[i][j-1] } ;
					char[] bases = { refSeq.charAt(j-1) , inSeq.charAt(i-1) } ;
					
					Tuple2<Integer,Character> cellScore = 
							new GetCellScore().call( cells , bases, alignInfo ) ;
					
					// update matrices
					int score = cellScore._1().intValue() ;
					scores[i][j] = score ;
					aligns[i][j] = cellScore._2().charValue() ;
					
					// max score bookeeping
					int[] coordinate = {i,j} ;
					
					if( score > maxScore )	// new max score
					{
						maxCells.clear() ;
						maxCells.add(coordinate) ;
						maxScore = score ;
					}
					else if( score == maxScore )  // another opt alignment
					{
						maxCells.add(coordinate) ;
					}
				}
			}
			
			return new Tuple4<int[][],char[][],ArrayList<int[]>,Integer>( scores , aligns , maxCells , new Integer(maxScore) ) ;
		}
	}
	
	
	/**
	 * 
	 * 
	 * @param cellScores {NW,N,W}
	 * @param bases {RefBase,InputBase}
	 * @param alignInfo { {Match,Mismatch,Gap} , {align,insert,delete} }
	 * 
	 * @return {score,alignType}
	 */
	private static class GetCellScore implements Function3< int[] , char[] , Tuple2<int[],char[]> , Tuple2<Integer,Character> > 
	{
		public Tuple2<Integer,Character> call( int[] cellScores , char[] bases , Tuple2<int[],char[]> alignInfo ) 
		{
			int[] alignScores = alignInfo._1() ;
			char[] alignTypes = alignInfo._2() ;
			
			int max = 0 ;
			char alignment = alignTypes[3] ;
			
			// deletion score
			int tmp = new InsDelScore().call( cellScores[2] , alignScores[2] ).intValue() ;
			if( tmp >= max )
			{
				max = tmp ;
				alignment = alignTypes[2] ;
			}
			
			// insertion score
			tmp = new InsDelScore().call( cellScores[1] , alignScores[2] ).intValue() ;
			if( tmp >= max )
			{
				max = tmp ;
				alignment = alignTypes[1] ;
			}
			
			// alignment score
			int[] aScores = {alignScores[0],alignScores[1]} ;
			tmp = new AlignScore().call( new Integer(cellScores[0]) , aScores , bases ) ;
			if( tmp >= max )
			{
				max = tmp ;
				alignment = alignTypes[0] ;
			}
			
			return new Tuple2<Integer,Character>( new Integer(max) , new Character(alignment) ) ;
		}
	}
	
	/**
	 * 
	 * 
	 * @param cellScore Cell scores of N (insertion) or W (deletion) cells
	 * @param gapScore
	 * 
	 * @return Score for insertion or deletion
	 */
	private static class InsDelScore implements Function2<Integer,Integer,Integer>
	{
		public Integer call( Integer cellScore , Integer gapScore )
		{	
			return new Integer( cellScore.intValue() + gapScore.intValue() ) ;
		}
	}
	
	/**
	 * 
	 * 
	 * @param nwCellScore Score of NW cell
	 * @param alignScores {Match,Mismatch}
	 * @param bases {refBase,inputBase}
	 * 
	 * @return Score for alignment (either match or mismatch)
	 */
	private static class AlignScore implements Function3< Integer , int[] , char[] , Integer > 
	{
		public Integer call( Integer nwCellScore , int[] alignScores , char[] bases )
		{
			char refBase = Character.toUpperCase( bases[0] ) ;
			char inputBase = Character.toUpperCase( bases[1] ) ;
			
			if( refBase == inputBase )
				return new Integer( nwCellScore.intValue() + alignScores[0] ) ;
			else
				return new Integer( nwCellScore.intValue() + alignScores[1] ) ;
		}
	}
	
	
	/**
	 * 
	 * @param cell {i,j} of starting cell
	 * @param matrixInfo { {refSeq,inSeq} , {align,ins,del} }
	 * @param matrices {scoreMatrix,alignTypeMatrix}
	 * 
	 * @return { {ref,in} , j } - j starts from 1
	 */
	private static class GetAlignment 
			  implements Function3< int[] , Tuple2<String[],char[]> , Tuple2<int[][],char[][]> , Tuple2<String[],Integer>>
	{
		public Tuple2<String[],Integer> call( int[] cell , Tuple2<String[],char[]> matrixInfo , Tuple2<int[][],char[][]> matrices )
		{
			final char GAP_CHAR = '_' ;
			
			// variables
			String refSeq = matrixInfo._1()[0] ;	// j
			String inSeq = matrixInfo._1()[1] ;		// i
			
			char[] alignTypes = matrixInfo._2() ;
			
			int[][] scores = matrices._1() ;
			char[][] aligns = matrices._2() ;
			
			
			// step 1: backtrack to find path
			Stack<char[]> stack = new Stack<char[]>() ;
			
			int i = cell[0] ;
			int j = cell[1] ;
			int score = scores[i][j] ;
			
			int beginning = 0 ;
			
			while( score > 0 )
			{
				// update beginning of alignment
				beginning = j ;
				
				// check alignment to get next cell
				char align = aligns[i][j] ;
				
				if( align == alignTypes[0] )	// alignment
				{
					char[] bases = { refSeq.charAt(j-1) , inSeq.charAt(i-1) } ;
					stack.push( bases ) ;
					i-- ;
					j-- ;
				}
				else if( align == alignTypes[1] )	// insertion
				{
					char[] bases = { GAP_CHAR , inSeq.charAt(i-1) } ;
					stack.push( bases ) ;
					i-- ;
				}
				else	// deletion
				{
					char[] bases = { refSeq.charAt(j-1) , GAP_CHAR } ;
					stack.push( bases ) ;
					j-- ;
				}
				
				score = scores[i][j] ;
			}
			
			
			// step 2: pop stack to form alignment
			StringBuilder ref = new StringBuilder() ;
			StringBuilder in = new StringBuilder() ;
			
			while( ! stack.isEmpty() )
			{
				char[] bases = stack.pop() ;
				
				ref.append( bases[0] ) ;
				in.append( bases[1] ) ;
			}
			
			
			// return
			String[] alignedSeq = { ref.toString() , in.toString() } ;
			return new Tuple2<String[],Integer>( alignedSeq , new Integer(beginning) ) ;
		}
	}
	
	
	private static void printMatrices( int[][] scores , char[][] aligns , String[] seq )
	{
		String newline = System.lineSeparator() ;
		StringBuilder str = new StringBuilder() ;
		
		String _seq1 = seq[1] ;
		String _seq2 = seq[0] ;
		
		// score matrix
		str.append( newline ) ;
		str.append( "   _  " ) ;
		
		for( int i = 0 ; i < _seq2.length() ; i++ )
		{
			str.append( Character.toUpperCase(_seq2.charAt(i)) + "  " ) ;
		}
		
		str.append( newline ) ;
		
		for( int i = 0 ; i < scores.length ; i++ )
		{
			if( i == 0 )
				str.append( "_  " ) ;
			else
				str.append( Character.toUpperCase(_seq1.charAt(i-1)) + "  " ) ;
			
			for( int j = 0 ; j < scores[i].length ; j++ )
			{
				int score = scores[i][j] ;
				
				if( score < 10 )
					str.append( score + "  " ) ;
				else
					str.append( score + " " ) ;
			}
			
			str.append( newline ) ;
		}
		
		str.append( newline ) ;
		
		// align type matrix
		str.append( "   _  " ) ;
		
		for( int i = 0 ; i < _seq2.length() ; i++ )
		{
			str.append( Character.toUpperCase(_seq2.charAt(i)) + "  " ) ;
		}
		
		str.append( newline ) ;
		
		for( int i = 0 ; i < aligns.length ; i++ )
		{
			if( i == 0 )
				str.append( "_  " ) ;
			else
				str.append( Character.toUpperCase(_seq1.charAt(i-1)) + "  " ) ;
			
			for( int j = 0 ; j < aligns[i].length ; j++ )
			{
				str.append( aligns[i][j] + "  " ) ;
			}
			
			str.append( newline ) ;
		}
		
		System.out.println( str.toString() ) ;
	}
	
	
	public static void main( String[] args )
	{
		// constants
		final String FILE_REF = "/home/ubuntu/project/testRef/vertebrate_mammalian.415.rna.fna" ;
		final String FILE_IN = "/home/ubuntu/project/testIn/test1.fa" ;
		
		final int[] alignScores = {5,-3,-4} ;	// {match,mismatch,gap}
		final char[] alignTypes = {'a','i','d','-'} ;
		
		final String[] seq = { "CGTGAATTCAT" , "GACTTAC" } ;	// {ref,in}
		
		System.out.println( "Str1 = " + seq[0] ) ;
		System.out.println( "Str2 = " + seq[1] ) ;
		
		// run algorithm
		Tuple2<Integer,ArrayList<Tuple2<String[],Integer>>> result = 
				new OptAlignments().call( seq , alignScores , alignTypes ) ;
		
		System.out.println( "	max score = " + result._1() ) ;
		
		for( Tuple2<String[],Integer> tuple : result._2() )
		{
			System.out.println( "		index = " + tuple._2() ) ;
			System.out.println( "		" + tuple._1()[0] ) ;
			System.out.println( "		" + tuple._1()[1] ) ;
			System.out.println( "" ) ;
		}
	}
}
