package com.sw ;

import org.apache.spark.api.java.function.Function2 ;
import org.apache.spark.api.java.function.Function3 ;

import scala.Tuple2 ;
import scala.Tuple4 ;

import java.lang.StringBuilder ;

import java.util.ArrayList ;
import java.util.Stack ;


/**
 * The Smith-Waterman genetic alignment algorithm.
 * Not distributed.
 * Written in functional Spark.
 * 
 * @author Elizabeth Fong
 * @version Insight Data Engineering NY, September-October 2015
 */
@SuppressWarnings( "serial" ) 
public class SmithWaterman 
{
	/* --- PUBLIC METHODS -------------------------------------------------- */
	
	/**
	 * Finds the best-match reference sequence(s) to the given read, using the Smith-Waterman algorithm.
	 * Returns the alignment score and the optimal alignments.
	 * 
	 * @param seqs			An array of {@link java.lang.String} elements of the sequences to be compared, in the order { reference , read }.
	 * @param alignScores	An {@code int[]} of alignment scores used in the Smith-Waterman algorithm, in the order { match , mismatch , gap }.
	 * @param alignTypes	A {@code char[]} of alignment types used in the Smith-Waterman algorithm, in the order { alignment , insertion , deletion , none }.
	 * 
	 * @return				A {@link scala.Tuple2} of the alignment score and the optimal alignments.			
	 */
	public static class OptAlignments implements Function3< String[] , int[] , char[] , Tuple2<Integer,ArrayList<Tuple2<Integer,String[]>>> >
	{
		public Tuple2<Integer,ArrayList<Tuple2<Integer,String[]>>> call( String[] seqs , int[] alignScores , char[] alignTypes )
		{
			// VARIABLES
			String refSeq = seqs[0] ;	// j
			String inSeq = seqs[1] ;	// i
			
			int[][] scores = new int[inSeq.length()+1][refSeq.length()+1] ;
			char[][] aligns = new char[inSeq.length()+1][refSeq.length()+1] ;
			
			
			// step 1: score matrix
			Tuple2<int[][],char[][]> matrices = new Tuple2<int[][],char[][]>( scores , aligns ) ;
			Tuple2<int[],char[]> alignInfo = new Tuple2<int[],char[]>( alignScores , alignTypes ) ;
			
			Tuple2<Integer,ArrayList<int[]>> result1 = new ScoreMatrix().call( matrices , seqs , alignInfo ) ;
			
			
			// step 2: backtrack from cells w/ max scores to find opt alignments
			ArrayList<int[]> maxCells = result1._2() ;
			Tuple2<String[],char[]> matrixInfo = new Tuple2<String[],char[]>( seqs , alignTypes ) ;
			
			ArrayList<Tuple2<Integer,String[]>> opt = new ArrayList<Tuple2<Integer,String[]>>(maxCells.size()) ;
			
			for( int[] cell : maxCells )
			{
				opt.add( new GetAlignment().call(cell,matrixInfo,matrices) ) ;
			}
			
			
			return new Tuple2<Integer,ArrayList<Tuple2<Integer,String[]>>>( result1._1() , opt ) ;
		}
	}
	
	
	/* --- MATRIX SCORING -------------------------------------------------- */
	
	/**
	 * Step 1 of the Smith-Waterman algorithm. Filling a matrix with scores and alignment information.
	 * This step is distributed, but is not optimal.
	 * 
	 * @param matrices	A {@link scala.Tuple2} of matrices to be filled. 
	 * 					The matrices to be filled are an {@code int[][]} of scores and a {@code char[][]} of corresponding alignment types.
	 * @param seqs		An array of {@link java.lang.String} elements of the sequences to be compared, in the order { reference , read }.
	 * @param alignInfo	A {@link scala.Tuple2} of alignment information used in filling the given matrices, in the order { alignment scores , alignment types }.
	 *
	 * @return			A {@link scala.Tuple2} of the maximum cell score and an {@link java.util.ArrayList} of coordinates of cells with this score.
	 * 					The given matrices will be filled upon return.
	 */
	private static class ScoreMatrix implements Function3< Tuple2<int[][],char[][]> , String[] , Tuple2<int[],char[]> , Tuple2<Integer,ArrayList<int[]>> >
	{
		public Tuple2<Integer,ArrayList<int[]>> call( Tuple2<int[][],char[][]> matrices , String[] seq , Tuple2<int[],char[]> alignInfo )
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
			
			return new Tuple2<Integer,ArrayList<int[]>>( new Integer(maxScore) , maxCells ) ;
		}
	}
	
	/**
	 * Returns the cell score and alignment type in a {@link scala.Tuple2} for a given cell.
	 * 
	 * @param cellScores	An {@code int[]} for the scores in the north-west, north, and west cells. Used in cell score calculation.
	 * @param bases			A {@code char[]} of base pairs corresponding to this cell, in the order { reference , input }.
	 * @param alignInfo		A {@link scala.Tuple2} of alignment information used in filling the given matrices, in the order { alignment scores , alignment types }.
	 * 
	 * @return				A {@link scala.Tuple2} of the cell score and the alignment type.
	 */
	private static class GetCellScore implements Function3< int[] , char[] , Tuple2<int[],char[]> , Tuple2<Integer,Character> > 
	{
		public Tuple2<Integer,Character> call( int[] cellScores , char[] bases , Tuple2<int[],char[]> alignInfo ) 
		{
			int[] alignScores = alignInfo._1() ;
			char[] alignTypes = alignInfo._2() ;
			alignInfo = null ;
			
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
			tmp = new AlignmentScore().call( new Integer(cellScores[0]) , bases , aScores ) ;
			if( tmp >= max )
			{
				max = tmp ;
				alignment = alignTypes[0] ;
			}
			
			return new Tuple2<Integer,Character>( new Integer(max) , new Character(alignment) ) ;
		}
	}
	
	/**
	 * Calculates and returns the insertion or deletion score of a cell. Both scores are calculated in the same way.
	 * 
	 * @param cellScore	The north (insertion) or west (deletion) cell.
	 * @param gapScore	The gap score. Should be negative.
	 * 
	 * @return			The calculated insertion or deletion score.
	 */
	private static class InsDelScore implements Function2< Integer , Integer , Integer >
	{
		public Integer call( Integer cellScore , Integer gapScore )
		{	
			return new Integer( cellScore.intValue() + gapScore.intValue() ) ;
		}
	}
	
	/**
	 * Calculates and returns the score for an alignment of base pairs. May be a match or a mismatch of base pairs.
	 * 
	 * @param nwScore		The score for the northwest cell.
	 * @param bases			A {@code char[]} of base pairs corresponding to the indices of this cell, in the form (reference,read).
	 * @param alignScores	An {@code int[]} of alignment scores used in this algorithm, in the form (match,mismatch,gap).
	 * 
	 * @return				The score for an alignment of the base pairs corresponding to this cell.
	 */
	private static class AlignmentScore implements Function3< Integer , char[] , int[] , Integer >
	{
		public Integer call( Integer nwScore , char[] bases , int[] alignScores )
		{
			char refBase = Character.toUpperCase( bases[0] ) ;
			char inputBase = Character.toUpperCase( bases[1] ) ;
			
			if( refBase == inputBase )
				return new Integer( nwScore.intValue() + alignScores[0] ) ;
			else
				return new Integer( nwScore.intValue() + alignScores[1] ) ;
		}
	}
	
	
	/* --- GET OPT ALIGNMENT ----------------------------------------------- */
	
	/**
	 * Step 2 of the Smith-Waterman algorithm.
	 * Given a starting cell, backtracks through the matrix to obtain an optimal alignment.
	 * 
	 * @param cell			The {@code (i,j)} coordinates of the starting cell
	 * @param matrixInfo	A {@link scala.Tuple2} of other data required to extract the optimal alignments. 
	 * 						Contains the sequences in this comparison (reference,read), and the alignment types (alignment,insertion,deletion,none).
	 * @param matrices		A {@link scala.Tuple2} of the {@code int[][]} of scores and the {@code char[][]} of corresponding alignment types, filled during Step 1.
	 * 
	 * @return				A {@link scala.Tuple2} of the index of the beginning of the alignment with respect to the reference sequence,
	 * 						and how the sequences are aligned.
	 */
	private static class GetAlignment implements Function3< int[] , Tuple2<String[],char[]> , Tuple2<int[][],char[][]> , Tuple2<Integer,String[]> >
	{
		public Tuple2<Integer,String[]> call( int[] cell , Tuple2<String[],char[]> matrixInfo , Tuple2<int[][],char[][]> matrices )
		{
			final char GAP_CHAR = '_' ;
			
			// variables
			String refSeq = matrixInfo._1()[0] ;	// j
			String inSeq = matrixInfo._1()[1] ;		// i
			
			char[] alignTypes = matrixInfo._2() ;
			
			int[][] scores = matrices._1() ;
			char[][] aligns = matrices._2() ;
			
			matrixInfo = null ;
			matrices = null ;
			
			
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
			
			refSeq = null ;
			inSeq = null ;
			scores = null ;
			aligns = null ;
			
			
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
			ref = null ;
			in = null ;
			
			return new Tuple2<Integer,String[]>( new Integer(beginning) , alignedSeq ) ;
		}
	}
}
