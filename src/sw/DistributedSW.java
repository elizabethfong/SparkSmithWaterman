package sw ;

import java.io.Serializable ;

import java.lang.StringBuilder ;

import java.util.ArrayList ;
import java.util.Comparator ;
import java.util.List ;
import java.util.Stack ;

import org.apache.spark.SparkConf ;

import org.apache.spark.api.java.JavaRDD ;
import org.apache.spark.api.java.JavaSparkContext ;

import org.apache.spark.api.java.function.Function ;
import org.apache.spark.api.java.function.Function2 ;
import org.apache.spark.api.java.function.Function3 ;

import scala.Tuple2 ;
import scala.Tuple4 ;
import scala.Tuple5 ;


/**
 * <p>
 * A distributed version of the Smith-Waterman genetic alignment algorithm. <br>
 * Written in function Spark. <br>
 * Not optimal.
 * </p>
 * 
 * @author Elizabeth Fong
 * @version Insight Data Engineering NY, September-October 2015
 */
@SuppressWarnings( "serial" ) 
public class DistributedSW 
{
	/* --- PUBLIC METHODS -------------------------------------------------- */
	
	/**
	 * <p>
	 * Finds the best-match reference sequence(s) to the given read using a distributed version of the Smith-Waterman algorithm.<br>
	 * Returns the alignment score and the optimal alignments.
	 * </p>
	 * 
	 * @see	{@link org.apache.spark.api.java.function.Function3		
	 */
	public static class OptAlignments implements Function3< String[] , int[] , char[] , Tuple2<Integer,ArrayList<Tuple2<Integer,String[]>>> >
	{
		/**
		 * <p>
		 * Finds the best-match reference sequence(s) to the given read using a distributed version of the Smith-Waterman algorithm.<br>
		 * Returns the alignment score and the optimal alignments.
		 * </p>
		 * 
		 * @param seqs			<p><ul>An array of {@link java.lang.String} elements of the sequences to be compared, in the order:
		 * 							<ul>reference sequence</ul>
		 * 							<ul>read</ul>
		 * 						</ul></p>
		 * @param alignScores	<p><ul>An {@code int[]} of alignment scores used in the Smith-Waterman algorithm, in the order:
		 * 							<ul>match score</ul>
		 * 							<ul>mismatch score</ul>
		 * 							<ul>gap score</ul>
		 * 						</ul></p>
		 * @param alignTypes	<p><ul>A {@code char[]} of alignment types used in the Smith-Waterman algorithm, in the order:
		 * 							<ul>alignment</ul>
		 * 							<ul>insertion</ul>
		 * 							<ul>deletion</ul>
		 * 							<ul>no alignment</ul>
		 * 						</ul></p>
		 * 
		 * @return				<p><ul>A {@link scala.Tuple2} of the alignment score and the optimal alignments.</ul></p>
		 */
		@Override
		public Tuple2<Integer,ArrayList<Tuple2<Integer,String[]>>> call( String[] seqs , int[] alignScores , char[] alignTypes )
		{
			// VARIABLES
			String refSeq = seqs[0] ;	// j
			String inSeq = seqs[1] ;	// i
			
			int[][] scores = new int[inSeq.length()+1][refSeq.length()+1] ;
			char[][] aligns = new char[inSeq.length()+1][refSeq.length()+1] ;
			
			
			// SCORE MATRIX
			Tuple2<int[][],char[][]> matrices = new Tuple2<int[][],char[][]>( scores , aligns ) ;
			Tuple2<int[],char[]> alignInfo = new Tuple2<int[],char[]>( alignScores , alignTypes ) ;
			
			Tuple2<Integer,ArrayList<int[]>> result = new ScoreMatrix().call( matrices , seqs , alignInfo ) ;
			
			int maxScore = result._1() ;
			ArrayList<int[]> maxCells = result._2() ;
			
			
			// GET OPT ALIGNMENTS
			Tuple2<String[],char[]> data = new Tuple2<String[],char[]>( seqs , alignTypes ) ;
			ArrayList<Tuple2<Integer,String[]>> optAlignments = new GetAlignments().call( maxCells , matrices, data ) ;
			
			
			// RETURN!!!
			return new Tuple2<Integer,ArrayList<Tuple2<Integer,String[]>>>( new Integer(maxScore) , optAlignments ) ;
		}
	}
	
	
	/* --- MATRIX SCORING -------------------------------------------------- */
	
	/**
	 * <p>
	 * Step 1 of the Smith-Waterman algorithm. Filling a matrix with scores and alignment information.<br>
	 * This step is distributed, but is not optimal.
	 * </p>
	 * 
	 * @see	{@link org.apache.spark.api.java.function.Function3	
	 */
	private static class ScoreMatrix implements Function3< Tuple2<int[][],char[][]> , String[] , Tuple2<int[],char[]> , Tuple2<Integer,ArrayList<int[]>> >
	{
		/**
		 * <p>
		 * Step 1 of the Smith-Waterman algorithm. Filling a matrix with scores and alignment information.<br>
		 * This step is distributed, but is not optimal.
		 * </p>
		 * 
		 * @param matrices	<p><ul>A {@link scala.Tuple2} of matrices to be filled. 
		 * 						<ul>{@code int[][]}  - matrix of scores</ul>
		 * 						<ul>{@code char[][]} - matrix of corresponding alignment types.</ul>
		 * 					</ul></p>
		 * @param seqs		<p><ul>An array of {@link java.lang.String} elements of the sequences to be compared, in the order:
		 * 						<ul>reference sequence</ul>
		 * 						<ul>read</ul>
		 * 					</ul></p>
		 * @param alignInfo	<p><ul>A {@link scala.Tuple2} of alignment information used in filling the given matrices, in the order:
		 * 						<ul>{@code int[]} alignment scores - match, mismatch, gap</ul>
		 * 						<ul>{@code char[]} alignment types - alignment, insertion, deletion, none
		 * 					</ul></p>
		 *
		 * @return			<p><ul>A {@link scala.Tuple2} of the maximum cell score and an {@link java.util.ArrayList} of coordinates of cells with this score.</ul>
		 * 					<ul>The given matrices will be filled upon return.</ul></ul></p>
		 */
		@Override
		public Tuple2<Integer,ArrayList<int[]>> call( Tuple2<int[][],char[][]> matrices , String[] seqs , Tuple2<int[],char[]> alignInfo )
		{
			// VARIABLES
			int[][] scores = matrices._1() ;
			char[][] aligns = matrices._2() ;
			
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
			int[] maxIndices = { seqs[1].length() , seqs[0].length() } ;	// i,j -> in,ref
			Tuple2<int[],String[]> arrInfo = new Tuple2<int[],String[]>( maxIndices , seqs ) ;
			
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
			
			// init lists (list, children)
			list = new GetInitList().call( coordinates , defaultTuple , arrInfo ) ;
			
			int[] nextCoord = {2,1} ;
			children = new GetInitList().call( nextCoord , defaultTuple , arrInfo ) ;
			
			
			// DISTRIBUTE!!!
			JavaSparkContext sc = new JavaSparkContext( new SparkConf() ) ;
			int maxScore = 0 ;
			
			int[] start = null ;
			ArrayList<int[]> maxCells = new ArrayList<int[]>() ;
			
			while( list != null )
			{
				// update grandchildren list
				start = list.get(0)._1()[0] ;
				start = new GetNextStart().call( start , maxIndices ) ;
				grandchildren = new GetInitList().call( start , defaultTuple , arrInfo ) ;
				
				
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
				list = children ;
				children = grandchildren ;
			}
			
			
			// RETURN!!!
			sc.close() ;
			return new Tuple2<Integer,ArrayList<int[]>>( new Integer(maxScore) , maxCells ) ;
		}
	}
	
	/**
	 * <p>
	 * The map function for matrix scoring. 
	 * Calculates the cell score with the given data.<br>
	 * Returns a {@link scala.Tuple4} of the following: cell coordinates (i,j), cell score, cell alignment type, and 
	 * a {@code boolean[]} indicating where this score should be broadcasted to (SE,S,E).
	 * </p>
	 * 
	 * @see {@link org.apache.spark.api.java.function.Function
	 */
	private static class GetCellScore implements Function< Tuple2<int[][],char[][]> , Tuple4<int[],Integer,Character,boolean[]> >
	{
		/**
		 * <p>
		 * The map function for matrix scoring. 
		 * Calculates the cell score with the given data.<br>
		 * Returns a {@link scala.Tuple4} of the following: cell coordinates (i,j), cell score, cell alignment type, and 
		 * a {@code boolean[]} indicating where this score should be broadcasted to (SE,S,E).
		 * </p>
		 * 
		 * @param data	<p><ul>The data required for the computation of the score for a single cell, grouped by data type into an {@code int[][]} and {@code char[][]}.<br>
		 * 				The order of the data in the {@code int[][]} is: 
		 * 					<ul>cell coordinates (i,j)</ul>
		 * 					<ul>scores of the surrounding cells (NW,N,W)</ul>
		 * 					<ul>alignment scores (match,mismatch,gap)</ul>
		 * 					<ul>maximum values of indices (i,j)</ul><br>
		 * 				The order of the data in the {@code char[][]} is: 
		 * 					<ul>base pairs corresponding to the coordinates of this cell (reference,read)</ul>
		 * 					<ul>alignment types (alignment,insertion,deletion,none)</ul>
		 * 				</ul></p>
		 * 
		 * @return		<p><ul>A {@link scala.Tuple4} of the following: 
		 * 					<ul>cell coordinates (i,j)</ul>
		 * 					<ul>cell score</ul>
		 * 					<ul>cell alignment type</ul>
		 * 					<ul>a {@code boolean[]} indicating where this score should be broadcasted to (SE,S,E)</ul>
		 * 				</ul></p>
		 */
		@Override
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
	
	/**
	 * <p>
	 * The 'reduce' function for matrix scoring.<br>
	 * Broacasts the cell scores for each cell in the current list of cells to the specified children and grandchildren cells.
	 * </p>
	 * 
	 * @see {@link org.apache.spark.api.java.function.Function3
	 */
	private static class Broadcast implements Function3< List<Tuple4<int[],Integer,Character,boolean[]>> , ArrayList<Tuple2<int[][],char[][]>> , ArrayList<Tuple2<int[][],char[][]>> , Boolean >
	{
		/**
		 * <p>
		 * The 'reduce' function for matrix scoring.<br>
		 * Broacasts the cell scores for each cell in the current list of cells to the specified children and grandchildren cells.
		 * </p>
		 * 
		 * @param list			<p><ul>A {@link java.util.List} of the current diagonal row cells.</ul></p>
		 * @param children		<p><ul>An {@link java.util.ArrayList} of the children cells.</ul></p>
		 * @param grandchildren	<p><ul>An {@link java.util.ArrayList} of the grandchildren cells.</ul></p>
		 * 
		 * @return {@code null}
		 */
		@Override
		public Boolean call( List<Tuple4<int[],Integer,Character,boolean[]>> list , ArrayList<Tuple2<int[][],char[][]>> children , ArrayList<Tuple2<int[][],char[][]>> grandchildren )
		{
			for( Tuple4<int[],Integer,Character,boolean[]> cell : list )
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
	
	
	/* --- GET OPT ALIGNMENT ----------------------------------------------- */
	
	/**
	 * <p>
	 * Step 2 of the Smith-Waterman algorithm.<br>
	 * Backtracks through the matrix from cells with the maximum cell score to obtain optimal alignments.<br>
	 * This step is distributed.<br>
	 * Returns an {@link java.util.ArrayList} of all optimal alignments and the index of their alignment locations.
	 * </p>
	 * 
	 * @see	{@link org.apache.spark.api.java.function.Function3	
	 */
	private static class GetAlignments implements Function3< ArrayList<int[]> , Tuple2<int[][],char[][]> , Tuple2<String[],char[]> , ArrayList<Tuple2<Integer,String[]>> >
	{
		/**
		 * <p>
		 * Step 2 of the Smith-Waterman algorithm.<br>
		 * Backtracks through the matrix from cells with the maximum cell score to obtain optimal alignments.<br>
		 * This step is distributed.<br>
		 * Returns an {@link java.util.ArrayList} of all optimal alignments and the index of their alignment locations.
		 * </p>
		 * 
		 * @param cell			<p><ul>The {@code (i,j)} coordinates of the starting cell</ul></p>
		 * @param matrixInfo	<p><ul>A {@link scala.Tuple2} of other data required to extract the optimal alignments. 
		 * 							<ul>A {@code String[]} of the sequences in this comparison - reference sequence , read</ul>
		 * 							<ul>{@code char[]} alignment types - alignment, insertion, deletion, none</ul>
		 * 						</ul></p>
		 * @param matrices		<p><ul>A {@link scala.Tuple2} of matrices filled in Step 1. 
		 * 							<ul>{@code int[][]}  - matrix of scores</ul>
		 * 							<ul>{@code char[][]} - matrix of corresponding alignment types.</ul>
		 * 						</ul></p>
		 * 
		 * @return				<p><ul>A {@link scala.Tuple2} of the index of the beginning of the alignment with respect to the reference sequence,
		 * 						and how the sequences are aligned.</ul></p>
		 */
		@Override
		public ArrayList<Tuple2<Integer,String[]>> call( ArrayList<int[]> starts , Tuple2<int[][],char[][]> matrices , Tuple2<String[],char[]> otherData )
		{
			// VARIABLES
			int[][] scores = matrices._1() ;
			char[][] aligns = matrices._2() ;
			
			// get list to map
			ArrayList<Tuple5<int[],int[][],char[][],String[],char[]>> list = new ArrayList<Tuple5<int[],int[][],char[][],String[],char[]>>(starts.size()) ;
			
			for( int[] start : starts )
			{
				list.add( new Tuple5<int[],int[][],char[][],String[],char[]>(start,scores,aligns,otherData._1(),otherData._2()) ) ;
			}
			
			
			// DISTRIBUTE ALIGNMENT START CELLS
			JavaSparkContext sc = new JavaSparkContext( new SparkConf() ) ;
			
			// parallelise start cells, then map
			JavaRDD<Tuple5<int[],int[][],char[][],String[],char[]>> startRDD = sc.parallelize( list ) ;
			JavaRDD<Tuple2<Integer,String[]>> matchRDD = startRDD.map( new GetMatchSite() ) ;
			
			// reduce: sort by keys, then add values to 			
			List<Tuple2<Integer,String[]>> matchSites = matchRDD.collect() ;
			matchSites.sort( new MatchSiteComp() ) ;
			
			ArrayList<Tuple2<Integer,String[]>> result = new ArrayList<Tuple2<Integer,String[]>>( matchSites.size() ) ;
			
			for( Tuple2<Integer,String[]> site : matchSites )
			{
				result.add( site ) ;
			}
			
			
			// RETURN!!!
			sc.close() ;
			return result ;
		}
	}
	
	/**
	 * <p>
	 * The map function for extracting optimal alignments.<br>
	 * This extracts and returns the optimal alignment (match location) of the read to the reference sequence.
	 * </p>
	 * 
	 * @see {@link org.apache.spark.api.java.function.Function
	 */
	private static class GetMatchSite implements Function< Tuple5<int[],int[][],char[][],String[],char[]> , Tuple2<Integer,String[]> >
	{
		/**
		 * <p>
		 * The map function for extracting optimal alignments.<br>
		 * This extracts and returns the optimal alignment (match location) of the read to the reference sequence.
		 * </p>
		 * 
		 * @param tuple	<p><ul>A {@code scala.Tuple5} with the data required to extract the optimal alignment.<br>
		 * 				Contains the following, in order:
		 * 					<ul>{@code int[]} - coordinates of the starting cell (i,j)</ul>
		 * 					<ul>{@code int[][]} - matrix of cell scores</ul>
		 * 					<ul>{@code char[][]} - matrix of corresponding alignment types</ul>
		 * 					<ul>{@code String[]} - sequences in this comparison (reference,read)</ul>
		 * 					<ul>{@code char[]} - alignment types used in this algorithm (alignment,insertion,deletion,none)</ul>
		 * 				</ul></p>
		 * 
		 * @return		<p><ul>A {@link scala.Tuple2} of the index of this alignment and how the sequences align.</ul></p>
		 */
		@Override
		public Tuple2<Integer,String[]> call( Tuple5<int[],int[][],char[][],String[],char[]> tuple )
		{
			// VARIABLES
			final char GAP_CHAR = '_' ;
			
			int[][] scores = tuple._2() ;
			char[][] aligns = tuple._3() ;
			
			String refSeq = tuple._4()[0] ;
			String inSeq = tuple._4()[1] ;
			
			char[] alignTypes = tuple._5() ;
			
			
			// BACKTRACK!
			Stack<char[]> stack = new Stack<char[]>() ;
			
			int i = tuple._1()[0] ;
			int j = tuple._1()[1] ;
			
			int score = scores[i][j] ;
			int beginning = 0 ;
			
			while( score > 0 )
			{
				// update binding site beginning index
				beginning = j ;
				
				// check alignment type -> bases for this location, next cell location
				char align = aligns[i][j] ;
				
				if( align == alignTypes[0] )	// alignment
				{
					char[] bases = { refSeq.charAt(j-1) , inSeq.charAt(i-1) } ;
					stack.push(bases) ;
					i-- ;
					j-- ;
				}
				else if( align == alignTypes[1] )	// insertion
				{
					char[] bases = { GAP_CHAR , inSeq.charAt(i-1) } ;
					stack.push(bases) ;
					i-- ;
				}
				else	// deletion
				{
					char[] bases = { refSeq.charAt(j-1) , GAP_CHAR } ;
					stack.push(bases) ;
					j-- ;
				}
				
				score = scores[i][j] ;
			}
			
			
			// POP STACK TO GENERATE BINDING SITE
			StringBuilder ref = new StringBuilder() ;
			StringBuilder in = new StringBuilder() ;
			
			while( ! stack.isEmpty() )
			{
				char[] bases = stack.pop() ;
				
				ref.append( bases[0] ) ;
				in.append( bases[1] ) ; 
			}
			
			
			// RETURN!!!
			String[] aligned = { ref.toString() , in.toString() } ;
			return new Tuple2<Integer,String[]>( new Integer(beginning) , aligned ) ;
		}
	}
	
	
	/* --- UTILITY METHODS ------------------------------------------------- */
	
	/**
	 * <p>
	 * Extracts and returns the base pairs corresponding to the {@code i} and {@code j} indices of a cell in the scoring matrix,
	 * in the order {reference,read}.
	 * </p>
	 * 
	 * @see {@link org.apache.spark.api.java.function.Function2
	 */
	private static class GetBases implements Function2< int[] , String[] , char[] >
	{
		/**
		 * <p>
		 * Extracts and returns the base pairs corresponding to the {@code i} and {@code j} indices of a cell in the scoring matrix,
		 * in the order {reference,read}.
		 * </p>
		 * 
		 * @param coordinates	<p><ul>An {@code int[]} of coordinates of a cell in the scoring matrix, in the form (i,j).</ul></p>
		 * @param sequences		<p><ul>A {@code String[]} of the sequences compared, in the order {reference,read}.</ul></p>
		 * 
		 * @return				<p><ul>A {@code char[]} of the base pairs corresponding to the given matrix cell coordinates, in the order {reference,read}.</ul></p>
		 */
		@Override
		public char[] call( int[] coordinates , String[] sequences )
		{
			// j -> reference, i -> read
			char[] bases = { sequences[0].charAt(coordinates[1]-1) , sequences[1].charAt(coordinates[0]-1) } ;
			return bases ;
		}
	}
	
	/**
	 * <p>
	 * Calculates and returns the insertion or deletion score of a cell.<br>
	 * Both scores are calculated in the same way.
	 * </p>
	 * 
	 * @see	{@link org.apache.spark.api.java.function.Function2
	 */
	private static class InsDelScore implements Function2< Integer , Integer , Integer >
	{
		/**
		 * <p>
		 * Calculates and returns the insertion or deletion score of a cell.<br>
		 * Both scores are calculated in the same way.
		 * </p>
		 * 
		 * @param cellScore	<p><ul>The score for the north (insertion) or west (deletion) cell.</ul></p>
		 * @param gapScore	<p><ul>The gap score. Should be negative.</ul></p>
		 * 
		 * @return			<p><ul>The calculated insertion or deletion score.</ul></p>
		 */
		@Override
		public Integer call( Integer cellScore , Integer gapScore )
		{
			return cellScore + gapScore ;
		}
	}
	
	/**
	 * <p>
	 * Calculates and returns the score for an alignment of base pairs.<br>
	 * May be a match or a mismatch of base pairs.
	 * </p>
	 * 
	 * @see	{@link org.apache.spark.api.java.function.Function3	
	 */
	private static class AlignmentScore implements Function3< Integer , char[] , int[] , Integer >
	{
		/**
		 * <p>
		 * Calculates and returns the score for an alignment of base pairs.<br>
		 * May be a match or a mismatch of base pairs.
		 * </p>
		 * 
		 * @param nwScore		<p><ul>The score for the northwest cell.</ul></p>
		 * @param bases			<p><ul>A {@code char[]} of base pairs corresponding to this cell, in the order:
		 * 							<ul>reference base pair</ul>
		 * 							<ul>read base pair</ul>
		 * 						</ul></p>
		 * @param alignScores	<p><ul>alignment scores - match, mismatch, gap</ul></p>
		 * 
		 * @return				<p><ul>The score for an alignment of the base pairs corresponding to this cell.</ul></p>
		 */
		@Override
		public Integer call( Integer nwScore , char[] bases , int[] alignScores )
		{
			if( Character.toUpperCase(bases[0]) == Character.toUpperCase(bases[1]) )	// match
				return nwScore + alignScores[0] ;
			else																		// mismatch
				return nwScore + alignScores[1] ;
		}
	}
	
	/**
	 * <p>
	 * Returns the next, incomplete, diagonal grandchildren row of the matrix as an {@link java.util.ArrayList}, or {@code null} if it does not exist.
	 * </p>
	 * 
	 * @see {@link org.apache.spark.api.java.function.Function3
	 */
	private static class GetInitList implements Function3< int[] , Tuple2<int[][],char[][]> , Tuple2<int[],String[]> , ArrayList<Tuple2<int[][],char[][]>> >
	{
		/**
		 * <p>
		 * Returns the next, incomplete, diagonal grandchildren row of the matrix as an {@link java.util.ArrayList}, or {@code null} if it does not exist.
		 * </p>
		 * 
		 * @param start			<p><ul>The {@code (i,j)} coordinates of the first cell of the next grandchildren row.<br>
		 * 						The first cell in the row is defined as the cell with the smallest {@code j} value.
		 * 						</ul><p>
		 * @param defaultTuple	<p><ul>The default, initial {@link scala.Tuple2} of data required to calculate the score of a cell (the map function), grouped by data type.<br>
		 * 						The {@code int[][]} contains, in order: 
		 * 							<ul>cell coordinates (i,j)</ul> 
		 * 							<ul>scores of cells required for score calculation (NW,N,W)</ul>
		 * 							<ul>alignment scores used for calculations (match,mismatch,gap)</ul>
		 * 							<ul>the maximum (i,j) indices the cells may have.</ul><br>
		 * 						The {@code char[][]} contains, in order: 
		 * 							<ul>the (reference,read) base pairs corresponding to the cell</ul>
		 * 							<ul>the alignment types used in this algorithm (alignment,insertion,deletion,none)</ul>
		 * 						</ul><p>
		 * @param arrInfo		<p><ul>A {@link scala.Tuple2} of other information used to obtain the next grandchildren list. <br>
		 * 						Contains, in order:
		 * 							<ul>an {@code int[]} of the maximum (i,j) indices of cells in the matrix</ul>
		 * 							<ul>a {@code String[]} of the sequences in this comparison (reference,read)</ul>
		 * 						</ul><p>
		 * 
		 * @return				<p><ul>The next, incomplete, diagonal grandchildren row of the matrix as an {@link java.util.ArrayList}, 
		 * 						or {@code null} if it does not exist.</ul><p>
		 */
		@Override
		public ArrayList<Tuple2<int[][],char[][]>> call( int[] start , Tuple2<int[][],char[][]> defaultTuple , Tuple2<int[],String[]> arrInfo )
		{
			// corner case: no next grandchildren list
			if( start == null )
			{
				return null ;
			}
			
			
			// array info
			int[] maxIndices = arrInfo._1() ;
			String[] seqs = arrInfo._2() ;
			
			
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
			
			for( int i = 0 ; i < len ; i++ )
			{
				list.add( new DeepCopyTuple().call(defaultTuple) ) ;
			}
			
				
			// update coordinates and bases of each tuple
			for( int i = 0 ; i < len ; i++ )
			{
				Tuple2<int[][],char[][]> tuple = list.get(i) ;
				
				int[] coordinates = { start[0]-i , start[1]+i } ;
				
				tuple._1()[0] = coordinates ;
				
				char[] bases = new GetBases().call( coordinates , seqs ) ;
				tuple._2()[0] = bases ;
			}
			
			return list  ;
		}
	}
	
	/**
	 * <p>
	 * Returns a deep copy of the given {@code Tuple2<int[][],char[][]>}.
	 * </p>
	 * 
	 * @see {@link org.apache.spark.api.java.function.Function
	 */
	private static class DeepCopyTuple implements Function< Tuple2<int[][],char[][]> , Tuple2<int[][],char[][]> >
	{
		/**
		 * <p>
		 * Returns a deep copy of the given {@code Tuple2<int[][],char[][]>}.
		 * </p>
		 * 
		 * @param tuple	<p><ul>The {@link scala.Tuple2} to be copied.</ul></p>
		 * 
		 * @return		<p><ul>The deep copy of the given {@code Tuple2<int[][],char[][]>}. </ul></p>
		 */
		@Override
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
	
	/**
	 * <p>
	 * Returns the {@code (i,j)} coordinates of the first cell of the next grandchildren row.<br>
	 * The first cell in the row is defined as the cell with the smallest {@code j} value.
	 * </p>
	 * 
	 * @see {@link org.apache.spark.api.java.function.Function2
	 */
	private static class GetNextStart implements Function2< int[] , int[] , int[] >
	{
		/**
		 * <p>
		 * Returns the {@code (i,j)} coordinates of the first cell of the next grandchildren row.<br>
		 * The first cell in the row is defined as the cell with the smallest {@code j} value.
		 * </p>
		 * 
		 * @param start			<p><ul>The {@code (i,j)} coordinates of the first cell in the current row.</ul></p>
		 * @param maxIndices	<p><ul>The maximum {@code (i,j)} indices of cells in the matrix.</ul></p>
		 * 
		 * @return				<p><ul>The {@code (i,j)} coordinates of the first cell of the next grandchildren row.</ul></p>
		 */
		@Override
		public int[] call( int[] start , int[] maxIndices )
		{
			int nextI = start[0] + 2 ;
			int nextJ = start[1] ;
			
			if( nextI > maxIndices[0] )
			{
				nextJ += nextI - maxIndices[0] ;
				nextI = maxIndices[0] ;
			}
			
			// corner case: no next grandchildren list
			if( nextJ > maxIndices[1] )
			{
				return null ;
			}
			
			int[] next = { nextI , nextJ } ;
			return next ;
		}
	}
	
	
	/* --- COMPARATORS ----------------------------------------------------- */
	
	/**
	 * <p>
	 * A {@link java.util.Comparator} of cell results from the map phase when filling the matrices.<br>
	 * This orders cells in ascending order of their {@code j} coordinate values.
	 * </p>
	 * 
	 * @see {@link java.util.Comparator#compare(Object, Object)}
	 */
	private static class CellResultComp implements Comparator<Tuple4<int[],Integer,Character,boolean[]>> , Serializable
	{
		/**
		 * <p>
		 * Returns a negative, 0, or positive integer if the {@code j}-coordinate value of {@code t1} is 
		 * less than, equal to, or greater than, the {@code j}-coordinate value of {@code t2).
		 * </p>
		 * 
		 * @param t1	The first element to be compared.
		 * @param t2	The second element to be compared.
		 * 
		 * @return 	<p><ul>A negative, 0, or positive integer if the {@code j}-coordinate value of {@code t1} is 
		 * 			less than, equal to, or greater than, the {@code j}-coordinate value of {@code t2).</ul></p>
		 */
		@Override
		public int compare( Tuple4<int[],Integer,Character,boolean[]> t1 , Tuple4<int[],Integer,Character,boolean[]> t2 )
		{
			return t1._1()[1] - t2._1()[1] ;
		}
	}
	
	/**
	 * <p>
	 * A {@link java.util.Comparator} of matching sites.<br>
	 * This orders elements in ascending order of the indices of the alignment.
	 * </p>
	 * 
	 * @see {@link java.util.Comparator#compare(Object, Object)}
	 */
	public static class MatchSiteComp implements Comparator<Tuple2<Integer,String[]>> , Serializable
	{
		/**
		 * <p>
		 * Returns a negative, 0, or positive integer, if the match site index of {@code t1} is
		 * smaller than, equal to, or greater than, the reference metadata of {@code t2}.
		 * </p>
		 * 
		 * @param t1	The first element to be compared.
		 * @param t2	The second element to be compared.
		 * 
		 * @return		<p><ul>A negative, 0, or positive integer, if the match site index of {@code t1} is
		 * 				smaller than, equal to, or greater than, the reference metadata of {@code t2}.</ul></p>
		 */
		@Override
		public int compare( Tuple2<Integer,String[]> t1 , Tuple2<Integer,String[]> t2 )
		{
			return t1._1().intValue() - t2._1().intValue() ;
		}
	}
}
