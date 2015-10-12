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
 * A distributed version of the Smith-Waterman genetic alignment algorithm. 
 * Written in function Spark. 
 * Not optimal.
 * 
 * @author Elizabeth Fong
 * @version Insight Data Engineering NY, September-October 2015
 */
@SuppressWarnings( "serial" ) 
public class DistributedSW 
{
	/* --- PUBLIC METHODS -------------------------------------------------- */
	
	/**
	 * Finds the best-match reference sequence(s) to the given read using a distributed version of the Smith-Waterman algorithm.
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
	 * The map function for matrix scoring. 
	 * Calculates the cell score with the given data.
	 * Returns a {@link scala.Tuple4} of the following: cell coordinates (i,j), cell score, cell alignment type, and 
	 * a {@code boolean[]} indicating where this score should be broadcasted to (SE,S,E).
	 * 
	 * @param data	The data required for the computation of the score for a single cell, grouped by data type into an {@code int[][]} and {@code char[][]}.
	 * 				The order of the data in the {@code int[][]} is: 
	 * 					cell coordinates (i,j), 
	 * 					scores of the surrounding cells (NW,N,W), 
	 * 					alignment scores (match,mismatch,gap), 
	 * 					maximum values of indices (i,j)
	 * 				The order of the data in the {@code char[][]} is: 
	 * 					base pairs corresponding to the coordinates of this cell (reference,read),
	 * 					alignment types (alignment,insertion,deletion,none)
	 * 
	 * @return		A {@link scala.Tuple4} of the following: 
	 * 					cell coordinates (i,j), 
	 * 					cell score, 
	 * 					cell alignment type, 
	 * 					a {@code boolean[]} indicating where this score should be broadcasted to (SE,S,E)
	 */
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
	
	/**
	 * The 'reduce' function for matrix scoring.
	 * Broacasts the cell scores for each cell in the current list of cells to the specified children and grandchildren cells.
	 * 
	 * @param list			A {@link java.util.List} of the current diagonal row cells.
	 * @param children		An {@link java.util.ArrayList} of the children cells.
	 * @param grandchildren	An {@link java.util.ArrayList} of the grandchildren cells.
	 * 
	 * @return {@code null}
	 */
	private static class Broadcast implements Function3< List<Tuple4<int[],Integer,Character,boolean[]>> , ArrayList<Tuple2<int[][],char[][]>> , ArrayList<Tuple2<int[][],char[][]>> , Boolean >
	{
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
	 * Step 2 of the Smith-Waterman algorithm. 
	 * Backtracks through the matrix from cells with the maximum cell score to obtain optimal alignments.
	 * This step is distributed.
	 * Returns an {@link java.util.ArrayList} of all optimal alignments and the index of their alignment locations.
	 * 
	 * @param starts	An {@link java.util.ArrayList} of coordinates (i,j) of cells with the maximum cell score.
	 * @param matrices	A {@link scala.Tuple2} of the {@code int[][]} of scores and the {@code char[][]} of corresponding alignment types, filled during Step 1.
	 * @param otherData	A {@link scala.Tuple2} of other data required to extract the optimal alignments. 
	 * 					Contains the sequences in this comparison (reference,read), and the alignment types (alignment,insertion,deletion,none).
	 * 
	 * @return			An {@link java.util.ArrayList} of all optimal alignments and the index of their alignment locations.
	 */
	private static class GetAlignments implements Function3< ArrayList<int[]> , Tuple2<int[][],char[][]> , Tuple2<String[],char[]> , ArrayList<Tuple2<Integer,String[]>> >
	{
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
	 * The map function for extracting optimal alignments.
	 * This extracts and returns the optimal alignment (match location) of the read to the reference sequence.
	 * 
	 * @param tuple	A {@code scala.Tuple5} with the data required to extract the optimal alignment. 
	 * 				Contains the following, in order:
	 * 					an {@code int[]} of the coordinates of the starting cell (i,j),
	 * 					the {@code int[][]} of cell scores,
	 * 					the {@code char[][]} of corresponding alignment types,
	 * 					the {@code String[]} of the sequences in this comparison (reference,read), and
	 * 					the {@code char[]} of alignment types used in this algorithm (alignment,insertion,deletion,none).
	 * 
	 * @return		A {@link scala.Tuple2} of the index of this alignment and how the sequences align.
	 */
	private static class GetMatchSite implements Function< Tuple5<int[],int[][],char[][],String[],char[]> , Tuple2<Integer,String[]> >
	{
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
	 * Extracts and returns the base pairs corresponding to the {@code i} and {@code j} indices of a cell in the scoring matrix,
	 * in the order {reference,read}.
	 * 
	 * @param coordinates	An {@code int[]} of coordinates of a cell in the scoring matrix, in the form (i,j).
	 * @param sequences		A {@code String[]} of the sequences compared, in the order {reference,read}.
	 * 
	 * @return				A {@code char[]} of the base pairs corresponding to the given matrix cell coordinates, in the order {reference,read}.
	 */
	private static class GetBases implements Function2< int[] , String[] , char[] >
	{
		public char[] call( int[] coordinates , String[] sequences )
		{
			// j -> reference, i -> read
			char[] bases = { sequences[0].charAt(coordinates[1]-1) , sequences[1].charAt(coordinates[0]-1) } ;
			return bases ;
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
			return cellScore + gapScore ;
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
			if( Character.toUpperCase(bases[0]) == Character.toUpperCase(bases[1]) )	// match
				return nwScore + alignScores[0] ;
			else																		// mismatch
				return nwScore + alignScores[1] ;
		}
	}
	
	/**
	 * Returns the next, incomplete, diagonal grandchildren row of the matrix as an {@link java.util.ArrayList}, or {@code null} if it does not exist.
	 * 
	 * @param start			The {@code (i,j)} coordinates of the first cell of the next grandchildren row.
	 * 						The first cell in the row is defined as the cell with the smallest {@code j} value.
	 * @param defaultTuple	The default, initial {@link scala.Tuple2} of data required to calculate the score of a cell (the map function), grouped by data type.
	 * 						The {@code int[][]} contains, in order: 
	 * 							cell coordinates (i,j), 
	 * 							scores of cells required for score calculation (NW,N,W),
	 * 							alignment scores used for calculations (match,mismatch,gap), 
	 * 							the maximum (i,j) indices the cells may have.
	 * 						The {@code char[][]} contains, in order: 
	 * 							the (reference,read) base pairs corresponding to the cell,
	 * 							the alignment types used in this algorithm (alignment,insertion,deletion,none).
	 * @param arrInfo		A {@link scala.Tuple2} of other information used to obtain the next grandchildren list. 
	 * 						Contains, in order:
	 * 							an {@code int[]} of the maximum (i,j) indices of cells in the matrix,
	 * 							a {@code String[]} of the sequences in this comparison (reference,read).
	 * 
	 * @return				The next, incomplete, diagonal grandchildren row of the matrix as an {@link java.util.ArrayList}, 
	 * 						or {@code null} if it does not exist.
	 */
	private static class GetInitList implements Function3< int[] , Tuple2<int[][],char[][]> , Tuple2<int[],String[]> , ArrayList<Tuple2<int[][],char[][]>> >
	{
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
	 * Returns a deep copy of the given {@code Tuple2<int[][],char[][]>}.
	 * 
	 * @param tuple	The {@link scala.Tuple2} to be copied.
	 * 
	 * @return		The deep copy of the given {@code Tuple2<int[][],char[][]>}. 
	 */
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
	
	/**
	 * Returns the {@code (i,j)} coordinates of the first cell of the next grandchildren row.
	 * The first cell in the row is defined as the cell with the smallest {@code j} value.
	 * 
	 * @param start			The {@code (i,j)} coordinates of the first cell in the current row.
	 * @param maxIndices	The maximum {@code (i,j)} indices of cells in the matrix.
	 * 
	 * @return				The {@code (i,j)} coordinates of the first cell of the next grandchildren row.
	 */
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
	 * A {@link java.util.Comparator} of cell results from the map phase when filling the matrices.
	 * This orders cells in ascending order of their {@code j} coordinate values.
	 * 
	 * @see {@link java.util.Comparator#compare(Object, Object)}
	 */
	private static class CellResultComp implements Comparator<Tuple4<int[],Integer,Character,boolean[]>> , Serializable
	{
		public int compare( Tuple4<int[],Integer,Character,boolean[]> t1 , Tuple4<int[],Integer,Character,boolean[]> t2 )
		{
			return t1._1()[1] - t2._1()[1] ;
		}
	}
	
	/**
	 * A {@link java.util.Comparator} of matching sites.
	 * This orders elements in ascending order of the indices of the alignment.
	 * 
	 * @see {@link java.util.Comparator#compare(Object, Object)}
	 */
	public static class MatchSiteComp implements Comparator<Tuple2<Integer,String[]>> , Serializable
	{
		public int compare( Tuple2<Integer,String[]> t1 , Tuple2<Integer,String[]> t2 )
		{
			return t1._1().intValue() - t2._1().intValue() ;
		}
	}
}
