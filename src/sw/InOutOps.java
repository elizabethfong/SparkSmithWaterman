package sw ;

import java.io.BufferedWriter ;
import java.io.File ;
import java.io.FileNotFoundException ;
import java.io.FileWriter ;
import java.io.IOException ;

import java.lang.StringBuilder ;

import java.util.ArrayList ;
import java.util.Scanner ;

import org.apache.spark.api.java.function.Function2 ;
import org.apache.spark.api.java.function.Function3 ;

import scala.Tuple2 ;
import scala.Tuple3 ;


/**
 * Class of functions dealing with input and ouput, restricted to:
 * reading from and writing to files, 
 * formatting data into a {@link java.lang.String} for printing, and
 * printing data to standard out.
 * 
 * Written in functional Spark.
 * 
 * @author Elizabeth Fong
 * @version Insight Data Engineering NY, September-October 2015
 */
@SuppressWarnings( "serial" )
public class InOutOps
{
	// Constants
	public static final String NEWLINE = System.lineSeparator() ;
	public static final String TAB = "	" ;
	
	
	/* --- INPUT ----------------------------------------------------------- */
	
	/**
	 * Extracts all reads from an input file and returns them, in order, in an {@link java.util.ArrayList}. 
	 * 
	 * @param file		The input file.
	 * @param delimiter	The {@link java.lang.String} which shows that a line in the file is metadata.
	 * 
	 * @return			An {@link java.util.ArrayList} of all reads from the given input file.
	 */
	public static class GetReads implements Function2< File , String , ArrayList<String> >
	{
		public ArrayList<String> call( File file , String delimiter )
		{
			ArrayList<String> reads = new ArrayList<String>() ;
			
			try
			{
				Scanner scanner = new Scanner( file ) ;
				
				// first line - may contain metadata
				String line = scanner.nextLine().trim() ;
				
				if( ! new IsMetadata().call(line,delimiter).booleanValue() )
					reads.add( line ) ;
				
				// subsequent lines
				while( scanner.hasNextLine() )
					reads.add( scanner.nextLine().trim() ) ;
				
				// done!
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
	
	/**
	 * Extracts each reference sequence from a file of reference sequences and returns them,
	 * in order, in an {@code ArrayList{String[]}, where each element has the form 
	 * { metadata , sequence }.
	 * 
	 * @param file		The file of reference sequences.
	 * @param delimiter	The {@link java.lang.String} which separates a reference sequence from another.
	 * 
	 * @return			An {@link java.util.ArrayList} of all reference sequences in the given file.
	 */
	public static class GetRefSeqs implements Function2< File , String , ArrayList<String[]> >
	{
		public ArrayList<String[]> call( File file , String delimiter )
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
					
					if( new IsMetadata().call(line,delimiter).booleanValue() )
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
	
	
	/* --- OUTPUT ---------------------------------------------------------- */
	
	/**
	 * Prints the given {@link java.lang.String} to the file with the given file path.
	 * The directory in which the file is to reside in must exist.
	 * 
	 * @param filepath	The file path.
	 * @param data		The data to be written to file.
	 * 
	 * @return			{@code true} if writing to file was successful, {@code false} otherwise.
	 */
	public static class PrintStrToFile implements Function2< String , String , Boolean >
	{
		public Boolean call( String filepath , String data )
		{
			try
			{
				File file = new File( filepath ) ;
				
				if( ! file.exists() )
					file.createNewFile() ;
				
				BufferedWriter writer = new BufferedWriter( new FileWriter(file.getAbsolutePath(),false) ) ;
				
				writer.write( data ) ;
				writer.close() ;
				
				return true ;
			}
			catch( IOException ioe )
			{
				System.out.println( "IOException on writing to file" ) ;
				ioe.printStackTrace() ;
				return false ;
			}
		}
	}
	
	/**
	 * Generates and returns a formatted {@link java.lang.String} of the given information. 
	 * 
	 * @param reads	An {@link java.util.ArrayList} of all reads in this input.
	 * @param data	A {@link scala.Tuple3} of the following, in order:
	 * 					number of reference sequences,
	 * 					number of reads,
	 * 					maximum alignment score,
	 * 					execution time
	 * @param opt	An {@link java.util.ArrayList} of the best-matched reference sequences and
	 * 				the corresponding matching locations.
	 * 
	 * @return		A formatted {@link java.lang.String} of the given information.
	 */
	public static class GetOutputStr implements Function3< ArrayList<String> , Tuple3<int[],Integer,Long> , ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> , String >
	{
		public String call( ArrayList<String> reads , Tuple3<int[],Integer,Long> data , ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> opt )
		{
			StringBuilder str = new StringBuilder() ;
			
			// execution time
			str.append( "Execution Time = " + data._3() + " ms" + NEWLINE ) ;
			str.append( NEWLINE ) ;
			
			// num reference and reads
			str.append( "# Reference Sequences = " + data._1()[0] + NEWLINE ) ;
			str.append( "# Reads = " + data._1()[1] + NEWLINE ) ;
			str.append( NEWLINE ) ;
			
			// Input
			str.append( "Input:" + NEWLINE ) ;
			for( String read : reads )
				str.append( read + NEWLINE ) ;
			str.append( NEWLINE ) ;
			
			// max score
			str.append( "Maximum alignment score = " + data._2() ) ;
			str.append( NEWLINE ) ;
			
			// print sequences & matching sites
			for( Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>> seq : opt )
			{
				str.append( "Reference:" + NEWLINE ) ;
				str.append( seq._1()[0] + NEWLINE ) ;
				str.append( seq._1()[1] + NEWLINE ) ;
				str.append( NEWLINE ) ;
				
				// matching sites
				ArrayList<Tuple2<Integer,String[]>> sites = seq._2() ;
				
				for( Tuple2<Integer,String[]> site : sites )
				{
					str.append( TAB + "Index = " + site._1() + NEWLINE ) ;
					str.append( TAB + site._2()[0] + NEWLINE ) ;
					str.append( TAB + site._2()[1] + NEWLINE ) ;
					str.append( NEWLINE ) ;
				}
			}
			
			return str.toString() ;
		}
	}
	
	/**
	 * Returns a formatted {@link java.lang.String} of the matrices of scores and corresponding 
	 * alignment types generated in the first step of the Smith-Waterman algorithm.
	 * Also prints this {@link java.lang.String} to standard out.
	 * 
	 * @param scores	The matrix of scores.
	 * @param aligns	The matrix of corresponding alignment types
	 * @param seqs		The sequences used in the alignment process, in the form { reference , read }.
	 * 
	 * @return			A formatted {@link java.lang.String} of the matrices of scores and corresponding alignment types.
	 */
	public static class PrintMatrices implements Function3< int[][] , char[][] , String[] , String >
	{
		public String call( int[][] scores , char[][] aligns , String[] seqs )
		{
			StringBuilder str = new StringBuilder() ;
			
			String _seq1 = seqs[1] ;
			String _seq2 = seqs[0] ;
			
			// score matrix
			str.append( NEWLINE ) ;
			str.append( "   _  " ) ;
			
			for( int i = 0 ; i < _seq2.length() ; i++ )
			{
				str.append( Character.toUpperCase(_seq2.charAt(i)) + "  " ) ;
			}
			
			str.append( NEWLINE ) ;
			
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
				
				str.append( NEWLINE ) ;
			}
			
			str.append( NEWLINE ) ;
			
			// align type matrix
			str.append( "   _  " ) ;
			
			for( int i = 0 ; i < _seq2.length() ; i++ )
			{
				str.append( Character.toUpperCase(_seq2.charAt(i)) + "  " ) ;
			}
			
			str.append( NEWLINE ) ;
			
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
				
				str.append( NEWLINE ) ;
			}
			
			System.out.println( str.toString() ) ;
			return str.toString() ;
		}
	}
	
	
	/* --- UTILITY --------------------------------------------------------- */
	
	/**
	 * Returns {@code true} if the given line is a line of metadata, {@code false} otherwise.
	 * 
	 * @param line		The {@link java.lang.String} to be checked.
	 * @param delimiter	The {@link java.lang.String} that identifies a line as metadata.
	 * 
	 * @return			{@code true} if the given line is a line of metadata, {@code false} otherwise.
	 */
	private static class IsMetadata implements Function2< String , String , Boolean >
	{
		public Boolean call( String line , String delimiter )
		{
			if( line.length() >= delimiter.length() && line.substring(0,delimiter.length()).equals(delimiter) )
				return true ;
			else
				return false ;
		}
	}
}
