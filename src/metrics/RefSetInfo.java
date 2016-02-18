package metrics ;

import java.io.File ;

import java.lang.StringBuilder ;

import java.util.ArrayList ;
import java.util.Collections;
import java.util.Comparator ;
import java.util.Formatter ;
import java.util.Locale ;

import scala.Tuple2 ;
import scala.Tuple5 ;

import sw.DirectoryCrawler ;
import sw.InOutOps ;


/**
 * Prints out information about the dataset of reference sequences to standard out.
 * 
 * @author Elizabeth Fong
 * @version Insight Data Engineering NY, September-October 2015
 */
public class RefSetInfo 
{
	// CONSTANTS
	private static final String NEWLINE = System.lineSeparator() ;
	
	// the directory
	private static final String REF_DIR = "/home/ubuntu/project/reference" ;
	private static final String DELIMITER = ">gi" ;
	
	
	/* --- INFORMATION EXTRACTION ------------------------------------------ */
	
	/**
	 * Extracts and returns information from the given directory of reference sequences.
	 * 
	 * @param directory	The directory of reference sequences.
	 * 
	 * @return			The extracted information, in a {@link scala.Tuple5}, in the following order:
	 * 					1 - path of the directory
	 * 					2 - number of files in the directory
	 * 					3 - a {@code long[]} containing, in order: 
	 * 						the number of sequences , 
	 * 						total number of base pairs, 
	 * 						minimum number of base pairs in a sequence , 
	 * 						maximum number of base pairs in a sequence
	 * 					4 - a {@code double[]} containing, in order:
	 * 						mean number of base pairs in a sequence
	 * 						median number of base pairs in a sequence
	 * 					5 - an {@link java.util.ArrayList} of files and their corresponding number of sequences.
	 */
	public static Tuple5<String,Integer,long[],double[],ArrayList<Tuple2<String,Integer>>> getInfo( String directory )
	{
		if( directory == null || directory.trim().length() == 0 )
			directory = REF_DIR ;
		
		// variables
		int numFiles = 0 ;
		
		long numSeqs = 0 ;
		long totalBp = 0 ;
		
		long minBp = Long.MAX_VALUE ;
		long maxBp = 0 ;
		
		double mean = 0 ;
		double median = 0 ;
		
		RunningMedian runningMed = new RunningMedian() ;
		
		// read files
		DirectoryCrawler dir = new DirectoryCrawler( directory ) ;
		ArrayList<Tuple2<String,Integer>> fileList = new ArrayList<Tuple2<String,Integer>>() ;
		
		while( dir.hasNext() )
		{
			File next = dir.next() ;
			ArrayList<String[]> refs = new InOutOps.GetRefSeqs().call( next , DELIMITER ) ;
			
			// extract file name from file path
			String[] path = next.getPath().split( "/" ) ;
			
			fileList.add( new Tuple2<String,Integer>(path[path.length-1],refs.size()) ) ;
			
			numSeqs += refs.size() ;
			numFiles ++ ;
			
			for( String[] ref : refs )
			{
				int bp = ref[1].length() ;
				totalBp += bp ;
				runningMed.add( bp ) ;
				
				// min/max
				if( bp < minBp )
					minBp = bp ;
				
				if( bp > maxBp )
					maxBp = bp ;
			}
		}
		
		mean = ( (double) totalBp ) / numSeqs ;
		median = runningMed.getRunningMedian() ;
		
		
		// return
		long[] longs = { numSeqs , totalBp , minBp , maxBp } ;
		double[] doubles = { mean , median } ;
		
		return new Tuple5<String,Integer,long[],double[],ArrayList<Tuple2<String,Integer>>>( directory , numFiles , longs , doubles , fileList ) ;
	}
	
	
	/* --- FORMATTING ------------------------------------------------------ */
	
	/**
	 * Extracts information the given directory of reference sequences,
	 * formats the extracted information and 
	 * writes this information to the file located in the given output file path.
	 * 
	 * @param directory		The directory of reference sequences.
	 * @param outputFile	The path of the output file where the information is to be written to.
	 */
	public static void printAllInfo( String directory , String outputFile )
	{
		StringBuilder str = new StringBuilder() ;
		Tuple5<String,Integer,long[],double[],ArrayList<Tuple2<String,Integer>>> result = getInfo( directory ) ;
		
		// directory
		str.append( "directory = " + result._1() + NEWLINE ) ;
		
		// # files, # sequences, # base pairs
		str.append( NEWLINE ) ;
		str.append( "# files  =  " + result._2() + NEWLINE ) ;
		str.append( format3("%-21s  %1s  %-,11d","# reference sequences","=",result._3()[0]) + NEWLINE ) ;
		str.append( format3("%-21s  %1s  %-,11d","# total base pairs","=",result._3()[1]) + NEWLINE ) ;
		
		// min, max, mean, median - bp
		str.append( NEWLINE ) ;
		str.append( "base pairs in a sequence:" + NEWLINE ) ;
		str.append( "-------------------------" + NEWLINE ) ;
		str.append( format3("%-6s  %1s  %-,10d","min","=",result._3()[2]) + NEWLINE ) ;
		str.append( format3("%-6s  %1s  %-,10d","max","=",result._3()[3]) + NEWLINE ) ;
		str.append( format3("%-6s  %1s  %-,7.2f","mean","=",result._4()[0]) + NEWLINE ) ;
		str.append( format3("%-6s  %1s  %-,7.2f","median","=",result._4()[1]) + NEWLINE ) ;
		
		// file list - alphabetical order (ascending)
		str.append( NEWLINE + NEWLINE ) ;
		ArrayList<Tuple2<String,Integer>> table = result._5() ;
		Collections.sort( table , new FilenameComparator() ) ;
		str.append( getFormattedTable(table) ) ;
		
		// file list - size order (ascending)
		str.append( NEWLINE + NEWLINE ) ;
		table = result._5() ;
		Collections.sort( table , new NumRefComparator() ) ;
		str.append( getFormattedTable(table) ) ;
		
		// print to file
		new InOutOps.PrintStrToFile().call( outputFile , str.toString() ) ;
	}
	
	/**
	 * Given an {@link java.util.ArrayList} of {@link scala.Tuple2} elements containing elements in each row
	 * of the table, returns a formatted {@code String} displaying the elements in a table.
	 * 
	 * @param table An {@link java.util.ArrayList} of {@link scala.Tuple2} elements containing elements 
	 * 				in each row of the table.
	 * 
	 * @return		A formatted {@code String} displaying the elements in a table.
	 */
	private static String getFormattedTable( ArrayList<Tuple2<String,Integer>> table )
	{
		StringBuilder str = new StringBuilder() ;
		Formatter formatter = new Formatter( null , Locale.US ) ;
		
		// header
		str.append( format3("%-35s%1s%11s","File Name","|","# Sequences") + NEWLINE ) ;
		str.append( "-----------------------------------+-----------" + NEWLINE ) ;
		formatter.close() ;
		
		// rows
		for( Tuple2<String,Integer> row : table )	
		{
			formatter = new Formatter() ;
			str.append( format3("%-35s%1s%,11d",row._1(),"|",row._2()) + NEWLINE ) ;
			formatter.close() ;
		}
		
		// return
		return str.toString() ;
	}
	
	/**
	 * Returns a formatted {@code String} of 3 elements, using the given format {@code String}.
	 * 
	 * @param format	A format {@code String} as defined by {@link java.util.Formatter}.
	 * @param arg1		The first element.
	 * @param arg2		The second element.
	 * @param arg3		The third element.
	 * 
	 * @return			A formatted {@code String} of 3 elements, using the given format {@code String}.
	 */
	private static String format3( String format , Object arg1 , Object arg2 , Object arg3 )
	{
		Formatter formatter = new Formatter() ;
		
		Object[] args = {arg1,arg2,arg3} ;
		formatter.format( format , args ) ;
		String str = formatter.toString() ;
		
		formatter.close() ;
		return str ;
	}
	
	
	/* --- COMPARATORS ----------------------------------------------------- */
	
	/**
	 * A {@link java.util.Comparator} of {@link scala.Tuple2} with file names and their number of sequences.
	 * This orders elements in ascending order of file names.
	 * 
	 * @see {@link java.util.Comparator#compare(Object, Object)}
	 */
	private static class FilenameComparator implements Comparator<Tuple2<String,Integer>>
	{
		public int compare( Tuple2<String,Integer> t1 , Tuple2<String,Integer> t2 )
		{
			return t1._1().compareTo( t2._1() ) ;
		}
	}
	
	/**
	 * A {@link java.util.Comparator} of {@link scala.Tuple2} with file names and their number of sequences.
	 * This orders elements in ascending order of number of sequences contained in each file.
	 * 
	 * @see {@link java.util.Comparator#compare(Object, Object)}
	 */
	private static class NumRefComparator implements Comparator<Tuple2<String,Integer>>
	{
		public int compare( Tuple2<String,Integer> t1 , Tuple2<String,Integer> t2 )
		{
			return t1._2() - t2._2() ;
		}
	}
	
	
	/* --- MAIN ------------------------------------------------------------ */
	
	/**
	 * Prints out information about the dataset of reference sequences to standard out.
	 * 
	 * @param args None expected.
	 */
	public static void main( String[] args )
	{
		String directory = "C:/Users/Elizabeth/Desktop/reference" ;
		String outputFile = "C:/Users/Elizabeth/Desktop/refSetInfo.txt" ;
		
		RefSetInfo.printAllInfo( directory , outputFile ) ;
	}
}
