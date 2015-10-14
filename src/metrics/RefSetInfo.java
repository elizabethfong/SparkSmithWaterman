package metrics ;

import java.io.File ;

import java.lang.StringBuilder ;

import java.util.ArrayList ;
import java.util.Comparator ;
import java.util.Formatter ;
import java.util.Locale ;

import scala.Tuple2 ;
import scala.Tuple5 ;

import sw.DirectoryCrawler ;
import sw.InOutOps ;

/**
 * Prints out information about the dataset of reference sequences to standard out.
 * TODO
 * 
 * @author Elizabeth Fong
 * @version Insight Data Engineering NY, September-October 2015
 */
public class RefSetInfo 
{
	private static final String NEWLINE = System.lineSeparator() ;
	
	// entire directory
	private static final String REF_DIR = "/home/ubuntu/project/reference" ;
	private static final String DELIMITER = ">gi" ;
	
	
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
			String[] path = next.getPath().split( "\\\\" ) ;
			
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
		table.sort( new FilenameComparator() ) ;
		str.append( getFormattedTable(table) ) ;
		
		// file list - size order (ascending)
		str.append( NEWLINE + NEWLINE ) ;
		table = result._5() ;
		table.sort( new NumRefComparator() ) ;
		str.append( getFormattedTable(table) ) ;
		
		// print to file
		new InOutOps.PrintStrToFile().call( outputFile , str.toString() ) ;
	}
	
	private static String getFormattedTable( ArrayList<Tuple2<String,Integer>> table )
	{
		StringBuilder str = new StringBuilder() ;
		Formatter formatter = new Formatter( null , Locale.US ) ;
		
		// header
		str.append( format3("%-35s%1s%11s","File Name","|","# Sequences") + NEWLINE ) ;
		str.append( "-----------------------------------|-----------" + NEWLINE ) ;
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
	
	private static String format3( String format , Object arg1 , Object arg2 , Object arg3 )
	{
		Formatter formatter = new Formatter() ;
		
		Object[] args = {arg1,arg2,arg3} ;
		formatter.format( format , args ) ;
		String str = formatter.toString() ;
		
		formatter.close() ;
		return str ;
	}
	
	private static class FilenameComparator implements Comparator<Tuple2<String,Integer>>
	{
		public int compare( Tuple2<String,Integer> t1 , Tuple2<String,Integer> t2 )
		{
			return t1._1().compareTo( t2._1() ) ;
		}
	}
	
	private static class NumRefComparator implements Comparator<Tuple2<String,Integer>>
	{
		public int compare( Tuple2<String,Integer> t1 , Tuple2<String,Integer> t2 )
		{
			return t1._2() - t2._2() ;
		}
	}
	
	
	
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
