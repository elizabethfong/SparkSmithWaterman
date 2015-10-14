package metrics ;

import java.io.BufferedWriter ;
import java.io.File ;
import java.io.FileWriter ;
import java.io.IOException ;

import java.lang.StringBuilder ;


/**
 * This class has methods which engineers datasets for more accurate benchmarking.
 * 
 * @author Elizabeth Fong
 * @version Insight Data Engineering NY, September-October 2015
 */
public class EngineerData 
{
	// CONSTANTS
	private static final String NEWLINE = System.lineSeparator() ;
	
	// a line of reference sequence
	private static final String REF = "CCTGGGTCCTGCCTCGCATCTGACCAGGGCAGGTGGCCTCCTCATCACACTGCTGCCTCTGCTGTTGGCCCTGCTCATGA" ;
	
	// a read - 80bp
	private static final String READ_80 = "AATTTTAGTCTCTCCCTACCCTTTTGGACAGAGCTTCCTGTCCTCTCATTTCACAGGTTATGCAACAGAGGGTTCTGTGT" ;
	
	// a part of a read - 20bp
	private static final String READ_20 = "ACTGACTGACTGACTGACTG" ;
	
	// file names
	private static final String REF_NAME = "ref" ;
	private static final String IN_NAME = "input" ;
	
	// file extensions
	private static final String REF_EXT = ".rna.fna" ;
	private static final String IN_EXT = ".fa" ;
	
	// delimiter
	private static final String DELIMITER = ">gi" ;
	
	
	/* --- INPUT ----------------------------------------------------------- */
	
	/**
	 * In the given directory, creates multiple input files with varying numbers of reads.
	 * The length of each read is the same (80bp).
	 * 
	 * @param directory The directory the files are to be created in.
	 */
	public static void changeReadNum( String directory )
	{
		// special # of rows, increment remainder of reads by num
		int[] rows = {20} ;
		String addedReads = getReadsSameLen(READ_80,50) ;
		
		int counter = 0 ;
		
		
		// write to file - special # of rows
		for( counter = 1 ; counter <= rows.length ; counter++ )
			writeToFile( getReadsSameLen(READ_80,rows[counter-1]) , getPath(directory,IN_NAME,counter,IN_EXT) ) ;
		
		counter -- ;
		
		
		// remainder input files - increment by 50 reads
		int numReads = 0 ; 
		StringBuilder str = new StringBuilder() ;
		
		while( numReads <= 1574 )
		{
			counter ++ ;
			numReads += 50 ;
			str.append( addedReads ) ;
			
			writeToFile( str.toString() , getPath(directory,IN_NAME,counter,IN_EXT) ) ;
		}
	}
	
	/**
	 * In the given directory, creates multiple input files with varying read lengths.
	 * The number of reads in each file is the same (5).
	 * 
	 * @param directory The directory the files are to be created in.
	 */
	public static void changeReadLen( String directory )
	{
		int counter = 0 ;
		int readLen = 0 ;
		
		StringBuilder read = new StringBuilder() ;
		
		while( readLen < 500 )
		{
			counter ++ ;
			readLen += 20 ;
			
			read.append( READ_20 ) ;
			String reads = getReadsSameLen( read.toString() , 5 ) ;
			
			writeToFile( reads , getPath(directory,IN_NAME,counter,IN_EXT) ) ;
		}
	}
	
	
	/* --- REFERENCE ------------------------------------------------------- */
	
	/**
	 * In the given directory, creates sub-directories which contain a reference file each.
	 * Each reference file has different numbers of reference sequences, 
	 * but each sequence has the same length (400bp).
	 * 
	 * @param directory The directory the sub-directories and files are to be created in.
	 */
	public static void changeRefNum( String directory )
	{
		String refSeq = getReadsSameLen( REF , 5 ) ;
		StringBuilder str = new StringBuilder() ;
		
		int counter = 0 ;
		int refNum = 0 ;
		
		// special numbers
		int[] nums = {1,10,30,50,100,500,1000,1500,2000} ;
		
		for( counter = 1 ; counter <= nums.length ; counter ++ )
		{
			String subDir = directory + "/" + REF_NAME + counter ;
			
			// if directory does not exist
			File tmp = new File( subDir ) ;
			if( ! tmp.exists() )
				tmp.mkdirs() ;
			
			// get sequences
			for( int i = refNum + 1 ; i <= nums[counter-1] ; i++ )
			{
				str.append( DELIMITER + "|" + REF_NAME + i + NEWLINE ) ;
				str.append( refSeq ) ;
			}
			
			refNum = nums[counter-1] ;
			writeToFile( str.toString() , getPath(subDir,REF_NAME,counter,REF_EXT) ) ;
		}
		
		counter -- ;
		
		while( refNum < 40000 )
		{
			counter ++ ;
			String subDir = directory + "/" + REF_NAME + counter ;
			
			// if directory does not exist
			File tmp = new File( subDir ) ;
			if( ! tmp.exists() )
				tmp.mkdirs() ;
			
			// get sequences
			for( int i = refNum + 1 ; i <= refNum + 2000 ; i++ )
			{
				str.append( DELIMITER + "|" + REF_NAME + i + NEWLINE ) ;
				str.append( refSeq + NEWLINE ) ;
			}
			
			refNum = refNum + 2000 ;
			writeToFile( str.toString() , getPath(subDir,REF_NAME,counter,REF_EXT) ) ;
		}
	}
	
	/**
	 * In the given directory, creates sub-directories which contain a reference file each.
	 * Each reference file has the same number of reference sequences (1),
	 * but each sequence has a different length.
	 * 
	 * @param directory The directory the sub-directories and files are to be created in.S
	 */
	public static void changeRefLen( String directory )
	{
		// is the change read num code, but with directory creation.
		String metadata = DELIMITER + "|" + REF_NAME + NEWLINE ;
		
		// special # of rows, increment remainder of reads by num
		int[] rows = {1,5,10,20} ;
		String addedSeq = getReadsSameLen(REF,50) ;
		
		int counter = 0 ;
		
		
		// write to file - special # of rows
		for( counter = 1 ; counter <= rows.length ; counter++ )
		{
			String subDir = directory + "/" + REF_NAME + counter ;
			
			File tmp = new File(subDir) ;
			if( ! tmp.exists() )
				tmp.mkdirs() ;
			
			writeToFile( metadata+getReadsSameLen(REF,rows[counter-1]) , getPath(subDir,REF_NAME,counter,REF_EXT) ) ;
		}
		
		counter -- ;
		
		
		// remainder reference files - increment by 50 lines
		int numLines = 0 ; 
		StringBuilder str = new StringBuilder() ;
		str.append( metadata ) ;
		
		while( numLines <= 1574 )
		{
			counter ++ ;
			numLines += 50 ;
			str.append( addedSeq ) ;
			
			String subDir = directory + "/" + REF_NAME + counter ;
			
			File tmp = new File(subDir) ;
			if( ! tmp.exists() )
				tmp.mkdirs() ;
			
			writeToFile( str.toString() , getPath(subDir,REF_NAME,counter,REF_EXT) ) ;
		}
	}
	
	
	/* --- UTILITY --------------------------------------------------------- */
	
	/**
	 * Returns a {@link java.util.String} of the given read, replicated over the
	 * given number of rows.
	 * 
	 * @param read	The read to be repeated.
	 * @param rows	The number of rows the read is to be repeated by.
	 * 
	 * @return		A {@link java.util.String} of the given read, replicated over the
	 * 				given number of rows.
	 */
	private static String getReadsSameLen( String read , int rows )
	{
		StringBuilder str = new StringBuilder() ;
		
		for( int i = 0 ; i < rows ; i++ )
			str.append( read + NEWLINE ) ;
		
		return str.toString() ;
	}
	
	/**
	 * Returns a {@link java.util.String} of the file path formed from the given parameters.
	 * 
	 * @param directory	The directory of the file.
	 * @param name		The name of the file.
	 * @param num		The number of the file. Appended to the name of the file.
	 * @param ext		The file extension.
	 * 
	 * @return			A {@link java.util.String} of the file path formed from the given parameters.
	 */
	private static String getPath( String directory , String name , int num , String ext )
	{
		return directory + "/" + name + num + ext ;
	}
	
	/**
	 * Writes the given {@link java.lang.String} to the file specified by the given path.
	 * 
	 * @param str			The {@link java.lang.String} to be written to file.
	 * @param outputPath	The path of the output file.
	 */
	private static void writeToFile( String str , String outputPath )
	{
		File file = new File( outputPath ) ;
		
		try
		{
			if( !file.exists() )
				file.createNewFile() ;
			
			BufferedWriter writer = new BufferedWriter( new FileWriter(file.getAbsolutePath(),false) ) ;
			
			writer.write( str.trim() ) ;
			writer.close() ;
		}
		catch( IOException ioe )
		{
			System.out.println( "IOException on writing to file" ) ;
			ioe.printStackTrace() ;
			System.exit(0) ;
		}
	}
	
	
	/* --- MAIN ------------------------------------------------------------ */
	
	/**
	 * Main method. Runs all public methods in {@link metrics.EngineerData} to
	 * enable more accurate benchmarking.
	 * 
	 * @param args None expected.
	 */
	public static void main( String[] args )
	{
		changeReadNum( "/home/ubuntu/project/input/readNum" ) ;
		changeReadLen( "/home/ubuntu/project/input/readLen" ) ;
		changeRefNum( "/home/ubuntu/project/testRef/refNum" ) ;
		changeRefLen( "/home/ubuntu/project/testRef/refLen" ) ;
	}
}
