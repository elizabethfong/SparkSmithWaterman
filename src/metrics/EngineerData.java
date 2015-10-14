package metrics;

import java.io.BufferedWriter ;
import java.io.File ;
import java.io.FileWriter ;
import java.io.IOException ;

import java.lang.StringBuilder ;

public class EngineerData 
{
	private static final String NEWLINE = System.lineSeparator() ;
	
	private static final String REF = "CCTGGGTCCTGCCTCGCATCTGACCAGGGCAGGTGGCCTCCTCATCACACTGCTGCCTCTGCTGTTGGCCCTGCTCATGA" ;
	private static final String READ_80 = "AATTTTAGTCTCTCCCTACCCTTTTGGACAGAGCTTCCTGTCCTCTCATTTCACAGGTTATGCAACAGAGGGTTCTGTGT" ;
	private static final String READ_20 = "ACTGACTGACTGACTGACTG" ;
	
	private static final String REF_NAME = "ref" ;
	private static final String IN_NAME = "input" ;
	
	private static final String REF_EXT = ".rna.fna" ;
	private static final String IN_EXT = ".fa" ;
	
	private static final String DELIMITER = ">gi" ;
	
	
	/* --- INPUT ----------------------------------------------------------- */
	
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
	
	private static String getReadsSameLen( String read , int rows )
	{
		StringBuilder str = new StringBuilder() ;
		
		for( int i = 0 ; i < rows ; i++ )
			str.append( read + NEWLINE ) ;
		
		return str.toString() ;
	}
	
	private static String getPath( String directory , String name , int num , String ext )
	{
		return directory + "/" + name + num + ext ;
	}
	
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
	
	
	
	
	
	
	
	public static void main( String[] args )
	{
		//changeReadNum( "C:/Users/Elizabeth/Desktop/input/readNum" ) ;
		//changeReadLen( "C:/Users/Elizabeth/Desktop/input/readLen" ) ;
		//changeRefNum( "C:/Users/Elizabeth/Desktop/testRef/refNum" ) ;
		//changeRefLen( "C:/Users/Elizabeth/Desktop/testRef/refLen" ) ;
	}
}
