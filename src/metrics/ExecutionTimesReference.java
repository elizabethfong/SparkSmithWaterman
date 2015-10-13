package metrics ;

import java.io.BufferedWriter ;
import java.io.File ;
import java.io.FileWriter ;
import java.io.IOException ;

import sw.DirectoryCrawler ;
import sw.Distribution ;

public class ExecutionTimesReference 
{
	private static final String NEWLINE = System.lineSeparator() ;
	
	private static final String REF_DIR = "/home/ubuntu/project/testRef" ;
	private static final String IN_DIR = "/home/ubuntu/project/input" ;
	
	private static final String IN_NAME = "input" ;
	private static final String IN_EXT = ".fa" ;
	
	private static final String READ = "AATTTTAGTCTCTCCCTACCCTTTTGGACAGAGCTTCCTGTCCTCTCATTTCACAGGTTATGCAACAGAGGGTTCTGTGT" ;
	
	private static final int MAX_READS = 1574 ;
	private static final int READ_INC = 50 ;
	
	// exec time against # reads
	public static void runTest()
	{
		String refDir = "/home/ubuntu/project/testRef/ref1" ;
		String inDir = "/home/ubuntu/project/input/in1" ;
		String delimiter = null ;
		String outDir = "/home/ubuntu/project/output/reference/readTest" ;
		String outFileName = null ;
		String outFileExt = null ;
		
		String[] args = { refDir , inDir , delimiter , outDir , outFileName , outFileExt } ;
		new Distribution.DistributeReference().call( args , null ) ;
	}
	
	// exec time against # refs
	public static void runTest1()
	{
		String refDir = "/home/ubuntu/project/testRef/ref" ;
		String inDir = "/home/ubuntu/project/input/in2" ;
		String delimiter = null ;
		String outDir = "/home/ubuntu/project/output/reference/refTest" ;
		String outFileName = "result";
		String outFileExt = null ;
		
		for( int i = 1 ; i <= 32 ; i++ )
		{
			String[] args = { refDir+i , inDir , delimiter , outDir , outFileName+i+"_" , outFileExt } ;
			new Distribution.DistributeReference().call( args , null ) ;
		}
	}
	
	
	public static void createInputFiles( String directory )
	{
		String readsAppend = createInputAppend(READ_INC) ;
		
		int counter = 0 ;
		int numReads = 0 ;
		
		String filepath = "" ;
		StringBuilder reads = new StringBuilder() ;
		
		while( numReads <= MAX_READS )
		{
			// update counters
			counter ++ ;
			numReads += READ_INC ;

			filepath = directory + IN_NAME + counter + IN_EXT ;
			reads.append( readsAppend ) ;
			
			
			try
			{	
				File file = new File( filepath ) ;
				
				if( ! file.exists() )
					file.createNewFile() ;
				
				BufferedWriter writer = new BufferedWriter( new FileWriter(file.getAbsolutePath(),false) ) ;
				
				writer.write( reads.toString() ) ;
				writer.close() ;
			}
			catch( IOException ioe )
			{
				System.out.println( "IOException on writing to file" ) ;
				ioe.printStackTrace() ;
			}
		}
	}
	
	private static String createInputAppend( int numRows )
	{
		StringBuilder str = new StringBuilder() ;
		
		for( int i = 0 ; i < numRows ; i++ )
		{
			str.append( READ + NEWLINE ) ;
		}
		
		return str.toString() ;
	}
	
	public static void main( String[] args )
	{	
		runTest() ;
		runTest1() ;
	}
	
}
