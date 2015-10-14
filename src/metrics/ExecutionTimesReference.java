package metrics ;

import sw.Distribution ;


/**
 * This class contains benchmarking tests on the Smith-Waterman algorithm, distributing the
 * reference dataset. Each test varies 1 factor while keeping the others constant.
 * 
 * @author Elizabeth Fong
 * @version Insight Data Engineering NY, September-October 2015
 */
public class ExecutionTimesReference 
{	
	// change num reads - file directories
	private static final String REF1 = "/home/ubuntu/project/testRef/in" ;
	private static final String IN1 = "/home/ubuntu/project/input/readNum" ;
	private static final String OUT1 = "/home/ubuntu/project/output/reference/readNum" ;
	
	// change read lengths - file directories
	private static final String REF2 = "/home/ubuntu/project/testRef/in" ;
	private static final String IN2 = "/home/ubuntu/project/input/readLen" ;
	private static final String OUT2 = "/home/ubuntu/project/output/reference/readLen" ;
	
	// change num references - file directories
	private static final String REF3 = "/home/ubuntu/project/testRef/refNum" ;
	private static final String IN3 = "/home/ubuntu/project/input/ref" ;
	private static final String OUT3 = "/home/ubuntu/project/output/reference/refNum" ;
	
	// change reference lengths - file directories
	private static final String REF4 = "/home/ubuntu/project/testRef/refLen" ;
	private static final String IN4 = "/home/ubuntu/project/input/ref" ;
	private static final String OUT4 = "/home/ubuntu/project/output/reference/refLen" ;
	
	
	/* --- TESTS ----------------------------------------------------------- */
	
	/**
	 * A test which changes the number of reads.
	 * Variable: # reads
	 * Constant: read length, # reference sequences, reference sequence length, # files of reference sequences
	 */
	public static void runTest1()
	{
		String refDir = REF1 ;
		String inDir = IN1 ;
		String delimiter = null ;
		String outDir = OUT1 ;
		String outFileName = null ;
		String outFileExt = null ;
		
		String[] args = { refDir , inDir , delimiter , outDir , outFileName , outFileExt } ;
		new Distribution.DistributeReference().call( args , null ) ;
	}
	
	/**
	 * A test which changes the length of the reads.
	 * Variable: read length
	 * Constant: # reads, # reference sequences, length of reference sequences, # files of reference sequences
	 */
	public static void runTest2()
	{
		String refDir = REF2 ;
		String inDir = IN2 ;
		String delimiter = null ;
		String outDir = OUT2 ;
		String outFileName = null ;
		String outFileExt = null ;
		
		String[] args = { refDir , inDir , delimiter , outDir , outFileName , outFileExt } ;
		new Distribution.DistributeReference().call( args , null ) ;
	}
	
	/**
	 * A test which changes the number of reference sequences.
	 * Variable: # reference sequences
	 * Constant: # reads, read length, length of reference sequence, # files of reference sequences
	 */
	public static void runTest3()
	{
		String refDirParent = REF3 ;
		String inDir = IN3 ;
		String delimiter = null ;
		String outDir = OUT3 ;
		String outGeneralName = "result" ;
		String outFileExt = null ;
		
		int numDirs = 28 ;
		
		for( int i = 1 ; i <= numDirs ; i++ )
		{
			String refDir = refDirParent + "/ref" + i ;
			String outFileName = outGeneralName + i + "_" ;
			
			String[] args = { refDir , inDir , delimiter , outDir , outFileName , outFileExt } ;
			new Distribution.DistributeReference().call( args , null ) ;
		}
	}
	
	/**
	 * A test which changes the length of the reference sequences.
	 * Variable: length of reference sequences
	 * Constant: # reads, read length, # reference sequences, # files of reference sequences.
	 */
	public static void runTest4()
	{
		String refDirParent = REF4 ;
		String inDir = IN4 ;
		String delimiter = null ;
		String outDir = OUT4 ;
		String outGeneralName = null ;
		String outFileExt = null ;
		
		int numDirs = 36 ;
		
		for( int i = 1 ; i <= numDirs ; i++ )
		{
			String refDir = refDirParent + "/ref" + i ;
			String outFileName = outGeneralName + i + "_" ;
			
			String[] args = { refDir , inDir , delimiter , outDir , outFileName , outFileExt } ;
			new Distribution.DistributeReference().call( args , null ) ;
		}
	}
	
	
	/* --- MAIN ------------------------------------------------------------ */
	
	/**
	 * Main method. Runs the 4 benchmarking tests present in {@link metrics.ExecutionTimesReference}.
	 * 
	 * @param args None expected.
	 */
	public static void main( String[] args )
	{	
		runTest1() ;
		runTest2() ;
		runTest3() ;
		runTest4() ;
	}
}
