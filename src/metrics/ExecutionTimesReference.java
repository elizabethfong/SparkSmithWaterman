package metrics ;

import sw.Distribution ;


public class ExecutionTimesReference 
{	
	// change num reads 
	private static final String REF1 = "/home/ubuntu/project/testRef/in" ;
	private static final String IN1 = "/home/ubuntu/project/input/readNum" ;
	private static final String OUT1 = "/home/ubuntu/project/output/reference/readNum" ;
	
	// change read lengths
	private static final String REF2 = "/home/ubuntu/project/testRef/in" ;
	private static final String IN2 = "/home/ubuntu/project/input/readLen" ;
	private static final String OUT2 = "/home/ubuntu/project/output/reference/readLen" ;
	
	// change num references
	private static final String REF3 = "/home/ubuntu/project/testRef/refNum" ;
	private static final String IN3 = "/home/ubuntu/project/input/ref" ;
	private static final String OUT3 = "/home/ubuntu/project/output/reference/refNum" ;
	
	// change reference lengths
	private static final String REF4 = "/home/ubuntu/project/testRef/refLen" ;
	private static final String IN4 = "/home/ubuntu/project/input/ref" ;
	private static final String OUT4 = "/home/ubuntu/project/output/reference/refLen" ;
	
	
	/* --- TESTS ----------------------------------------------------------- */
	
	// change num reads
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
	
	// change read lengths
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
	
	// change ref num
	public static void runTest3()
	{
		String refDir = REF3 ;
		String inDir = IN3 ;
		String delimiter = null ;
		String outDir = OUT3 ;
		String outFileName = "result" ;
		String outFileExt = null ;
		
		int numDirs = 28 ;
		
		for( int i = 1 ; i <= numDirs ; i++ )
		{
			String[] args = { refDir+i , inDir , delimiter , outDir , outFileName+i+"_" , outFileExt } ;
			new Distribution.DistributeReference().call( args , null ) ;
		}
	}
	
	// change ref length
	public static void runTest4()
	{
		String refDir = REF4 ;
		String inDir = IN4 ;
		String delimiter = null ;
		String outDir = OUT4 ;
		String outFileName = null ;
		String outFileExt = null ;
		
		int numDirs = 36 ;
		
		for( int i = 1 ; i <= numDirs ; i++ )
		{
			String[] args = { refDir+i , inDir , delimiter , outDir , outFileName+i+"_" , outFileExt } ;
			new Distribution.DistributeReference().call( args , null ) ;
		}
	}
	
	
	/* --- MAIN ------------------------------------------------------------ */
	
	public static void main( String[] args )
	{	
		runTest1() ;
		runTest2() ;
		runTest3() ;
		runTest4() ;
	}
}
