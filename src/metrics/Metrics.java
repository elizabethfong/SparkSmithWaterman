package metrics ;

import java.util.ArrayList ;
import sw.* ;

public class Metrics 
{
	// FILE VARIABLES
	public final static String REF_240 = "/home/ubuntu/project/testRef/vertebrate_mammalian.415.rna.fna" ;
	public final static String REF_121 = "/home/ubuntu/project/testRef/vertebrate_mammalian.432.rna.fna" ;
	public final static String REF_85 = "/home/ubuntu/project/testRef/vertebrate_mammalian.107.rna.fna" ;
	public final static String REF_70 = "/home/ubuntu/project/testRef/vertebrate_mammalian.123.rna.fna" ;
	public final static String REF_50 = "/home/ubuntu/project/testRef/vertebrate_mammalian.436.rna.fna" ;
	public final static String REF_30K = "/home/ubuntu/project/testRef/vertebrate_mammalian.129.rna.fna" ;
	
	public final static String IN_240 = "/home/ubuntu/project/testIn/test1.fa" ;
	public final static String IN_121 = "/home/ubuntu/project/testIn/test2.fa" ;
	public final static String IN_85 = "/home/ubuntu/project/testIn/test5.fa" ;
	public final static String IN_70 = "/home/ubuntu/project/testIn/test4.fa" ;
	public final static String IN_50 = "/home/ubuntu/project/testIn/test3.fa" ;
	public final static String IN_30K = "/home/ubuntu/project/testIn/test6.fa" ;
	
	// entire directory
	public final static String REF_DIR = "C:/Users/Elizabeth/Desktop/reference" ;
	
	final static String DELIMITER = ">gi" ;
	
	public static void main( String[] args )
	{
		// variables
		long numSeqs = 0 ;
		long totalBp = 0 ;
		
		double mean = 0 ;
		double median = 0 ;
		
		int numFiles = 0 ;
		
		DirectoryCrawler dir = new DirectoryCrawler( REF_DIR ) ;
		RunningMedian runningMed = new RunningMedian() ;
		
		while( dir.hasNext() )
		{
			ArrayList<String[]> refs = new InOutOps.GetRefSeqs().call( dir.next() , DELIMITER ) ;
			numSeqs += refs.size() ;
			numFiles ++ ;
			
			for( String[] ref : refs )
			{
				int bp = ref[1].length() ;
				totalBp += bp ;
				runningMed.add( bp ) ;
			}
			
			System.out.println( "	# sequences = " + numSeqs + " , # total base pairs = " + totalBp ) ;
		}
		
		mean = ( (double) totalBp ) / numSeqs ;
		median = runningMed.getRunningMedian() ;
		
		System.out.println( "# files = " + numFiles ) ;
		System.out.println( "# sequences = " + numSeqs ) ;
		System.out.println( "# total base pairs = " + totalBp ) ;
		System.out.println( "" ) ;
		System.out.println( "mean # base pairs = " + mean ) ;
		System.out.println( "median # base pairs = " + median ) ;
	}
}
