package metrics ;

import java.util.ArrayList ;
import sw.* ;

/**
 * Prints out information about the dataset of reference sequences to standard out.
 * 
 * @author Elizabeth Fong
 * @version Insight Data Engineering NY, September-October 2015
 */
public class RefSetInfo 
{
	// entire directory
	private final static String REF_DIR = "/home/ubuntu/project/reference" ;
	private final static String DELIMITER = ">gi" ;
	
	
	/**
	 * Prints out information about the dataset of reference sequences to standard out.
	 * 
	 * @param args None expected.
	 */
	public static void main( String[] args )
	{
		// variables
		long numSeqs = 0 ;
		long totalBp = 0 ;
		
		double mean = 0 ;
		double median = 0 ;
		
		int numFiles = 0 ;
		
		RunningMedian runningMed = new RunningMedian() ;
		
		// read files
		DirectoryCrawler dir = new DirectoryCrawler( REF_DIR ) ;
		
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
		
		// printing
		System.out.println( "# files = " + numFiles ) ;
		System.out.println( "# sequences = " + numSeqs ) ;
		System.out.println( "# total base pairs = " + totalBp ) ;
		System.out.println( "" ) ;
		System.out.println( "mean # base pairs = " + mean ) ;
		System.out.println( "median # base pairs = " + median ) ;
	}
}
