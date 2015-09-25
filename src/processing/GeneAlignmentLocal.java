package processing ;

import java.util.ArrayList ;

public class GeneAlignmentLocal 
{
	//private static final String DIR_FASTA = "C:/Users/Elizabeth/Desktop/fasta" ;
	//private static final String DIR_REF = "C:/Users/Elizabeth/Desktop/ncbi-refseq-rna" ;
	private static final String DIR_FASTA = "/home/ubuntu/project/testIn" ;
	private static final String DIR_REF = "/home/ubuntu/project/testRef" ;
	
	private static final String DELIMITER = ">gi" ;
	
	public GeneAlignmentLocal()
	{
		long startTime = System.currentTimeMillis() ;
		
		DirectoryCrawler inputDir = new DirectoryCrawler(DIR_FASTA) ;
		DirectoryCrawler refDir = new DirectoryCrawler(DIR_REF) ;
		
		if( inputDir.hasNext() && refDir.hasNext() )
		{
			ArrayList<String> reads = new Fasta( inputDir.next() ).getReads() ;
			RefSeqParser parser = new RefSeqParser( refDir.next() , DELIMITER ) ;
			
			int max = 0 ;
			ArrayList<RefSeq> opt = new ArrayList<RefSeq>() ;
			
			while( parser.hasNext() )
			{
				RefSeq ref = parser.next() ;
				int score = align( ref.getSequence() , reads ) ;
				
				if( score > max )
				{
					max = score ;
					opt.clear() ;
					opt.add(ref) ;
				}
				else if( score == max )
				{
					opt.add(ref) ;
				}
			}
			
			// output
			System.out.println( "Max Score = " + max ) ;
			
			for( RefSeq r : opt )
			{
				System.out.println( r.getMetadata() ) ;
			}
		}
		
		long endTime = System.currentTimeMillis() ;
		long duration = endTime - startTime ;
		System.out.println( "Total Execution Time = " + duration + "ms" ) ;
	}
	
	private int align( String ref , ArrayList<String> reads )
	{
		int total = 0 ;
		
		for( String read : reads )
		{
			total += new SmithWaterman( ref , read ).alignmentScore() ;
		}
		
		return total ;
	}
	
	public static void main( String[] args )
	{
		new GeneAlignmentLocal() ;
	}
}
