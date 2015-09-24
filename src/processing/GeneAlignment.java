package processing ;

//spark default imports
import org.apache.spark.api.java.JavaSparkContext ;		// entry pt for spark
import org.apache.spark.api.java.JavaRDD ;		// resilient distributed database
import org.apache.spark.SparkConf ;		// spark application config

public class GeneAlignment
{
	public static final String NEWLINE = System.lineSeparator() ;
	
	public static final String APP_NAME = "GeneAlignment" ;
	
	public static final String DIR_FASTA = "fasta" ;
	public static final String DIR_DATASET = "ncbi-refseq-rna" ;
	
	public static void main( String[] args )
	{
		// set spark application configurations
		SparkConf config = new SparkConf() ;
		config.setAppName( APP_NAME ) ;
		
		// entry pt for Spark - only 1 may exist
		JavaSparkContext sc = new JavaSparkContext( config ) ;
		
		
		// read fasta input!
		System.out.println( NEWLINE + NEWLINE ) ;
		
		InputReader input = new InputReader( DIR_FASTA ) ;
		
		while( input.hasNextFile() )
		{
			input.nextFile() ;
			
			while( input.hasNextRead() )
			{
				System.out.println( input.nextRead() ) ;
			}
			
			System.out.println( "" ) ;
		}
		
		System.out.println( NEWLINE + NEWLINE ) ;
		
		
		/*
		String str1 = "CGTGAATTCAT" ;
		String str2 = "GACTTAC" ;
		
		System.out.println( "Str1 = " + str1 ) ;
		System.out.println( "Str2 = " + str2 ) ;
		
		SmithWaterman sw = new SmithWaterman(str1,str2) ;
		
		System.out.println( "" ) ;
		System.out.println( sw.toString() ) ;
		*/
		
		sc.close() ;
	}
}
