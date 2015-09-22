package processing ;

// spark default imports
import org.apache.spark.api.java.JavaSparkContext ;		// entry pt for spark
import org.apache.spark.api.java.JavaRDD ;		// resilient distributed database
import org.apache.spark.SparkConf ;		// spark application config

// spark functions
import org.apache.spark.api.java.function.Function ;
import org.apache.spark.api.java.function.Function2 ;
import org.apache.spark.api.java.function.VoidFunction ;

// aws s3
import com.amazonaws.services.s3.AmazonS3Client ;
import com.amazonaws.services.s3.model.S3Object ;

// java imports
//import java.lang.Iterable ;
//import java.util.ArrayList ; 
//import java.util.Scanner ;


/**
 * For accessing the 1000genomes database and doing sth with it
 * First time trying to write functional Spark!
 * 
 * @author Elizabeth Fong
 */
public class DatabaseUseTest 
{
	public static final String NEWLINE = System.lineSeparator() ;
	
	public static final String APP_NAME = "DatabaseUseTest" ;
	public static final String BUCKET = "ncbi-refseq-rna" ;
	
	
	// so apparently everything happens in the main...
	@SuppressWarnings("serial")
	public static void main( String[] args )
	{
		/*
		// set spark application configurations
		SparkConf config = new SparkConf() ;
		config.setAppName( APP_NAME ) ;
		
		// entry pt for Spark - only 1 may exist
		JavaSparkContext sc = new JavaSparkContext( config ) ;
		*/
		
		// amazon s3 client to read s3 bucket
		AmazonS3Client s3Client = new AmazonS3Client() ;
		S3Object obj = s3Client.getObject( BUCKET , "" ) ;
		
		System.out.println( NEWLINE + NEWLINE ) ;
		System.out.println( "Bucket = " + obj.getBucketName() ) ;
		System.out.println( "Key = " + obj.getKey() ) ;
		System.out.println( NEWLINE + NEWLINE ) ;
		
		//sc.close() ;
	}
	
}
