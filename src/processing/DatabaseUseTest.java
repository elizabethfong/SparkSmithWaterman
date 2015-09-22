package processing ;

// spark default imports
import org.apache.spark.api.java.JavaSparkContext ;		// entry pt for spark
import org.apache.spark.api.java.JavaRDD ;		// resilient distributed database
import org.apache.spark.SparkConf ;		// spark application config

// spark functions
import org.apache.spark.api.java.function.Function ;
import org.apache.spark.api.java.function.Function2 ;
import org.apache.spark.api.java.function.VoidFunction ;

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
	public static final String BUCKET = "s3n://1000genomes/" ;
	
	
	// so apparently everything happens in the main...
	@SuppressWarnings("serial")
	public static void main( String[] args )
	{
		// set spark application configurations
		SparkConf config = new SparkConf() ;
		config.setAppName( APP_NAME ) ;
		
		// entry pt for Spark - only 1 may exist
		JavaSparkContext sc = new JavaSparkContext( config ) ;
		
		// get rdd from s3
		JavaRDD<String> bucket = sc.textFile( BUCKET ) ;
		
		System.out.println( NEWLINE + NEWLINE + NEWLINE + 
							"This is the id of the object: " + bucket.toString() + 
							NEWLINE + NEWLINE + NEWLINE ) ;
		
		
		
		sc.close() ;
	}
	
}
