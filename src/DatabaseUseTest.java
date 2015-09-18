// spark default imports
import org.apache.spark.api.java.JavaSparkContext ;		// entry pt for spark
import org.apache.spark.api.java.JavaRDD ;		// resilient distributed database
import org.apache.spark.SparkConf ;		// spark application config

// spark functions
import org.apache.spark.api.java.function.Function ;
import org.apache.spark.api.java.function.Function2 ;

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
	public static final String DATABASE = "s3n://s3.amazonaws.com/1000genomes" ;
	
	
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
		JavaRDD<String> database = sc.textFile( DATABASE ) ;
		
		System.out.println( NEWLINE + NEWLINE + NEWLINE + 
							"This is the id of the object: " + database + 
							NEWLINE + NEWLINE + NEWLINE ) ;
		
		// map each line of the database index...let's try it
		JavaRDD<String> indexFiles = database.map
		( 
			// this returns 0 + output records from each input... interesting
			new Function<String,String>()
			{
				public String call( String str )
				{
					return NEWLINE + NEWLINE + str + NEWLINE + NEWLINE ;
				}
			}
		) ;
		
		String result = indexFiles.reduce
		( 
			new Function2<String,String,String>()
			{
				public String call( String str1 , String str2 )
				{
					return str1 + str2 ;
				}
			}
		) ;
		
		System.out.println( result ) ;
		
		sc.close() ;
	}
	
}
