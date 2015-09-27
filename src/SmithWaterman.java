

import org.apache.spark.SparkConf ;

import org.apache.spark.api.java.JavaRDD ;
import org.apache.spark.api.java.JavaSparkContext ;

import org.apache.spark.api.java.function.Function ;
import org.apache.spark.api.java.function.Function2 ;
import org.apache.spark.api.java.function.Function3 ;

import scala.Tuple2 ;
import scala.Tuple3 ;
import scala.Tuple5 ;


@SuppressWarnings( "serial" ) 
public class SmithWaterman 
{
	public static class GetCellScore 
			 implements Function3< Tuple3<Integer,Integer,Integer> ,
			 					   Tuple2<Character,Character> ,
			 					   Tuple5<Tuple2<Integer,Integer>,Integer,Character,Character,Character> , 
			 					   Tuple2<Integer,Character> > 
	{
		/**
		 * 
		 * 
		 * @param cellScores {NW,N,W}
		 * @param bases {RefBase,InputBase}
		 * @param alignInfo {{Match,Mismatch},Gap,align,insert,delete}
		 * 
		 * @return {score,alignType}
		 */
		public Tuple2<Integer,Character> call( Tuple3<Integer,Integer,Integer> cellScores ,
											   Tuple2<Character,Character> bases ,
											   Tuple5<Tuple2<Integer,Integer>,Integer,Character,Character,Character> alignInfo ) 
		{
			int max = 0 ;
			Character alignment = null ;
			
			// deletion score
			int tmp = new InsDelScore().call( cellScores._3() , alignInfo._2() ).intValue() ;
			if( tmp >= max )
			{
				max = tmp ;
				alignment = alignInfo._5() ;
			}
			
			// insertion score
			tmp = new InsDelScore().call( cellScores._2() , alignInfo._2() ).intValue() ;
			if( tmp >= max )
			{
				max = tmp ;
				alignment = alignInfo._4() ;
			}
			
			// alignment score
			tmp = new AlignmentScore().call( cellScores._1() , alignInfo._1() , bases ) ;
			if( tmp >= max )
			{
				max = tmp ;
				alignment = alignInfo._3() ;
			}
			
			return new Tuple2<Integer,Character>( new Integer(max) , alignment ) ;
		}
	}
	
	
	private static class InsDelScore implements Function2<Integer,Integer,Integer>
	{
		/**
		 * 
		 * 
		 * @param cellScore Cell scores of N (insertion) or W (deletion) cells
		 * @param gapScore
		 * 
		 * @return Score for insertion or deletion
		 */
		public Integer call( Integer cellScore , Integer gapScore )
		{	
			return new Integer( cellScore.intValue() + gapScore.intValue() ) ;
		}
	}
	
	private static class AlignmentScore implements Function3< Integer , 
															 Tuple2<Integer,Integer> ,
															 Tuple2<Character,Character> ,
															 Integer > 
	{
		/**
		 * 
		 * 
		 * @param nwCellScore Score of NW cell
		 * @param alignScores {Match,Mismatch}
		 * @param bases {refBase,inputBase}
		 * 
		 * @return Score for alignment (either match or mismatch)
		 */
		public Integer call( Integer nwCellScore , 
							 Tuple2<Integer,Integer> alignScores ,
							 Tuple2<Character,Character> bases )
		{
			char refBase = Character.toUpperCase( bases._1.charValue() ) ;
			char inputBase = Character.toUpperCase( bases._2.charValue() ) ;
			
			if( refBase == inputBase )
				return new Integer( nwCellScore.intValue() + alignScores._1.intValue() ) ;
			else
				return new Integer( nwCellScore.intValue() + alignScores._2.intValue() ) ;
		}
	}
	
	
	public static class TestClass
	{
		public TestClass()
		{
			System.out.println( "created an instance of inner class" ) ;
		}
	}
	
	public static void main( String[] args )
	{
		// constants
		final String FILE_REF = "/home/ubuntu/project/testRef/vertebrate_mammalian.415.rna.fna" ;
		final String FILE_IN = "/home/ubuntu/project/testIn/test1.fa" ;
		
		final int SCORE_MATCH = 5 ;
		final int SCORE_MISMATCH = -3 ;
		final int SCORE_GAP = -4 ;
		
		new SmithWaterman.TestClass() ;
	}
}
