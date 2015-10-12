package sw ;

import java.io.Serializable ;

import java.util.ArrayList ;
import java.util.Comparator ;

import org.apache.spark.SparkConf ;

import org.apache.spark.api.java.JavaPairRDD ;
import org.apache.spark.api.java.JavaRDD ;
import org.apache.spark.api.java.JavaSparkContext ;

import org.apache.spark.api.java.function.Function2 ;
import org.apache.spark.api.java.function.Function3 ;
import org.apache.spark.api.java.function.PairFunction ;

import scala.Tuple2 ;
import scala.Tuple3 ;


/**
 * <p>Runs the Smith-Waterman genetic alignment algorithm with different distribution methods. Written in functional Spark.<br />
 * Implemented methods: distribute algorithm, distribute reference dataset, no distribution <br />
 * To do: distribute reads
 * </p>
 * 
 * @author Elizabeth Fong
 * @version Insight Data Engineering NY, September-October 2015
 */
@SuppressWarnings( "serial" ) 
public class Distribution 
{
	// CONSTANTS
	private static final int[] ALIGN_SCORES = {5,-3,-4} ;	// {match,mismatch,gap}
	private static final char[] ALIGN_TYPES = {'a','i','d','-'} ;
	
	// IO DEFAULTS
	private static final String OUT_FILE = "result" ;
	private static final String OUT_EXT = ".txt" ;
	
	private static final String REF_DIR = "/home/ubuntu/project/reference" ;
	private static final String IN_DIR = "/home/ubuntu/project/input" ;
	
	private static final String DELIMITER = ">gi" ;
	
	private static final String OUT_DIR_ALGORITHM = "/home/ubuntu/project/output/algorithm" ;
	private static final String OUT_DIR_CONTROL = "/home/ubuntu/project/output/control" ;
	private static final String OUT_DIR_REFERENCE = "/home/ubuntu/project/output/reference" ;
	
	
	/* --- DISTRIBUTE ALGORITHM -------------------------------------------- */
	
	/**
	 * Finds the best-aligned reference to each of the input sequences in the input directory.
	 * This uses a distributed version of the Smith-Waterman genetic alignment algorithm.
	 * 
	 * @param ioArgs 	An array of {@link java.lang.String} elements dealing with io, where: 
	 * 			   		{@code ioArgs[0]} is the path to the directory with the reference sequences,
	 * 			   		{@code ioArgs[1]} is the path to the directory with the input sequences,
	 * 			   		{@code ioArgs[2]} is the delimiter to separate a reference sequence from the others,
	 * 			   		{@code ioArgs[3]} is the path to the directory where output files should be written to,
	 * 			   		{@code ioArgs[4]} is the general file name for the output files, and
	 * 			   		{@code ioArgs[5]} is the file extension for the output files.
	 * @param algoArgs 	A {@link scala.Tuple2} of elements that the algorithm requires, where:
	 * 					{@code algoArgs._1()} is an {@code int[]} of scores, in the order { match , mismatch , gap }, and
	 * 					{@code algoArgs._2()} is a {@code char[]} of alignment types, in the order { alignment , insertion , deletion , none }.
	 * 
	 * @return 			{@code null}
	 */
	public static class DistributeAlgorithm implements Function2< String[] , Tuple2<int[],char[]> , Boolean >
	{
		public Boolean call( String[] ioArgs , Tuple2<int[],char[]> algoArgs )
		{
			// PARAMETERS
			String _ref_dir = REF_DIR ;
			String _in_dir = IN_DIR ;
			
			String _delimiter = DELIMITER ;
			
			String _out_dir = OUT_DIR_ALGORITHM ;
			String _out_file_name = OUT_FILE ;
			String _out_file_ext = OUT_EXT ;
			
			int[] _align_scores = ALIGN_SCORES ;
			char[] _align_types = ALIGN_TYPES ;
			
			// not using default values - io arguments
			if( ioArgs != null && ioArgs.length == 6 )
			{
				if( ioArgs[0] != null )
					_ref_dir = ioArgs[0] ;
				if( ioArgs[1] != null )
					_in_dir = ioArgs[1] ;
				if( ioArgs[2] != null )
					_delimiter = ioArgs[2] ;
				if( ioArgs[3] != null )
					_out_dir = ioArgs[3] ;
				if( ioArgs[4] != null )
					_out_file_name = ioArgs[4] ;
				if( ioArgs[5] != null )
					_out_file_ext = ioArgs[5] ;
			}
			
			// not using default values - algorithm arguments
			if( algoArgs != null )
			{
				int[] arg1 = algoArgs._1() ;
				char[] arg2 = algoArgs._2() ;
				if( arg1 != null )
					_align_scores = arg1 ;
				if( arg2 != null )
					_align_types = arg2 ;
			}
			
			
			// VARIABLES
			DirectoryCrawler inDir = new DirectoryCrawler( _in_dir ) ;
			DirectoryCrawler refDir ;
			
			int inputNum = 0 ;
			
			
			// RUN!!
			while( inDir.hasNext() )
			{	
				inputNum ++ ;
				
				ArrayList<String> reads = new InOutOps.GetReads().call( inDir.next() , _delimiter ) ;
				
				int numReads = reads.size() ;
				int numRefs = 0 ;
				long execTime = System.currentTimeMillis() ;
				
				refDir = new DirectoryCrawler( _ref_dir ) ;
				
				
				// MAX - Bookkeeping
				int max = 0 ;
				ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> opt = new ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>>() ;
				
				
				// RUN!!
				while( refDir.hasNext() )
				{
					ArrayList<String[]> refSeqs = new InOutOps.GetRefSeqs().call( refDir.next() , _delimiter ) ;
					numRefs += refSeqs.size() ;
					
					// COMPARISON
					for( String[] ref : refSeqs )
					{
						int total = 0 ;
						ArrayList<Tuple2<Integer,String[]>> matchSites = new ArrayList<Tuple2<Integer,String[]>>() ;
						
						for( String read : reads )
						{
							// sw
							String[] seq = { ref[1] , read } ;
							Tuple2<Integer,ArrayList<Tuple2<Integer,String[]>>> result = new DistributedSW.OptAlignments().call( seq , _align_scores , _align_types ) ;
							
							// combine
							total += result._1().intValue() ;
							matchSites.addAll( result._2() ) ;
						}
						
						// get max ref
						if( total > max )
						{
							max = total ;
							
							opt.clear() ;
							matchSites.sort( new DistributedSW.MatchSiteComp() ) ; 
							opt.add( new Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>(ref,matchSites) ) ;
						}
						else if( total == max )
						{
							matchSites.sort( new DistributedSW.MatchSiteComp() ) ;
							opt.add( new Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>(ref,matchSites) ) ;
						}
						
					}
				}
				
				
				// print to file
				execTime = System.currentTimeMillis() - execTime ;
				opt.sort( new OptSeqsComp() ) ;
				
				int[] nums = { numRefs , numReads } ;
				Tuple3<int[],Integer,Long> tuple = new Tuple3<int[],Integer,Long>( nums , max , execTime ) ;
				
				String printStr = new InOutOps.GetOutputStr().call( reads , tuple , opt ) ;
				String filepath = _out_dir + "/" + _out_file_name + inputNum + _out_file_ext ;
				
				new InOutOps.PrintStrToFile().call( filepath , printStr ) ;
			}
			
			return null ;
		}
	}
	
	
	/* --- DISTRIBUTE REFERENCE -------------------------------------------- */
	
	/**
	 * Finds the best-aligned reference to each of the input sequences in the input directory.
	 * This distributes the dataset of reference sequences.
	 * 
	 * @param ioArgs 	An array of {@link java.lang.String} elements dealing with io, where: 
	 * 			   		{@code ioArgs[0]} is the path to the directory with the reference sequences,
	 * 			   		{@code ioArgs[1]} is the path to the directory with the input sequences,
	 * 			   		{@code ioArgs[2]} is the delimiter to separate a reference sequence from the others,
	 * 			   		{@code ioArgs[3]} is the path to the directory where output files should be written to,
	 * 			   		{@code ioArgs[4]} is the general file name for the output files, and
	 * 			   		{@code ioArgs[5]} is the file extension for the output files.
	 * @param algoArgs 	A {@link scala.Tuple2} of elements that the algorithm requires, where:
	 * 					{@code algoArgs._1()} is an {@code int[]} of scores, in the order { match , mismatch , gap }, and
	 * 					{@code algoArgs._2()} is a {@code char[]} of alignment types, in the order { alignment , insertion , deletion , none }.
	 * 
	 * @return 			{@code null}
	 */
	public static class DistributeReference implements Function2< String[] , Tuple2<int[],char[]> , Boolean >
	{
		public Boolean call( String[] ioArgs , Tuple2<int[],char[]> algoArgs )
		{
			// PARAMETERS
			String _ref_dir = REF_DIR ;
			String _in_dir = IN_DIR ;
			
			String _delimiter = DELIMITER ;
			
			String _out_dir = OUT_DIR_REFERENCE ;
			String _out_file_name = OUT_FILE ;
			String _out_file_ext = OUT_EXT ;
			
			int[] _align_scores = ALIGN_SCORES ;
			char[] _align_types = ALIGN_TYPES ;
			
			// not using default values - io arguments
			if( ioArgs != null && ioArgs.length == 6 )
			{
				if( ioArgs[0] != null )
					_ref_dir = ioArgs[0] ;
				if( ioArgs[1] != null )
					_in_dir = ioArgs[1] ;
				if( ioArgs[2] != null )
					_delimiter = ioArgs[2] ;
				if( ioArgs[3] != null )
					_out_dir = ioArgs[3] ;
				if( ioArgs[4] != null )
					_out_file_name = ioArgs[4] ;
				if( ioArgs[5] != null )
					_out_file_ext = ioArgs[5] ;
			}
			
			// not using default values - algorithm arguments
			if( algoArgs != null )
			{
				int[] arg1 = algoArgs._1() ;
				char[] arg2 = algoArgs._2() ;
				if( arg1 != null )
					_align_scores = arg1 ;
				if( arg2 != null )
					_align_types = arg2 ;
			}
			
			algoArgs = new Tuple2<int[],char[]>( _align_scores , _align_types ) ;
			
			
			// VARIABLES
			DirectoryCrawler inDir = new DirectoryCrawler( _in_dir ) ;
			DirectoryCrawler refDir ;
			
			int inputNum = 0 ;
			
			
			// for parallelisation - reuse RDDs as much as possible
			JavaSparkContext sc = new JavaSparkContext( new SparkConf() ) ;
			JavaRDD<Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>>> listRDD  = null ;
			JavaPairRDD<Integer,Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> mapRDD = null ;
			
			// RUN!!
			while( inDir.hasNext() )
			{	
				inputNum ++ ;
				
				ArrayList<String> reads = new InOutOps.GetReads().call( inDir.next() , _delimiter ) ;
				
				int numReads = reads.size() ;
				int numRefs = 0 ;
				long execTime = System.currentTimeMillis() ;
				
				refDir = new DirectoryCrawler( _ref_dir ) ;
				
				
				// MAX - Bookkeeping
				int max = 0 ;
				ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> opt = new ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>>() ;
				
				
				// RUN!!
				while( refDir.hasNext() )
				{
					ArrayList<String[]> refSeqs = new InOutOps.GetRefSeqs().call( refDir.next() , _delimiter ) ;
					numRefs += refSeqs.size() ;
					
					ArrayList<Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>>> list = new CombineReadsToRef().call( refSeqs , reads , algoArgs ) ;
					
					// parallelise and map
					listRDD = sc.parallelize( list ) ;
					mapRDD = listRDD.mapToPair( new MapRef() ) ;
					
					// 'reduce' - extract max
					mapRDD.sortByKey(false) ;
					int maxKey = mapRDD.first()._1() ;
					
					if( maxKey > max )
					{
						max = maxKey ;
						opt.clear() ;
						opt.addAll( mapRDD.lookup(new Integer(maxKey)) ) ;
					}
					else if( maxKey == max )
					{
						opt.addAll( mapRDD.lookup(new Integer(maxKey)) ) ;
					}
				}
				
							
				// print to file
				execTime = System.currentTimeMillis() - execTime ;
				opt.sort( new OptSeqsComp() ) ;
				
				int[] nums = { numRefs , numReads } ;
				Tuple3<int[],Integer,Long> tuple = new Tuple3<int[],Integer,Long>( nums , max , execTime ) ;
				
				String printStr = new InOutOps.GetOutputStr().call( reads , tuple , opt ) ;
				String filepath = _out_dir + "/" + _out_file_name + inputNum + _out_file_ext ;
				
				new InOutOps.PrintStrToFile().call( filepath , printStr ) ;
			}
			
			sc.close() ;
			return null ;
		}
	}
	
	/**
	 * This function is run when the dataset of reference sequences is mapped. 
	 * This uses the non-distributed version of the Smith-Waterman algorithm.
	 * 
	 * @param tuple	A {@link scala.Tuple3} of arguments, where:
	 * 				{@code tuple._1()} is an array of 2 {@link java.lang.String} elements representing a reference sequence, in the order { metadata , sequence },
	 * 				{@code tuple._2()} is an {@link java.util.ArrayList} of reads, extracted from an input file, and
	 * 				{@code tuple._3()} is a {@link scala.Tuple2} of arguments used by the algorithm, in the order { alignment scores , alignment types }.
	 * 
	 * @return 		A {@link scala.Tuple2} representing a key-value pair, where:
	 * 				the key is an {@link java.lang.Integer} representing the total score from aligning all reads, and
	 * 				the value is a {@link scala.Tuple2} of the reference sequence and how the reads align to the sequence.
	 */
	private static class MapRef implements PairFunction< Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>> , Integer , Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>> >
	{
		public Tuple2<Integer,Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> call( Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>> tuple )
		{
			// VARIABLES
			String[] ref = tuple._1() ;
			ArrayList<String> reads = tuple._2() ;
			
			int[] alignScores = tuple._3()._1() ;
			char[] alignTypes = tuple._3()._2() ;
			
			
			// total score
			int totalScore = 0 ;
			ArrayList<Tuple2<Integer,String[]>> matchSites = new ArrayList<Tuple2<Integer,String[]>>() ;
			
			
			// RUN ALGORITHM
			for( String read : reads )
			{
				String[] seqs = { ref[1] , read } ;
				Tuple2<Integer,ArrayList<Tuple2<Integer,String[]>>> result = new SmithWaterman.OptAlignments().call( seqs , alignScores , alignTypes ) ;
				
				totalScore += result._1().intValue() ;
				matchSites.addAll( result._2() ) ;
			}
			
			matchSites.sort( new MatchSiteComp() ) ; 
			
			
			// RETURN
			Integer key = new Integer(totalScore) ;
			Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>> value = new Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>( ref , matchSites ) ;
			
			return new Tuple2<Integer,Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>>( key , value ) ;
		}
	}
	
	
	/* --- DISTRIBUTE READS ------------------------------------------------ */
	
	/**
	 * TODO
	 * 
	 * Finds the best-aligned reference to each of the input sequences in the input directory.
	 * This distributes the reads of the input.
	 * 
	 * @param ioArgs 	An array of {@link java.lang.String} elements dealing with io, where: 
	 * 			   		{@code ioArgs[0]} is the path to the directory with the reference sequences,
	 * 			   		{@code ioArgs[1]} is the path to the directory with the input sequences,
	 * 			   		{@code ioArgs[2]} is the delimiter to separate a reference sequence from the others,
	 * 			   		{@code ioArgs[3]} is the path to the directory where output files should be written to,
	 * 			   		{@code ioArgs[4]} is the general file name for the output files, and
	 * 			   		{@code ioArgs[5]} is the file extension for the output files.
	 * @param algoArgs 	A {@link scala.Tuple2} of elements that the algorithm requires, where:
	 * 					{@code algoArgs._1()} is an {@code int[]} of scores, in the order { match , mismatch , gap }, and
	 * 					{@code algoArgs._2()} is a {@code char[]} of alignment types, in the order { alignment , insertion , deletion , none }.
	 * 
	 * @return 			{@code null}
	 */
	// less data skew, but need to remember all matching sites and scores
	// for all sequences, before finding max
	
	
	/* --- CONTROL - NO DISTRIBUTION --------------------------------------- */
	
	/**
	 * Control. 
	 * Finds the best-aligned reference to each of the input sequences in the input directory.
	 * Does not use distribution.
	 * 
	 * @param ioArgs 	An array of {@link java.lang.String} elements dealing with io, where: 
	 * 			   		{@code ioArgs[0]} is the path to the directory with the reference sequences,
	 * 			   		{@code ioArgs[1]} is the path to the directory with the input sequences,
	 * 			   		{@code ioArgs[2]} is the delimiter to separate a reference sequence from the others,
	 * 			   		{@code ioArgs[3]} is the path to the directory where output files should be written to,
	 * 			   		{@code ioArgs[4]} is the general file name for the output files, and
	 * 			   		{@code ioArgs[5]} is the file extension for the output files.
	 * @param algoArgs 	A {@link scala.Tuple2} of elements that the algorithm requires, where:
	 * 					{@code algoArgs._1()} is an {@code int[]} of scores, in the order { match , mismatch , gap }, and
	 * 					{@code algoArgs._2()} is a {@code char[]} of alignment types, in the order { alignment , insertion , deletion , none }.
	 * 
	 * @return 			{@code null}
	 */
	public static class NoDistribution implements Function2< String[] , Tuple2<int[],char[]> , Boolean >
	{
		public Boolean call( String[] ioArgs , Tuple2<int[],char[]> algoArgs )
		{
			// PARAMETERS
			String _ref_dir = REF_DIR ;
			String _in_dir = IN_DIR ;
			
			String _delimiter = DELIMITER ;
			
			String _out_dir = OUT_DIR_CONTROL ;
			String _out_file_name = OUT_FILE ;
			String _out_file_ext = OUT_EXT ;
			
			int[] _align_scores = ALIGN_SCORES ;
			char[] _align_types = ALIGN_TYPES ;
			
			// not using default values - io arguments
			if( ioArgs != null && ioArgs.length == 6 )
			{
				if( ioArgs[0] != null )
					_ref_dir = ioArgs[0] ;
				if( ioArgs[1] != null )
					_in_dir = ioArgs[1] ;
				if( ioArgs[2] != null )
					_delimiter = ioArgs[2] ;
				if( ioArgs[3] != null )
					_out_dir = ioArgs[3] ;
				if( ioArgs[4] != null )
					_out_file_name = ioArgs[4] ;
				if( ioArgs[5] != null )
					_out_file_ext = ioArgs[5] ;
			}
			
			// not using default values - algorithm arguments
			if( algoArgs != null )
			{
				int[] arg1 = algoArgs._1() ;
				char[] arg2 = algoArgs._2() ;
				if( arg1 != null )
					_align_scores = arg1 ;
				if( arg2 != null )
					_align_types = arg2 ;
			}
			
			
			// VARIABLES
			DirectoryCrawler inDir = new DirectoryCrawler( _in_dir ) ;
			DirectoryCrawler refDir ;
			
			int inputNum = 0 ;
			
			
			// RUN!!
			while( inDir.hasNext() )
			{	
				inputNum ++ ;
				
				ArrayList<String> reads = new InOutOps.GetReads().call( inDir.next() , _delimiter ) ;
				
				int numReads = reads.size() ;
				int numRefs = 0 ;
				long execTime = System.currentTimeMillis() ;
				
				refDir = new DirectoryCrawler( _ref_dir ) ;
				
				
				// MAX - Bookkeeping
				int max = 0 ;
				ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> opt = new ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>>() ;
				
				
				// RUN!!
				while( refDir.hasNext() )
				{
					ArrayList<String[]> refSeqs = new InOutOps.GetRefSeqs().call( refDir.next() , _delimiter ) ;
					numRefs += refSeqs.size() ;
					
					// COMPARISON
					for( String[] ref : refSeqs )
					{
						int total = 0 ;
						ArrayList<Tuple2<Integer,String[]>> matchSites = new ArrayList<Tuple2<Integer,String[]>>() ;
						
						for( String read : reads )
						{
							// sw
							String[] seq = { ref[1] , read } ;
							Tuple2<Integer,ArrayList<Tuple2<Integer,String[]>>> result = new SmithWaterman.OptAlignments().call( seq , _align_scores , _align_types ) ;
							
							// combine
							total += result._1().intValue() ;
							matchSites.addAll( result._2() ) ;
						}
						
						// get max ref
						if( total > max )
						{
							max = total ;
							
							opt.clear() ;
							matchSites.sort( new MatchSiteComp() ) ; 
							opt.add( new Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>(ref,matchSites) ) ;
						}
						else if( total == max )
						{
							matchSites.sort( new MatchSiteComp() ) ;
							opt.add( new Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>(ref,matchSites) ) ;
						}
						
					}
				}
				
				
				// print to file
				execTime = System.currentTimeMillis() - execTime ;
				opt.sort( new OptSeqsComp() ) ;
				
				int[] nums = { numRefs , numReads } ;
				Tuple3<int[],Integer,Long> tuple = new Tuple3<int[],Integer,Long>( nums , max , execTime ) ;
				
				String printStr = new InOutOps.GetOutputStr().call( reads , tuple , opt ) ;
				String filepath = _out_dir + "/" + _out_file_name + inputNum + _out_file_ext ;
				
				new InOutOps.PrintStrToFile().call( filepath , printStr ) ;
			}
			
			return null ;
		}
	}
	
	
	/* --- UTILITY --------------------------------------------------------- */
	
	/**
	 * A {@link java.util.Comparator} of optimal sequences.
	 * This orders elements in ascending order of the metadata of the reference sequences.
	 * 
	 * @see {@link java.util.Comparator#compare(Object, Object)}
	 */
	private static class OptSeqsComp implements Comparator<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> , Serializable
	{
		public int compare( Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>> t1 , Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>> t2 )
		{
			return t1._1()[0].compareTo( t2._1()[0] ) ;
		}
	}
	
	/**
	 * A {@link java.util.Comparator} of matching sites.
	 * This orders elements in ascending order of the indices of the alignment.
	 * 
	 * @see {@link java.util.Comparator#compare(Object, Object)}
	 */
	private static class MatchSiteComp implements Comparator<Tuple2<Integer,String[]>> , Serializable
	{
		public int compare( Tuple2<Integer,String[]> t1 , Tuple2<Integer,String[]> t2 )
		{
			return t1._1().intValue() - t2._1().intValue() ;
		}
	}
	
	/**
	 * A function used for combining arguments required for the map function when distributing reference sequences.
	 * 
	 * @param references	An array of {@String} elements which represent a reference sequence. Contains { metadata , sequence }.
	 * @param reads			An {@link java.util.ArrayList} of reads extracted from an input file.
	 * @param algoArgs		A {@link scala.Tuple2} of arguments required for the algorithm. Contains { alignment scores , alignment types}.
	 * 
	 * @return				An {@link java.util.ArrayList} where the reads and algorithm arguments are combined with each reference sequence.
	 */
	private static class CombineReadsToRef implements Function3< ArrayList<String[]> , ArrayList<String> , Tuple2<int[],char[]> , ArrayList<Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>>> >
	{
		public ArrayList<Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>>> call( ArrayList<String[]> references , ArrayList<String> reads , Tuple2<int[],char[]> algoArgs )
		{
			ArrayList<Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>>> list = new ArrayList<Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>>>( references.size() ) ;
			
			for( String[] ref : references )
			{
				list.add( new Tuple3<String[],ArrayList<String>,Tuple2<int[],char[]>>(ref,reads,algoArgs) ) ;
			}
			
			return list ;
		}
	}
}
