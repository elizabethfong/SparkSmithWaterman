import java.lang.StringBuilder ;

/**
 * Non-parallel Smith-Waterman
 * 
 * @author Elizabeth Fong
 * @version September 2015, Insight Data Engineering Program NY
 */
public class SmithWaterman
{
	public static final int MATCH = 2 ;
	public static final int MISMATCH = -1 ;
	public static final int GAP = -1 ;
	
	private final String _seq1 , _seq2 ;
	
	private int[][] _matrix ;
	
	/**
	 * Initialisation
	 * 
	 * @param seq1
	 * @param seq2
	 */
	public SmithWaterman( String seq1 , String seq2 )
	{
		_seq1 = seq1 ;
		_seq2 = seq2 ;
		
		// i -> east-directed
		// j -> south-directed
		_matrix = new int[seq2.length()+1][seq1.length()+1] ;
		
		// init matrix
		for( int i = 0 ; i < _matrix.length ; i++ )
			for( int j = 0 ; j < _matrix[i].length ; j++ )
				_matrix[i][j] = 0 ;
		
		// score matrix
		for( int i = 1 ; i < _matrix.length ; i ++ )
			for( int j = 1 ; j < _matrix[i].length ; j++ )
				_matrix[i][j] = score(i,j) ;
	}
	
	public int score( int i , int j )
	{
		char char1 = Character.toUpperCase( _seq2.charAt(i-1) ) ;
		char char2 = Character.toUpperCase( _seq1.charAt(j-1) ) ;
		
		int[] scores = new int[3] ;
		
		scores[0] = alignmentScore( i , j , char1 , char2 ) ;
		scores[1] = insertionScore(i,j) ;
		scores[2] = deletionScore(i,j) ;
		
		int max = 0 ;
		
		for( int x = 0 ; x < scores.length ; x++ )
		{
			int score = scores[x] ;
			
			if( score > max )
				max = score ;
		}
		
		return max ;
	}
	
	// score: alignment -> NW
	public int alignmentScore( int i , int j , char char1 , char char2 )
	{
		return _matrix[i-1][j-1] + similarity( char1 , char2 ) ;
	}
	
	public int similarity( char char1 , char char2 )
	{
		// match
		if( char1 == char2 )
			return MATCH ;
		// mismatch
		else
			return MISMATCH ;
	}
	
	// score: insertion -> north
	public int insertionScore( int i , int j )
	{
		return _matrix[i][j-1] + GAP ;
	}
	
	// score: deletion -> west
	public int deletionScore( int i , int j )
	{
		return _matrix[i-1][j] + GAP ;
	}
	
	// PRINT MATRIX
	public String printMatrix()
	{
		StringBuilder str = new StringBuilder() ;
		
		for( int i = 0 ; i < _matrix.length ; i++ )
		{
			for( int j = 0 ; j < _matrix[i].length ; j++ )
				str.append( _matrix[i][j] + " " ) ;
			
			str.append( System.lineSeparator() ) ;
		}
		
		return str.toString() ;
	}
	
	// MAIN
	public static void main( String[] args )
	{
		//String str1 = "ACACACTA" ;
		//String str2 = "AGCACACA" ;
		String str1 = "ATGCA" ;
		String str2 = "ACTCA" ;
		
		System.out.println( "Str1 = " + str1 ) ;
		System.out.println( "Str2 = " + str2 ) ;
		
		SmithWaterman sw = new SmithWaterman(str1,str2) ;
		
		System.out.println( "" ) ;
		System.out.println( sw.printMatrix() ) ;
	}
}
