import java.lang.StringBuilder ;
import java.util.Stack ;

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
	
	/* --- CONSTRUCTION ---------------------------------------------------- */
	
	/**
	 * Initialisation
	 * 
	 * @param seq1
	 * @param seq2
	 */
	public SmithWaterman( String seq1 , String seq2 )
	{
		_seq1 = seq2 ;
		_seq2 = seq1 ;
		
		// i -> east-directed
		// j -> south-directed
		_matrix = new int[seq1.length()+1][seq2.length()+1] ;
		
		// init matrix
		for( int i = 0 ; i < _matrix.length ; i++ )
			for( int j = 0 ; j < _matrix[i].length ; j++ )
				_matrix[i][j] = 0 ;
		
		// score matrix
		for( int i = 1 ; i < _matrix.length ; i ++ )
			for( int j = 1 ; j < _matrix[i].length ; j++ )
				_matrix[i][j] = score(i,j) ;
	}
	
	
	/* --- SCORING --------------------------------------------------------- */
	
	// SCORE
	public int score( int i , int j )
	{
		char base1 = Character.toUpperCase( _seq1.charAt(i-1) ) ;
		char base2 = Character.toUpperCase( _seq2.charAt(j-1) ) ;
		
		int[] scores = new int[3] ;
		
		scores[0] = alignmentScore( i , j , base1 , base2 ) ;
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
	public int alignmentScore( int i , int j , char base1 , char base2 )
	{
		return _matrix[i-1][j-1] + similarity( base1 , base2 ) ;
	}
	
	public int similarity( char base1 , char base2 )
	{
		// match
		if( base1 == base2 )
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
	
	
	/* --- ALIGNMENT ------------------------------------------------------- */
	
	
	
	
	/* --- PRINT ----------------------------------------------------------- */
	
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
	
	
	/* --- FOR STACK ------------------------------------------------------- */
	
	private class Node
	{
		private static final int ALIGN = 0 ;
		private static final int INSERT = 1 ;
		private static final int DELETE = 2 ;
		
		private final char base1 , base2 ;
		private final int i , j ;
		private final int score ;
		private int alignType ;
		
		private Node( char base1 , char base2 , int i , int j , int score )
		{
			this.base1 = base1 ;
			this.base2 = base2 ;
			this.i = i ;
			this.j = j ;
			this.score = score ;
		}
		
		private void setAlignType( int alignType )
		{
			this.alignType = alignType ; 
		}
	}
	
	
	/* --- MAIN ------------------------------------------------------------ */
	
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
