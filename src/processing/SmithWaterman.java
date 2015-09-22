package processing ;


import java.lang.StringBuilder ;

import java.util.ArrayList ;
import java.util.Stack ;

/**
 * Smith-Waterman OOP version
 * 
 * @author Elizabeth Fong
 * @version September 2015, Insight Data Engineering NY
 */
public class SmithWaterman 
{
	// Sequence 1 -> genome, Sequence 2 -> the read
	private final String _seq1 , _seq2 ;
	
	// Matrix for finding optimal alignment
	private Proclet[][] _matrix ;
	
	// Optimal alignment(s) with highest alignment score
	private Alignment[] _optAlignments ;
	
	
	/* --- CONSTRUCTION ---------------------------------------------------- */
	
	/**
	 * Constructor: Runs the S-W algorithm to find optimal alignment(s)
	 * 
	 * @param seq1 The gene/sequence the read is compared to
	 * @param seq2 The read
	 */
	public SmithWaterman( String seq1 , String seq2 )
	{
		_seq1 = seq2 ;
		_seq2 = seq1 ;
		
		_matrix = new Proclet[_seq1.length()+1][_seq2.length()+1] ;
		
		// init top and left edges with 0
		for( int i = 0 ; i < _matrix.length ; i++ )
			_matrix[i][0] = new Proclet() ;
		
		for( int j = 1 ; j < _matrix[0].length ; j++)
			_matrix[0][j] = new Proclet() ;
		
		
		// scoring - returns starting points for finding optimal sequences
		Stack<Proclet> proclets = scoring() ;
		
		
		// get optimal alignment(s)
		ArrayList<Alignment> alignments = new ArrayList<Alignment>(proclets.size()) ;
		
		int maxScore = 0 ;
		
		while( ! proclets.isEmpty() )
		{
			Alignment align = getAlignment( proclets.pop() ) ;
			
			if( align.score > maxScore )
			{
				alignments.clear() ;
				alignments.add( align ) ;
				
				maxScore = align.score ;
			}
			else if( align.score == maxScore )
			{
				alignments.add( align ) ;
			}
		}
		
		Alignment[] array = new Alignment[alignments.size()] ;
		_optAlignments = alignments.toArray( array ) ;
	}
	
	
	/* --- MATRIX SCORING -------------------------------------------------- */
	
	/**
	 * Fills the matrix of proclets with scores for each alignment
	 * 
	 * @return A {@code Stack} of proclets representing the end of each
	 * 		   potential optimal alignment.
	 */
	private Stack<Proclet> scoring()
	{
		// init alignment
		Stack<Proclet> maxProclets = new Stack<Proclet>() ;
		maxProclets.push( _matrix[0][0] ) ;
		
		int max = 0 ;
		
		// score rest of matrix
		for( int i = 1 ; i < _matrix.length ; i++ )
		{
			for( int j = 1 ; j < _matrix[i].length ; j++ )
			{
				Proclet proclet = new Proclet(i,j) ;
				
				// proclet score > max score - new max score
				if( proclet.score > max )
				{
					maxProclets = new Stack<Proclet>() ;
					maxProclets.push( proclet ) ;
					
					max = proclet.score ;
				}
				
				// proclet score = max score -> new start pt for opt sequence
				else if( proclet.score == max )
				{
					maxProclets.push( proclet ) ;
				}
				
				_matrix[i][j] = proclet ;
			}
		}
		
		return maxProclets ;
	}
	
	
	/* --- OPTIMAL ALIGNMENT ----------------------------------------------- */
	
	/**
	 * Returns the alignment of the sequences ending with the given proclet.
	 * 
	 * @param proc The last proclet of the alignment.
	 * 
	 * @return The alignment of the sequences ending with the given proclet.
	 */
	private Alignment getAlignment( Proclet proc )
	{
		// find optimal alignment - push proclets of highest score onto stack
		Stack<Proclet> proclets = new Stack<Proclet>() ;
		proclets.push( proc ) ;
		
		Proclet next = getNextProclet( proc ) ;
		
		while( next.score > 0 )
		{
			proclets.push( next ) ;
			next = getNextProclet( next ) ;
		}
		
		
		// pop stack to get optimal alignment
		StringBuilder seq1 = new StringBuilder() ;
		StringBuilder seq2 = new StringBuilder() ;
		
		int alignScore = 0 ;
		
		while( ! proclets.isEmpty() )
		{
			Proclet tmp = proclets.pop() ;
			
			alignScore += tmp.alignmentScore ;
			
			seq1.append( tmp.base1 ) ;
			seq2.append( tmp.base2 ) ;
		}
		
		return new Alignment( seq1.toString() , seq2.toString() , alignScore ) ;
	}
	
	/**
	 * Returns the next proclet when reverse-engineering the alignment.
	 * 
	 * @param proc The current proclet.
	 * 
	 * @return The next proclet.
	 */
	private Proclet getNextProclet( Proclet proc )
	{
		char alignment = proc.alignmentType ;
		
		int i = proc.i ;
		int j = proc.j ;
		
		switch( alignment )
		{
			case Proclet.ALIGN :
				return _matrix[i-1][j-1] ;
			
			case Proclet.INSERTION :
				return _matrix[i][j-1] ;
			
			default :
				return _matrix[i-1][j] ;
		}
	}
	
	
	/* --- OUTPUT ---------------------------------------------------------- */
	
	/**
	 * Returns a {@code String} representation of the sequence alignment,
	 * showing the scoring matrix and all optimal alignments and their
	 * alignment scores.
	 * 
	 * @return The {@code String} representation of this sequence alignment.
	 */
	@Override
	public String toString()
	{
		String newline = System.lineSeparator() ;
		
		// print matrix
		StringBuilder str = new StringBuilder() ;
		str.append( "   _  " ) ;
		
		for( int i = 0 ; i < _seq2.length() ; i++ )
		{
			str.append( Character.toUpperCase(_seq2.charAt(i)) + "  " ) ;
		}
		
		str.append( newline + newline ) ;
		
		for( int i = 0 ; i < _matrix.length ; i++ )
		{
			if( i == 0 )
				str.append( "_  " ) ;
			else
				str.append( Character.toUpperCase(_seq1.charAt(i-1)) + "  " ) ;
			
			for( int j = 0 ; j < _matrix[i].length ; j++ )
			{
				int score = _matrix[i][j].score ;
				
				if( score < 10 )
					str.append( score + "  " ) ;
				else
					str.append( score + " " ) ;
			}
			
			str.append( newline + newline ) ;
		}
		
		// optimal alignment(s)
		for( int i = 0 ; i < _optAlignments.length ; i++ )
		{
			Alignment align = _optAlignments[i] ;
			
			str.append( "total score = " + align.score + newline ) ;
			str.append( align.seq2 + newline ) ;
			str.append( align.seq1 + newline ) ;
			str.append( newline ) ;
		}
		
		return str.toString() ;
	}
	
	
	/* --- INNER CLASSES --------------------------------------------------- */
	
	/**
	 * An entry in the scoring matrix.
	 * Calculates and stores the alignment of the 2 base pairs compared and 
	 * their alignment score and type (match/mismatch/insertion/deletion).
	 * 
	 * @author Elizabeth Fong
	 */
	private class Proclet
	{
		// Constants: scoring -> match, mismatch and gap.
		private static final int MATCH = 5 ;
		private static final int MISMATCH = -3 ;
		private static final int GAP = -4 ;
		
		// Constants: alignment type
		private static final char ALIGN = 'a' ;
		private static final char INSERTION = 'i' ;
		private static final char DELETION = 'd' ;
		
		// Constant: display character for a gap (either from insertion or deletion)
		private static final char GAP_CHAR = '_' ;
		
		// i and j indices for matrix position.
		private final int i , j ;
		
		// proclet/matrix score for the alignment of these 2 base pairs
		private int score ;
		
		// The base pairs in this alignment. May contain a gap from insertion/deletion
		private char base1 , base2 ;
		
		// alignment type and score for the alignment of these 2 base pairs
		private char alignmentType ;
		private int alignmentScore ;
		
		
		/**
		 * A 0-proclet, aligning a base pair and a gap. Score = 0 ;
		 */
		private Proclet()
		{
			i = 0 ;
			j = 0 ;
			score = 0 ;
		}
		
		/**
		 * A proclet at index i and j of the matrix, aligning 2 base pairs.
		 * 
		 * @param i Index of sequence 1
		 * @param j Index of sequence 2
		 */
		private Proclet( int i , int j )
		{
			this.i = i ;
			this.j = j ;
			
			base1 = Character.toUpperCase(_seq1.charAt(i-1)) ;
			base2 = Character.toUpperCase(_seq2.charAt(j-1)) ;
			
			// get score and alignment
			score = procletScore() ;
			getAlignment() ;
		}
		
		
		/**
		 * Returns the proclet/matrix score for this proclet.
		 * score = max( 0 , alignment , insertion , deletion )
		 * 
		 * @return The proclet/matrix score for this proclet.
		 */
		private int procletScore()
		{
			int max = 0 ;
			
			// deletion score
			int tmp = deletionScore() ;
			if( tmp >= max )
			{
				max = tmp ;
				alignmentType = DELETION ;
			}
			
			// insertion score
			tmp = insertionScore() ;
			if( tmp >= max )
			{
				max = tmp ;
				alignmentType = INSERTION ;
			}
			
			// alignment score
			tmp = alignmentScore() ;
			if( tmp >= max )
			{
				max = tmp ;
				alignmentType = ALIGN ;
			}
			
			return max ;
		}
		
		/**
		 * Returns the proclet/matrix score for an alignment (match/mismatch).
		 * alignment -> from NW
		 * 
		 * @return The proclet/matrix score for an alignment (match/mismatch).
		 */
		private int alignmentScore()
		{
			int similarity = 0 ;
			
			// similarity score -> match or mismatch
			if( base1 == base2 )
				similarity = MATCH ;
			else
				similarity = MISMATCH ;
			
			return _matrix[i-1][j-1].score + similarity ;
		}
		
		/**
		 * Returns the proclet/matrix score for an insertion.
		 * insertion -> from north
		 * 
		 * @return The proclet/matrix score for an insertion.
		 */
		private int insertionScore()
		{
			return _matrix[i][j-1].score + GAP ;
		}
		
		/**
		 * Returns the proclet/matrix score for a deletion.
		 * deletion -> from west
		 * 
		 * @return The proclet/matrix score for a deletion.
		 */
		private int deletionScore()
		{
			return _matrix[i-1][j].score + GAP ;
		}
		
		/**
		 * From the alignment type, determine which bases are aligned to
		 * each other in this set of base pairs.
		 */
		private void getAlignment()
		{
			switch( alignmentType )
			{
				// alignment: match/mismatch
				case ALIGN :
					if( base1 == base2 )
						alignmentScore = MATCH ;
					else
						alignmentScore = MISMATCH ;
					break ;
				
				// insertion -> insertion into read, thus gap in gene
				case INSERTION :
					alignmentScore = GAP ;
					base1 = GAP_CHAR ;
					break ;
				
				// deletion -> deletion from read, thus gap in read
				case DELETION :
					alignmentScore = GAP ;
					base2 = GAP_CHAR ;
					break ;
			}
		}
	}
	
	
	/**
	 * A wrapper class wrapping the 2 seqences in their alignment with the
	 * alignment score.
	 * 
	 * @author Elizabeth Fong
	 */
	private class Alignment
	{
		// seq1 -> the part of the gene in the alignment
		// seq2 -> the part of the read in the alignment
		private String seq1 , seq2 ;
		
		// the alignment score
		private int score ;
		
		/**
		 * Constructor.
		 * 
		 * @param seq1 The part of the gene in the alignment
		 * @param seq2 The part of the read in the alignment
		 * @param alignmentScore The alignment score
		 */
		private Alignment( String seq1 , String seq2 , int alignmentScore )
		{
			this.seq1 = seq1 ;
			this.seq2 = seq2 ;
			score = alignmentScore ;
		}
	}
	
	
	/* --- MAIN ------------------------------------------------------------ */
	
	
	/**
	 * Main. Runs the Smith-Waterman algorithm with 2 sequences for comparison.
	 * 
	 * @param args None expected.
	 */
	public static void main( String[] args )
	{
		//String str1 = "ACACACTA" ;
		//String str2 = "AGCACACA" ;
		//String str1 = "ATGCA" ;
		//String str2 = "ACTCA" ;
		String str1 = "CGTGAATTCAT" ;
		String str2 = "GACTTAC" ;
		
		System.out.println( "Str1 = " + str1 ) ;
		System.out.println( "Str2 = " + str2 ) ;
		
		SmithWaterman sw = new SmithWaterman(str1,str2) ;
		
		System.out.println( "" ) ;
		System.out.println( sw.toString() ) ;
	}
}
