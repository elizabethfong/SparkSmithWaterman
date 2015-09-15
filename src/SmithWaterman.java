import java.lang.StringBuilder ;

import java.util.ArrayList ;
import java.util.Stack ;


public class SmithWaterman 
{
	private final String _seq1 , _seq2 ;
	
	private Proclet[][] _matrix ;
	
	private Alignment[] _optAlignments ;
	
	
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
	
	private class Proclet
	{
		private static final int MATCH = 5 ;
		private static final int MISMATCH = -3 ;
		private static final int GAP = -4 ;
		
		private static final char ALIGN = 'a' ;
		private static final char INSERTION = 'i' ;
		private static final char DELETION = 'd' ;
		
		private static final char GAP_CHAR = '_' ;
		
		private final int i , j ;
		
		private int score ;
		
		private char base1 , base2 ;
		
		private char alignmentType ;
		private int alignmentScore ;
		
		private Proclet()
		{
			i = 0 ;
			j = 0 ;
			score = 0 ;
		}
		
		private Proclet( int i , int j )
		{
			this.i = i ;
			this.j = j ;
			
			base1 = Character.toUpperCase(_seq1.charAt(i-1)) ;
			base2 = Character.toUpperCase(_seq2.charAt(j-1)) ;
			
			score = procletScore() ;
			getAlignment() ;
		}
		
		private int procletScore()
		{
			int max = 0 ;
			
			int tmp = deletionScore() ;
			if( tmp >= max )
			{
				max = tmp ;
				alignmentType = DELETION ;
			}
			
			tmp = insertionScore() ;
			if( tmp >= max )
			{
				max = tmp ;
				alignmentType = INSERTION ;
			}
			
			tmp = alignmentScore() ;
			if( tmp >= max )
			{
				max = tmp ;
				alignmentType = ALIGN ;
			}
			
			return max ;
		}
		
		private int alignmentScore()
		{
			int similarity = 0 ;
			
			// match or mismatch
			if( base1 == base2 )
				similarity = MATCH ;
			else
				similarity = MISMATCH ;
			
			return _matrix[i-1][j-1].score + similarity ;
		}
		
		// score: insertion -> north
		private int insertionScore()
		{
			return _matrix[i][j-1].score + GAP ;
		}
		
		// score: deletion -> west
		private int deletionScore()
		{
			return _matrix[i-1][j].score + GAP ;
		}
		
		private void getAlignment()
		{
			switch( alignmentType )
			{
				case ALIGN :
					if( base1 == base2 )
						alignmentScore = MATCH ;
					else
						alignmentScore = MISMATCH ;
					break ;
					
				case INSERTION :
					alignmentScore = GAP ;
					base1 = GAP_CHAR ;
					break ;
				
				case DELETION :
					alignmentScore = GAP ;
					base2 = GAP_CHAR ;
					break ;
			}
		}
	}
	
	private class Alignment
	{
		private String seq1 , seq2 ;
		private int score ;
		
		private Alignment( String seq1 , String seq2 , int alignmentScore )
		{
			this.seq1 = seq1 ;
			this.seq2 = seq2 ;
			score = alignmentScore ;
		}
	}
	
	
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
