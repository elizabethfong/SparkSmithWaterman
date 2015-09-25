package processing ;

import java.io.File ;
import java.io.FileNotFoundException ;

import java.util.ArrayList ;
import java.util.Iterator ;
import java.util.Scanner ;

public class RefSeqParser 
{
	private final String _delim ;
	private ArrayList<RefSeq> _seq ;
	
	private Iterator<RefSeq> _iterator ;
	
	public RefSeqParser( File file , String delim )
	{
		_delim = delim ;
		_seq = new ArrayList<RefSeq>() ;
		_iterator = null ;
		
		try
		{
			Scanner scanner = new Scanner( file ) ;
			
			RefSeq tmp = null ;
			
			while( scanner.hasNextLine() )
			{
				String str = scanner.nextLine() ;
				
				// length of line is less than delimiter || line does not start with delimiter
				if( str.length() < _delim.length() || ! str.substring(0,_delim.length()).equals(_delim) )
				{
					tmp.addRead(str) ;
				}
				
				// start of a new sequence
				else
				{
					if( tmp != null )
						_seq.add(tmp) ;
					
					tmp = new RefSeq( str.substring(_delim.length()) ) ;
				}
			}
			
			_iterator = _seq.iterator() ;
			scanner.close() ;
		}
		catch( FileNotFoundException fnfe )
		{
			System.out.println( "Reference Sequence file not found." ) ;
			System.exit(0) ;
		}
	}
	
	public ArrayList<RefSeq> getSequences()
	{
		return _seq ;
	}
	
	public boolean hasNext()
	{
		return _iterator.hasNext() ;
	}
	
	public RefSeq next()
	{
		return _iterator.next() ;
	}
	
}
