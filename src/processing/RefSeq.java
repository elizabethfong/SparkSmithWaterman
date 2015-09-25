package processing ;

import java.lang.StringBuilder ;

// fasta format
public class RefSeq 
{
	private static final String NEWLINE = System.lineSeparator() ;
	
	private String _meta ;
	private StringBuilder _seq ;
	
	public RefSeq( String meta )
	{
		_meta = meta ;
		_seq = new StringBuilder() ;
	}
	
	public void addRead( String read )
	{
		_seq.append( read ) ;
	}
	
	public String getMetadata()
	{
		return _meta ;
	}
	
	public String getSequence()
	{
		return _seq.toString() ;
	}
	
	public String toString()
	{
		StringBuilder str = new StringBuilder() ;
		
		str.append( _meta + NEWLINE ) ;
		str.append( getSequence() ) ;
		
		return str.toString() ;
	}
}
