package processing ;

import java.io.File ;
import java.io.FileNotFoundException ;

import java.lang.StringBuilder ;

import java.util.ArrayList ;
import java.util.Scanner ;

public class Fasta 
{
	private static final String NEWLINE = System.lineSeparator() ;
	
	private String _metadata ;
	private ArrayList<String> _reads ;
	
	public Fasta( File file )
	{
		try
		{
			Scanner scanner = new Scanner( file ) ;
			
			_reads = new ArrayList<String>() ;
			_metadata = scanner.nextLine() ;
			
			while( scanner.hasNextLine() )
				_reads.add( scanner.nextLine() ) ;
		
			scanner.close() ;
		}
		catch( FileNotFoundException fnfe )
		{
			System.out.println( "Fasta file not found" ) ;
			System.exit(0) ; 
		}
	}
	
	public String getMetadata()
	{
		return _metadata ;
	}
	
	public ArrayList<String> getReads()
	{
		return _reads ;
	}
	
	public String toString()
	{
		StringBuilder str = new StringBuilder() ;
		str.append( _metadata + NEWLINE ) ;
		
		for( String read : _reads )
		{
			str.append( read + NEWLINE ) ;
		}
		
		str.append( NEWLINE ) ;
		return str.toString() ;
	}
}
