package processing ;

import java.io.File ;


public class DirectoryCrawler
{
	private Directory _root ;
	private Directory _current ;
	
	public DirectoryCrawler( String root )
	{
		// error handling -> directory does not exist
		File file = new File( root ) ;
		
		if( ! file.exists() )
		{
			System.out.println( "Root directory not found" ) ;
			System.exit(0) ;
		}
		
		// directory exists!
		_root = new Directory( root , null ) ;
		_current = _root ;
	}
	
	public boolean hasNext()
	{
		return _current.hasNext() ;
	}
	
	public File next()
	{
		return _current.next() ;
	}
	
	
	/* --- NODE CLASS ------------------------------------------------------ */
	
	@SuppressWarnings("serial")
	private class Directory extends File
	{	
		final Directory parent ;
		File[] children ;
		
		int index ;
		
		
		private Directory( String path , Directory parent )
		{
			super( path ) ;
			
			// init
			this.parent = parent ;
			children = listFiles() ;
			index = -1 ;
			
			_current = this ;
		}
		
		private boolean hasNext()
		{
			if( index > -1 )
				children[index] = null ;
			
			index ++ ;
			
			// at end of children, move to parent
			if( index >= children.length )
			{
				if( parent == null )
				{
					_current = null ;
					return false ;
				}
				
				return parent.hasNext() ;
			}
			
			// next is a directory
			File next = children[index] ;
			
			if( next.isDirectory() )
			{
				Directory dir = new Directory( next.getPath() , this ) ;
				children[index] = dir ;
				return dir.hasNext() ;
			}
			
			// next is not a directory
			_current = this ;
			return true ;
		}
		
		private File next()
		{
			return children[index] ;
		}
	}
	
	/* --- MAIN ------------------------------------------------------------ */
	
	/*
	public static void main( String[] args )
	{
		String root = "test" ;
		
		DirectoryCrawler dir = new DirectoryCrawler( root ) ;
		
		while( dir.hasNext() )
		{
			System.out.println( dir.next() ) ;
		}
		
	}
	*/
}
