package sw ;

import java.io.File ;

/**
 * A directory crawler. Implemented in an Object-Oriented manner.
 * 
 * @author Elizabeth Fong
 * @version Insight Data Engineering NY, September-October 2015
 */
public class DirectoryCrawler
{
	// directory/file pointers
	private Directory _root ;
	private Directory _current ;
	
	
	/* --- METHODS --------------------------------------------------------- */
	
	/**
	 * Constructor. Initialises the directory crawler.
	 * 
	 * @param root	The path for the root of the directory.
	 */
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
	
	/**
	 * Returns {@code true} if the directory has a next file, {@code false} otherwise.
	 * 
	 * @return {@code true} if the directory has a next file, {@code false} otherwise.
	 */
	public boolean hasNext()
	{
		return _current.hasNext() ;
	}
	
	/**
	 * Returns the next file in the directory.
	 * 
	 * @return The next file in the directory.
	 */
	public File next()
	{
		return _current.next() ;
	}
	
	
	/* --- NODE CLASS ------------------------------------------------------ */
	
	/**
	 * This class organises the directories in a tree-like structure,
	 * where each directory/sub-directories and files are nodes.
	 * Directories have children, where files are leaf nodes (no children).
	 * 
	 * @author Elizabeth Fong
	 * @version Insight Data Engineering NY, September-October 2015
	 */
	@SuppressWarnings("serial")
	private class Directory extends File
	{	
		// pointers
		private final Directory parent ;
		private File[] children ;
		
		private int index ;
		
		
		/**
		 * Constructor.
		 * Creates a node, given the path to the file/directory and the parent node.
		 * 
		 * @param path		The path to this file/directory.
		 * @param parent	The parent directory.
		 */
		private Directory( String path , Directory parent )
		{
			super( path ) ;
			
			// init
			this.parent = parent ;
			children = listFiles() ;
			index = -1 ;
			
			_current = this ;
		}
		
		/**
		 * Returns {@code true} if this directory has a next file, {@code false} otherwise.
		 * This method moves the pointer to the next file.
		 * 
		 * @return {@code true} if this directory has a next file, {@code false} otherwise.
		 */
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
		
		/**
		 * Returns the next file in the directory.
		 * 
		 * @return The next file in the directory.
		 */
		private File next()
		{
			return children[index] ;
		}
	}
}
