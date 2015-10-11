package com.sw ;

import java.io.File ;

/**
 * TODO 
 * OOP Directory Crawler
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
	 * TODO
	 * 
	 * @param root	
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
	 * TODO
	 * 
	 * @return	
	 */
	public boolean hasNext()
	{
		return _current.hasNext() ;
	}
	
	/**
	 * TODO
	 * 
	 * @return	
	 */
	public File next()
	{
		return _current.next() ;
	}
	
	
	/* --- NODE CLASS ------------------------------------------------------ */
	
	/**
	 * TODO
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
		 * TODO 
		 * 
		 * @param path		
		 * @param parent	
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
		 * TODO
		 * 
		 * @return	
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
		 * TODO
		 * 
		 * @return	
		 */
		private File next()
		{
			return children[index] ;
		}
	}
}
