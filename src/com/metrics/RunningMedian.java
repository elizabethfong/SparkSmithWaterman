package com.metrics ;

import java.io.BufferedWriter ;
import java.io.File ;
import java.io.FileWriter ;
import java.io.IOException ;

import java.util.PriorityQueue ;

/**
 * Calculates the median. Taken and modified from my Running Median code
 * which was written for the Coding Challenge for Insight Data Engineering.
 * 
 * @author Elizabeth Fong
 * @version October 2015
 */
public class RunningMedian
{
	// buffered writer for writing to file
	private BufferedWriter _writer ; 
	
    // calculates running median using 2 heaps - a max heap and a min heap
    private PriorityQueue<Integer> _maxHeap ;   // values < median
    private PriorityQueue<Integer> _minHeap ;   // values >= median
    
    // the median
    private double _median ;
    
    
    /* --- CONSTRUCTION ----------------------------------------------------- */
    
    /**
     * Constructor. Initialises the queues used for calculating the running
     * median.
     * 
     * @param directory The name of the directory the output file should be located.
     * @param filename The name the output file.
     */
    public RunningMedian( String directory , String filename )
    {
        _maxHeap = new PriorityQueue<Integer>() ;
        _minHeap = new PriorityQueue<Integer>() ;
        
        _median = 0 ;
        
        // init writing to file
        createWriter( directory , filename ) ;
    }
    
    /**
     * Constructor. Initialises the queues used for calculating the running
     * median.
     */
    public RunningMedian()
    {
    	_maxHeap = new PriorityQueue<Integer>() ;
        _minHeap = new PriorityQueue<Integer>() ;
        
        _median = 0 ;
    }
    
    /**
     * Creates a {@code BufferedWriter} to write to the specified file.
     * If the file or directory does not exist, create them.
     * 
     * @param directory The name of the directory the output file should be located.
     * @param filename The name the output file.
     */
    private void createWriter( String directory , String filename )
    {
    	try
    	{
    		// directory not found
    		File file = new File( directory ) ;
    		
    		if( ! file.exists() )
    		{
    			file.mkdir() ;
    		}
    		
    		// file not found
    		file = new File( directory + "/" + filename ) ;
    		
    		if( ! file.exists() )
    		{
    			file.createNewFile() ;
    		}
    		
    		// init writing
    		_writer = new BufferedWriter( new FileWriter(file.getAbsoluteFile(),false) ) ;
    	}
    	catch( IOException ioe )
        {
        	System.out.println( "IOException at file opening - running median" ) ;
        	ioe.printStackTrace() ;
        }
    }
    
    /* --- ADD VALUE AND CALCULATE MEDIAN ----------------------------------- */
    
    /**
     * Adds the given value and calculates the resulting running median.
     * 
     * @param value The number of unique words in a tweet to be added.
     */
    public void add( int value )
    {
        // add value to a heap
        if( value < _median )
        {
            _maxHeap.add( new Integer(0-value) ) ;
        }
        else
        {
            _minHeap.add( new Integer(value) ) ;
        }
        
        // balance heaps & calculate median
        balanceHeaps() ;
        calculateMedian() ;
        
    }
    
    /**
     * Balances both min and max heaps so that the maximum size difference 
     * between them is 1.
     */
    private void balanceHeaps()
    {
        // max heap -> min heap
        if( _maxHeap.size() > _minHeap.size() + 1 )
        {
            int val = 0 - _maxHeap.poll().intValue() ;
            _minHeap.add( new Integer(val) ) ;
        }
        
        // min heap -> max heap
        else if( _minHeap.size() > _maxHeap.size() + 1 )
        {
            int val = _minHeap.poll().intValue() ;
            _maxHeap.add( new Integer(0-val) ) ;
        }
    }
    
    /**
     * Calculates the median from the balanced min and max heaps.
     */
    private void calculateMedian()
    {
        int sizeMax = _maxHeap.size() ;
        int sizeMin = _minHeap.size() ;
        
        // mean = average of middle 2 values
        if( sizeMax == sizeMin )
        {
            int valMax = 0 - _maxHeap.peek().intValue() ;
            int valMin = _minHeap.peek().intValue() ;
            
            _median = (valMax + valMin) / 2.0 ;
        }
        
        // median is in max heap
        else if( sizeMax > sizeMin )
        {
            _median = 0 - _maxHeap.peek().intValue() ;
        }
        
        // median is in min heap
        else
        {
            _median = _minHeap.peek().intValue() ;
        }
    }
    
    
    /* --- OUTPUT ----------------------------------------------------------- */
    
    /**
     * Returns the running median.
     * 
     * @return The running median.
     */
    public double getRunningMedian()
    {
    	return _median ;
    }
    
    /**
     * Prints the running median to the specified file.
     */
    public void printRunningMedian()
    {
    	try
    	{
    		_writer.write( _median + "" ) ;
    		_writer.newLine() ;
    		
    		System.out.println( "running median: " + _median ) ;
    	}
    	catch( IOException ioe )
    	{
    		System.out.println( "IOException on writing to file - running median" ) ; 
    	}
    }
    
    /**
     * Closes the {@code BufferedWriter}.
     */
    public void closeWriter()
    {
    	try
    	{
    		_writer.close() ;
    	}
    	catch( IOException ioe )
    	{
    		System.out.println( "IOException at BufferedWriter closing - running median" ) ;
    		ioe.printStackTrace() ;
    	}
    }
}