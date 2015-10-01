package com.sw ;

import java.io.BufferedWriter ;
import java.io.File ;
import java.io.FileNotFoundException ;
import java.io.FileWriter ;
import java.io.IOException ;

import java.util.ArrayList ;
import java.util.Scanner ;

import org.apache.spark.api.java.function.Function2 ;


@SuppressWarnings( "serial" )
public class InOutOps
{
	public static final String NEWLINE = System.lineSeparator() ;
	
	
	/* --- INPUT ----------------------------------------------------------- */
	
	public static class GetReads implements Function2< String , String , ArrayList<String> >
	{
		public ArrayList<String> call( String filepath , String delimiter )
		{
			ArrayList<String> reads = new ArrayList<String>() ;
			
			try
			{
				Scanner scanner = new Scanner( new File(filepath) ) ;
				
				// first line - may contain metadata
				String line = scanner.nextLine().trim() ;
				
				if( ! new IsMetadata().call(line,delimiter).booleanValue() )
					reads.add( line ) ;
				
				// subsequent lines
				while( scanner.hasNextLine() )
					reads.add( scanner.nextLine().trim() ) ;
				
				// done!
				scanner.close() ;
			}
			catch( FileNotFoundException fnfe )
			{
				System.out.println( "Input file not found." ) ;
				System.exit(0) ;
			}
			
			return reads ;
		}
	}
	
	public static class GetRefSeqs implements Function2< String , String , ArrayList<String[]> >
	{
		public ArrayList<String[]> call( String filepath , String delimiter )
		{
			ArrayList<String[]> sequences = new ArrayList<String[]>() ;
			
			try
			{
				Scanner scanner = new Scanner( new File(filepath) ) ;
				
				String[] ref = null ;
				StringBuilder seq = null ;
				
				// each line
				while( scanner.hasNextLine() )
				{
					String line = scanner.nextLine() ;
					
					if( new IsMetadata().call(line,delimiter).booleanValue() )
					{
						if( ref != null )
						{
							ref[1] = seq.toString() ;
							sequences.add(ref) ;
							
							seq = null ;
							ref = null ;
						}
						
						ref = new String[2] ;
						ref[0] = line ;
						seq = new StringBuilder() ;
					}
					else
					{
						seq.append( line ) ;
					}
				}
				
				// last line
				ref[1] = seq.toString() ;
				sequences.add(ref) ;
				
				seq = null ;
				ref = null ;
				
				scanner.close() ;
			}
			catch( FileNotFoundException fnfe )
			{
				System.out.println( "Reference file not found" ) ;
				System.exit(0) ;
			}
			
			return sequences ;
		}
	}
	
	
	/* --- OUTPUT ---------------------------------------------------------- */
	
	public static class PrintToFile implements Function2< String , String , Boolean >
	{
		public Boolean call( String filepath , String data )
		{
			try
			{
				File file = new File( filepath ) ;
				
				if( ! file.exists() )
					file.createNewFile() ;
				
				BufferedWriter writer = new BufferedWriter( new FileWriter(file.getAbsolutePath(),false) ) ;
				
				writer.write( data ) ;
				writer.close() ;
				
				return true ;
			}
			catch( IOException ioe )
			{
				System.out.println( "IOException on writing to file" ) ;
				ioe.printStackTrace() ;
				return false ;
			}
		}
	}
	
	
	/* --- UTILITY --------------------------------------------------------- */
	
	private static class IsMetadata implements Function2< String , String , Boolean >
	{
		public Boolean call( String line , String delimiter )
		{
			if( line.length() >= delimiter.length() && line.substring(0,delimiter.length()).equals(delimiter) )
				return true ;
			else
				return false ;
		}
	}
}
