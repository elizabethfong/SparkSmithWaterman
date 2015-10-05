package com.sw ;

import java.io.BufferedWriter ;
import java.io.File ;
import java.io.FileNotFoundException ;
import java.io.FileWriter ;
import java.io.IOException ;

import java.lang.StringBuilder ;

import java.util.ArrayList ;
import java.util.Scanner ;

import org.apache.spark.api.java.function.Function2 ;
import org.apache.spark.api.java.function.Function3 ;

import scala.Tuple2 ;
import scala.Tuple3 ;


@SuppressWarnings( "serial" )
public class InOutOps
{
	public static final String NEWLINE = System.lineSeparator() ;
	public static final String TAB = "	" ;
	
	
	/* --- INPUT ----------------------------------------------------------- */
	
	public static class GetReads implements Function2< File , String , ArrayList<String> >
	{
		public ArrayList<String> call( File file , String delimiter )
		{
			ArrayList<String> reads = new ArrayList<String>() ;
			
			try
			{
				Scanner scanner = new Scanner( file ) ;
				
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
	
	public static class GetRefSeqs implements Function2< File , String , ArrayList<String[]> >
	{
		public ArrayList<String[]> call( File file , String delimiter )
		{
			ArrayList<String[]> sequences = new ArrayList<String[]>() ;
			
			try
			{
				Scanner scanner = new Scanner( file ) ;
				
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
	
	public static class PrintStrToFile implements Function2< String , String , Boolean >
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
	
	public static class GetOutputStr 
			 implements Function3< ArrayList<String> , Tuple3<int[],Integer,Long> , 
			 					   ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> ,
			 					   String >
	{
		public String call( ArrayList<String> reads , Tuple3<int[],Integer,Long> data , 
							ArrayList<Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>>> opt )
		{
			StringBuilder str = new StringBuilder() ;
			
			// execution time
			str.append( "Execution Time = " + data._3() + " ms" + NEWLINE ) ;
			str.append( NEWLINE ) ;
			
			// num reference and reads
			str.append( "# Reference Sequences = " + data._1()[0] + NEWLINE ) ;
			str.append( "# Reads = " + data._1()[1] + NEWLINE ) ;
			str.append( NEWLINE ) ;
			
			// Input
			str.append( "Input:" + NEWLINE ) ;
			for( String read : reads )
				str.append( read + NEWLINE ) ;
			str.append( NEWLINE ) ;
			
			// max score
			str.append( "Maximum alignment score = " + data._2() ) ;
			str.append( NEWLINE ) ;
			
			// print sequences & matching sites
			for( Tuple2<String[],ArrayList<Tuple2<Integer,String[]>>> seq : opt )
			{
				str.append( "Reference:" + NEWLINE ) ;
				str.append( seq._1()[0] + NEWLINE ) ;
				str.append( seq._1()[1] + NEWLINE ) ;
				str.append( NEWLINE ) ;
				
				// matching sites
				ArrayList<Tuple2<Integer,String[]>> sites = seq._2() ;
				
				for( Tuple2<Integer,String[]> site : sites )
				{
					str.append( TAB + "Index = " + site._1() + NEWLINE ) ;
					str.append( TAB + site._2()[0] + NEWLINE ) ;
					str.append( TAB + site._2()[1] + NEWLINE ) ;
					str.append( NEWLINE ) ;
				}
			}
			
			return str.toString() ;
		}
	}
	
	
	
	
	public static class PrintMatrices implements Function3< int[][] , char[][] , String[] , String >
	{
		public String call( int[][] scores , char[][] aligns , String[] seq )
		{
			StringBuilder str = new StringBuilder() ;
			
			String _seq1 = seq[1] ;
			String _seq2 = seq[0] ;
			
			// score matrix
			str.append( NEWLINE ) ;
			str.append( "   _  " ) ;
			
			for( int i = 0 ; i < _seq2.length() ; i++ )
			{
				str.append( Character.toUpperCase(_seq2.charAt(i)) + "  " ) ;
			}
			
			str.append( NEWLINE ) ;
			
			for( int i = 0 ; i < scores.length ; i++ )
			{
				if( i == 0 )
					str.append( "_  " ) ;
				else
					str.append( Character.toUpperCase(_seq1.charAt(i-1)) + "  " ) ;
				
				for( int j = 0 ; j < scores[i].length ; j++ )
				{
					int score = scores[i][j] ;
					
					if( score < 10 )
						str.append( score + "  " ) ;
					else
						str.append( score + " " ) ;
				}
				
				str.append( NEWLINE ) ;
			}
			
			str.append( NEWLINE ) ;
			
			// align type matrix
			str.append( "   _  " ) ;
			
			for( int i = 0 ; i < _seq2.length() ; i++ )
			{
				str.append( Character.toUpperCase(_seq2.charAt(i)) + "  " ) ;
			}
			
			str.append( NEWLINE ) ;
			
			for( int i = 0 ; i < aligns.length ; i++ )
			{
				if( i == 0 )
					str.append( "_  " ) ;
				else
					str.append( Character.toUpperCase(_seq1.charAt(i-1)) + "  " ) ;
				
				for( int j = 0 ; j < aligns[i].length ; j++ )
				{
					str.append( aligns[i][j] + "  " ) ;
				}
				
				str.append( NEWLINE ) ;
			}
			
			System.out.println( str.toString() ) ;
			return str.toString() ;
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
