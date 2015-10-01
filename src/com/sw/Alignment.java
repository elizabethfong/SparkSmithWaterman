package com.sw ;

public class Alignment 
{
	// CONSTANTS
	final String FILE_REF = "/home/ubuntu/project/testRef/vertebrate_mammalian.107.rna.fna" ;
	final String FILE_IN = "/home/ubuntu/project/testIn/test5.fa" ;
	
	final int[] ALIGN_SCORES = {5,-3,-4} ;	// {match,mismatch,gap}
	final char[] ALIGN_TYPES = {'a','i','d','-'} ;
	final String DELIMITER = ">gi" ;
	
	final String APP_NAME = "Smith-Waterman in Spark" ;
	
	
	public static void main( String[] args )
	{
		// TODO main - code here
	}
	
	
	// DISTRIBUTE ALGORITHM
	
	
	// DISTRIBUTE REFERENCE
	
	
	// DISTRIBUTE READS
	
	
	// NO DISTRIBUTION
	public static class NoDistribution
	{
		
	}
}
