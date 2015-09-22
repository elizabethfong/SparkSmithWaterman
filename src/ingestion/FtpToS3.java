package ingestion ;

import org.apache.commons.net.ftp.FTPClient ;
import org.apache.commons.net.ftp.FTPFile ;

import java.io.FileOutputStream ;
import java.io.IOException ;

import java.net.SocketException ;

/**
 * Ftp -> S3 bucket ingestion
 * 
 * @author Elizabeth
 */
public class FtpToS3
{
	public FtpToS3( String ftpServer , String s3Bucket )
	{
		FTPClient ftp = new FTPClient() ;
		FileOutputStream output = null ;	// ?
		
		try
		{
			System.out.println( "trying to connect" ) ;
			ftp.connect( ftpServer ) ;
			
			FTPFile[] files = ftp.listDirectories() ;
			System.out.println( "# files = " + files.length ) ;
			
			for( int i = 0 ; i < files.length ; i++ )
			{
				System.out.println( files[i].getName() + "	" + files[i].isDirectory() ) ;
			}
			
			ftp.disconnect() ;
		}
		catch( SocketException se )
		{
			System.out.println( "SocketException - FTP to S3" ) ;
			se.printStackTrace() ;
			System.exit(0) ;
		}
		catch( IOException ioe )
		{
			System.out.println( "IOException - FTP to S3" ) ;
			ioe.printStackTrace() ;
			System.exit(0) ;
		}
	}
	
	
	public static void main( String[] args )
	{
		String ftpServer = "ftp.ncbi.nlm.nih.gov" ;
		String s3Bucket = "ncbi-refseq-rna" ;
		
		new FtpToS3( ftpServer , s3Bucket ) ;
	}
}
