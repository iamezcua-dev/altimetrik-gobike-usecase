package com.altimetrik.gobike.utils

import java.io.File
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

import scala.sys.process._

object IngestionUtils {
	private def getFileFromURL( url: String, outputFilename: String ) = {
		val file = new File( outputFilename )
		( new URL( url ) #> file ).!!
		file.getPath
	}
	
	def dateFormatter( format: String, timeInMillis: Long = System.currentTimeMillis ): String = {
		val date = new Date( timeInMillis )
		val dateFormat = new SimpleDateFormat( format )
		println( dateFormat.format( date ) )
		dateFormat.format( date )
	}
	
	def fetchResourceFrom( resourceUrl: String, landingFolder: String, cleanTarget: Boolean = false ): String = {
		val landingFileName = resourceUrl.split( System.getProperty( "file.separator" ) ).last
		val landingTargetPath = s"$landingFolder${System.getProperty( "file.separator" )}$landingFileName"
		if ( cleanTarget ) FileUtils.deleteQuietly( new File( landingFolder ) )
		FileUtils.forceMkdir( new File( landingFolder ) )
		println( s"Fetching resource from $resourceUrl ..." )
		getFileFromURL( resourceUrl, landingTargetPath )
	}
	
	def getFileSystemConnection( fileSystemUri: String ): FileSystem = {
		val conf = new Configuration()
		conf.set( "fs.defaultFS", fileSystemUri )
		val fs = FileSystem.get( conf )
		//    logger.debug( s"Default filesystem: ${conf.get( "fs.defaultFS" )}" )
		//    logger.debug( s"Filesystem URI: ${fs.getUri}" )
		//    logger.debug( s"Filesystem configuration read: ${fs.getConf}" )
		fs
	}
	
	def ingestToHdfs( filesystemUri: String, fileToIngest: String, hdfsTargetFolder: String ): Unit = {
		//		logger.info( "Putting the read file into HDFS" )
		// Building configuration for HDFS access
		val fs = getFileSystemConnection( filesystemUri )
		// Obtaining filename from source string
		val hdfsFilename = fileToIngest.split( s"${System.getProperty( "file.separator" )}" ).last
		//    logger.debug( s"Filename on HDFS: $hdfsFilename" )
		// Converting sourcePath to a HDFScompatible one
		val sourcePath = new Path( fileToIngest )
		// Defining the HDFS target path
		val hdfsDestinationPath = new Path( s"$hdfsTargetFolder/$hdfsFilename" )
		//    logger.debug( s"Source path: $sourcePath" )
		//    logger.debug( s"Destination path: $hdfsDestinationPath" )
		// Open data output stream
		val output = fs.create( hdfsDestinationPath, true )
		// Perform file ingestion to HDFS target folder
		println( s"Ingesting file $fileToIngest to HDFS target $hdfsDestinationPath ..." )
		fs.copyFromLocalFile( sourcePath, hdfsDestinationPath )
		// Release connection and resources
		fs.close()
		//		logger.info( s"File ingestion of $sourcePath done!" )
	}
	
	def isFileInHdfs( filesystemUri: String, hdfsPath: String ): Boolean = {
		val fs = getFileSystemConnection( filesystemUri )
		val existsOrNot = fs.exists( new Path( hdfsPath ) )
		// Release connection and resources
		fs.close()
		existsOrNot
	}
	
	def deleteFromHdfs( filesystemUri: String, hdfsPathToDelete: String ): Unit = {
		val fs = getFileSystemConnection( filesystemUri )
		val hdfsPath = new Path( hdfsPathToDelete )
		if ( fs.exists( hdfsPath ) ) fs.delete( hdfsPath, true )
		// Release connection and resources
		fs.close()
	}
}