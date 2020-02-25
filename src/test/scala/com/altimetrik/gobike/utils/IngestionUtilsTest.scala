package com.altimetrik.gobike.utils

import java.io.File

import com.altimetrik.gobike.utils.IngestionUtils._
import org.apache.commons.io.FileUtils
import org.scalatest.PrivateMethodTester._
import org.scalatest.{ BeforeAndAfterAll, DiagrammedAssertions, FlatSpec }

class IngestionUtilsTest extends FlatSpec with DiagrammedAssertions with BeforeAndAfterAll {
	
	override def beforeAll: Unit = {
		FileUtils.deleteQuietly( new File( "landing" ) )
	}
	
	override protected def afterAll: Unit = {
		val filesystemUri = "hdfs://localhost:9000"
		FileUtils.deleteQuietly( new File( "src/test/landing" ) )
		deleteFromHdfs( filesystemUri, "/tmp/go-bike" )
	}
	
	"The time in millis 1582420819511" should "be formatted as 20200222" in {
		val timeInMillis = 1582420819511L
		val dateFormat = "YYYYMMdd"
		val expectedDate = "20200222"
		assert( dateFormatter( dateFormat, timeInMillis ).equals( expectedDate ) )
	}
	
	"If we provide a valid URL https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.csv associated to a file, we" should
			"download it to a clean and empty target folder \"landing\"" in {
		val url = "https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.csv"
		val expectedFilename = "2017-fordgobike-tripdata.csv"
		val targetFolder = "src/test/landing"
		fetchResourceFrom( url, targetFolder, true )
		assert( FileUtils.getFile( s"$targetFolder${System.getProperty( "file.separator" )}$expectedFilename" ).exists() )
	}
	
	"If we provide an invalid URL https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.cs associated to an inexistent file, we" should
			"get an RuntimeException" in {
		val url = "https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.cs"
		val expectedFilename = "2017-fordgobike-tripdata.csv"
		val targetFolder = "src/test/landing"
		assertThrows[ RuntimeException ] {
			fetchResourceFrom( url, targetFolder, true )
		}
	}
	
	"Invoking the private method \"getFileFromURL\" with a valid remote URL and a valid landing target" should
			"write the file in that target and return the absolute path of the output file" in {
		val resourceUrl = "https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.csv"
		val landingFolder = "src/test/landing"
		val landingFileName = resourceUrl.split( System.getProperty( "file.separator" ) ).last
		val landingTarget = s"$landingFolder${System.getProperty( "file.separator" )}$landingFileName"
		
		FileUtils.deleteQuietly( new File( landingFolder ) )
		FileUtils.forceMkdir( new File( landingFolder ) )
		
		println( s"Fetching resource from $resourceUrl ..." )
		val getFileFromURL = PrivateMethod[ String ]( 'getFileFromURL )
		val actualPath = IngestionUtils.invokePrivate( getFileFromURL( resourceUrl, landingTarget ) )
		println( actualPath )
		assert( s"$landingFolder${System.getProperty( "file.separator" )}$landingFileName" equals actualPath )
	}
	
	"The ingestToHdfs method" should "ingest a file from our local filesystem to HDFS target" in {
		val filesystemUri = "hdfs://localhost:9000"
		val fileToIngest = "src/test/resources/2017-fordgobike-tripdata.csv"
		
		// Ingest file to HDFS target
		val hdfsTargetFolder = "/tmp/go-bike"
		ingestToHdfs( filesystemUri, fileToIngest, hdfsTargetFolder )
		assert( isFileInHdfs( filesystemUri, s"$hdfsTargetFolder/${fileToIngest.split( System.getProperty( "file.separator" ) ).last}" ) )
	}
	
}