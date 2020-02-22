import java.io.File
import java.net.URL

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

import scala.sys.process._;

object IngestFiles extends App with LazyLogging {
	// Input URL - "https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.csv
	// HDFS target folder - /user/eddiejobs/tripdata
	
	def getFileFrom( url: String, outputFilename: String ) = {
		new URL( url ).#>( new File( outputFilename ) ).!!
	}
	
	val inputURL = args( 0 )
	val hdfsTargetFolder = args( 1 )
	
	val landingTargetFile = s"tripdata-${System.currentTimeMillis}.csv"
	logger.info( "Reading file from datasource ..." )
	getFileFrom( inputURL, landingTargetFile )
	
	def ingestToHdfs( source: String, target: String ): Unit = {
		logger.info( "Putting the read file into HDFS" )
		val conf = new Configuration()
		val fs = FileSystem.get( conf )
		val srcPath = new Path( source )
		val hdfsTargetFile = s"hdfs:///localhost:8020/$target/$source.csv"
		val destPath = new Path( hdfsTargetFile )
		val output = fs.create( destPath )
		fs.copyFromLocalFile( srcPath, destPath )
		logger.info( s"File ingestion of $source done!" )
	}
	
	ingestToHdfs( s"$landingTargetFile", s"$landingTargetFile" )
}
