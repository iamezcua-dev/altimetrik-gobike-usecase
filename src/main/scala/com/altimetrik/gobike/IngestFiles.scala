package com.altimetrik.gobike

import com.altimetrik.gobike.utils.IngestionUtils._

object IngestFiles extends App {
	// Fetching file from remote URL to landing zone
	val resourceUrl = s"${args( 0 )}"
	val landingFolder = s"${args( 1 )}"
	val fileSystemUri = s"${args( 2 )}"
	val hdfsTargetFolder = s"${args( 3 )}"
	val fileToIngest = fetchResourceFrom( resourceUrl, landingFolder, true )
	
	// Ingesting file to HDFS target
	ingestToHdfs( fileSystemUri, fileToIngest, hdfsTargetFolder )
	
	println( "Done!" )
}