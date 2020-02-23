object IngestFiles extends App {
	// Configuring landing zone target
	val resourceUrl = "https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.csv"
	// Edge node
	val landingFolder = "landing"
	IngestionUtils.fetchResourceFrom( resourceUrl, landingFolder, true )
	println( "Done!" )
}
