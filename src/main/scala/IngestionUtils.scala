import java.io.File
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.io.FileUtils

import scala.sys.process._

object IngestionUtils {
	private def getFileFromURL( url: String, outputFilename: String ) = {
		( new URL( url ) #> new File( outputFilename ) ).!!
	}
	
	def dateFormatter( format: String, timeInMillis: Long = System.currentTimeMillis ): String = {
		val date = new Date( timeInMillis )
		val dateFormat = new SimpleDateFormat( format )
		println( dateFormat.format( date ) )
		dateFormat.format( date )
	}
	
	def fetchResourceFrom( resourceUrl: String, landingFolder: String, cleanTarget: Boolean = false ): String = {
		val landingFileName = resourceUrl.split( "/" ).last
		val landingTargetPath = s"$landingFolder${System.getProperty( "file.separator" )}$landingFileName"
		if ( cleanTarget ) FileUtils.deleteQuietly( new File( landingFolder ) )
		FileUtils.forceMkdir( new File( landingFolder ) )
		println( s"Fetching resource from $resourceUrl ..." )
		getFileFromURL( resourceUrl, landingTargetPath )
	}
}
