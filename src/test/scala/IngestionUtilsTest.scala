import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{ BeforeAndAfterAll, DiagrammedAssertions, FlatSpec }

class IngestionUtilsTest extends FlatSpec with DiagrammedAssertions with BeforeAndAfterAll {
	
	override def beforeAll( ): Unit = {
		FileUtils.deleteQuietly( new File( "landing" ) )
	}
	
	"The time in millis 1582420819511" should "be formatted as 20200222" in {
		val timeInMillis = 1582420819511L
		val dateFormat = "YYYYMMdd"
		val expectedDate = "20200222"
		assert( IngestionUtils.dateFormatter( dateFormat, timeInMillis ).equals( expectedDate ) )
	}
	
	"If we provide a valid URL https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.csv associated to a file, we" should
			"download it a clean and empty target folder \"landing\"" in {
		val url = "https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.csv"
		val expectedFilename = "2017-fordgobike-tripdata.csv"
		val targetFolder = "landing"
		IngestionUtils.fetchResourceFrom( url, targetFolder, true )
		assert( FileUtils.getFile( s"$targetFolder${System.getProperty( "file.separator" )}$expectedFilename" ).exists() )
	}
	
	"If we provide an invalid URL https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.cs associated to an inexistent file, we" should
			"get an RuntimeException" in {
		val url = "https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.cs"
		val expectedFilename = "2017-fordgobike-tripdata.csv"
		val targetFolder = "landing"
		assertThrows[ RuntimeException ] {
			IngestionUtils.fetchResourceFrom( url, targetFolder, true )
		}
	}
	
}
