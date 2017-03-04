import java.io.{BufferedReader, InputStreamReader}

import io.circe.{Json, JsonObject}
import io.circe.{Decoder, Encoder}
import io.circe.syntax._


case class dataModel(data:Option[String],label:Option[String]) //TODO:rename this. too generic

object  dataModel{
  implicit val encoder: Encoder[dataModel] = io.circe.generic.semiauto.deriveEncoder
  implicit val decoder: Decoder[dataModel] = io.circe.generic.semiauto.deriveDecoder
}

object HeadChef extends JsonConverter {
  /** HeadChef is the main resource of Entree and will direct every other resource. */
  private val cfnMap:Map[String,Array[String]] = Map (
    "email_address" -> Array("email_address","email","emailaddress")
  )

  private val rowsPerFile = 5000


  def getFilesWithLabel(source:S3Bucket,label:String) = {
    val files = DtlS3Cook.apply.listFiles(source.bucket).filterNot(_.endsWith("/"))
    val fileNames = files.map(_.split("/").last)
    label match {
      case "." => {
        println(fileNames)
        aggregateFiles(fileNames,source)
      }
      case _ => {
        val lf = fileNames.filterNot(cfnMap(label).contains(_)) //lf = labeled files or files whose name are under the designated lf
        println(lf)
        aggregateFiles(lf,source)
      }
    }
  }

  def aggregateFiles(flist:Vector[String],s3bucket:S3Bucket):Vector[Json] = flist.flatMap( f => readFile(f,s3bucket))


  def readFile(fileName:String,s3Bucket: S3Bucket) = {
    println(s3Bucket.folderPath.getOrElse("") + fileName)
    val input = DtlS3Cook.apply.getFileStream(s3Bucket.bucket,s3Bucket.folderPath.getOrElse("") + fileName)
    val reader = new BufferedReader(new InputStreamReader(input))
    val fileString = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString(",")
    val vectorString = fileString.split(",").toVector
    val mo : Vector[Option[Map[String,Json]]] = vectorString.map( s => toJson(s) match { //mo = map object
      case None => None
      case Some(j) => Some(j.asObject.get.toMap)
    }).filter(m => m.isDefined && m.get.nonEmpty)
    // each object per line was converted to a JSON object and then to a Map. Any empty objects or None were filtered out.
    val modelVector = mo.flatMap(m => map2Model(m.get))
    val jmv : Vector[Json] = modelVector.map(_.asJson) // jmv = Json Model Vector
    reader.close()
    jmv
  }

  def map2Model ( m: Map[String,Json]) : Vector[dataModel] = m.map{case (k,v) => dataModel(v.asString,Some(k))}.toVector

}
