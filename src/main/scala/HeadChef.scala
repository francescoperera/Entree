import java.io._

import io.circe.{Json, JsonObject}
import io.circe.{Decoder, Encoder}
import io.circe.syntax._

import com.typesafe.scalalogging.LazyLogging


case class dataFormat(data:Option[String],label:Option[String],originalLabel:Option[String]) //TODO:rename this. too generic
//TODO:Add original label

object  dataFormat{
  implicit val encoder: Encoder[dataFormat] = io.circe.generic.semiauto.deriveEncoder
  implicit val decoder: Decoder[dataFormat] = io.circe.generic.semiauto.deriveDecoder
}

object HeadChef extends JsonConverter with LazyLogging {
  /** HeadChef is the main resource of Entree and will direct every other resource. */
  val cfnMap:Map[String,Array[String]] = Map ( //TODO: Fucking manual work. Automate this ASAP
    "email_address" -> Array("email_address","email","emailaddress"),
    "full_name" -> Array("full_name"),
    "first_name" -> Array("first_name","firstname"),
    "last_name" -> Array("last_name","lastname","surname","family_name"),
    "name" -> Array ("name"),
    "middle_name" -> Array("middle_name","middlename"),
    "address" -> Array("address","physical_address","physicaladdress","employer_address","work_address"),
    "phone_number" -> Array("phone_number","phonenumber","phone"),
    "username" -> Array("username","user_name"),
    "education"-> Array("college","university","school"),
    "employee" -> Array("employee"),
    "employee_id" -> Array("employee_id"),
    "uuid" -> Array("uuid"),
    "job_title" -> Array("job_title","jobtitle","occupation","occupation_title"), //TODO: double check  difference between occupation and job title
    "salary" -> Array("salary"),
    "employer" -> Array("employer")
  )

  private val rowsPerFile = 10000


  def getFilesWithLabel(source:S3Bucket,destination:S3Bucket,label:String) = {
    logger.info(s"Getting files from S3 Bucket: ${source.bucket}")
    logger.info(s"Following Folder Path : ${source.folderPath.getOrElse("")}")
    val files = DtlS3Cook.apply.listFiles(source.bucket).filterNot( fp => fp.endsWith("/")).filter(_.contains(source.folderPath.getOrElse("")))
    val fileNames = files.map(_.split("/").last)
    label match {
      case "all" =>
        aggregateFiles(fileNames,source,destination,"all")
      case _ =>
        val lf = fileNames.filterNot(CFNMappingCook.isValPresent(label,_)) // lf = labeled files or files whose name are under the designated lf //TODO: filter based on content not file name
        aggregateFiles(lf,source,destination,_)
    }
  }


  def aggregateFiles(flist:Vector[String],source:S3Bucket,destination:S3Bucket,label:String) = {
    logger.info(s"Aggregating data from ${flist.length} files")
    val dataModels = flist.flatMap( f => readFile(f,source))
    logger.info(s"Saving to Bucket: ${destination.bucket}, Path:${destination.folderPath}")
    batchSave(dataModels,destination,label)

  }

  def readFile(fileName:String,s3Bucket: S3Bucket) : Vector[String] = {
    logger.info(s" Reading file: $fileName")
    val input = DtlS3Cook.apply.getFileStream(s3Bucket.bucket,s3Bucket.folderPath.getOrElse("") + fileName)
    val reader = new BufferedReader(new InputStreamReader(input))
    val fileString = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString(",")
    reader.close()
    val vectorString = fileString.split(",").toVector
    val mo : Vector[Option[Map[String,Json]]] = vectorString.map( s => toJson(s) match { //mo = map object
      case None => Some(Map[String,Json]())
      case Some(j) =>
        Some(j.asObject.getOrElse(JsonObject.empty).toMap)
    }).filterNot(_.isEmpty) //filterNot(m => m.isDefined)
    // each object per line was converted to a JSON object and then to a Map. Any empty objects or None were filtered out.
    val modelVector = mo.flatMap{m => map2Model(m.get)}
    modelVector.map(_.asJson.noSpaces)
  }

  def map2Model ( m: Map[String,Json]) : Vector[dataFormat] = m.map{case (k,v) => dataFormat(v.asString,Some(CFNMappingCook.getKeyFromVal(k)),Some(k))}.toVector

  def saveToS3(v:Vector[String],dest:S3Bucket,fname:String) = {
    val f = new File(s"$fname.json") //TODO: Figure out file naming convention
    val bw = new BufferedWriter(new FileWriter(f))
    v.foreach(s => bw.write( s + "\n"))
    bw.close()
//    val f = v.map(_.toByte).toArray  //TODO:Figure out to stream the content (v) back to S3.
    logger.info(s"Saving to S3:$fname")
    DtlS3Cook.apply.saveFile(dest.bucket,dest.folderPath.getOrElse(""),f)
  }

  def batchSave(v:Vector[String],dest:S3Bucket,label:String) = {
    val splitIdx = Seq.range(1,v.length/rowsPerFile).toVector.map( _ * rowsPerFile)
    val splitV = v.grouped(rowsPerFile).toVector.zipWithIndex
    splitV.foreach{ case (vec,idx) => saveToS3(vec,dest,label + "_" + idx.toString)}
  }

}
