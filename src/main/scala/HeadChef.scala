import java.io._

import io.circe.{Json, JsonObject}
import io.circe.{Decoder, Encoder}
import io.circe.syntax._


case class dataFormat(data:Option[String],label:Option[String],originalLabel:Option[String]) //TODO:rename this. too generic
//TODO:Add original label

object  dataFormat{
  implicit val encoder: Encoder[dataFormat] = io.circe.generic.semiauto.deriveEncoder
  implicit val decoder: Decoder[dataFormat] = io.circe.generic.semiauto.deriveDecoder
}

object HeadChef extends JsonConverter {
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

  private val rowsPerFile = 5000


  def getFilesWithLabel(source:S3Bucket,destination:S3Bucket,label:String) = {
    println(source.bucket)
    println(source.folderPath.getOrElse(""))
    val files = DtlS3Cook.apply.listFiles(source.bucket).filterNot( fp => fp.endsWith("/")).filter(_.contains(source.folderPath.getOrElse("")))
    val fileNames = files.map(_.split("/").last)
    label match {
      case "all" =>
        println(fileNames)
        val dataModels = aggregateFiles(fileNames,source)
        batchSave(dataModels,destination,label)
      case _ =>
        val lf = fileNames.filterNot(CFNMappingCook.isValPresent(label,_)) //lf = labeled files or files whose name are under the designated lf
        println(lf)
        val dataModels = aggregateFiles(lf,source)
        batchSave(dataModels,destination,label)
    }
  }

  def aggregateFiles(flist:Vector[String],s3bucket:S3Bucket):Vector[String] = flist.flatMap( f => readFile(f,s3bucket))

  def readFile(fileName:String,s3Bucket: S3Bucket) : Vector[String] = {
    println(s3Bucket.folderPath.getOrElse("") + fileName)
    val input = DtlS3Cook.apply.getFileStream(s3Bucket.bucket,s3Bucket.folderPath.getOrElse("") + fileName)
    val reader = new BufferedReader(new InputStreamReader(input))
    val fileString = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString(",")
    reader.close()
    val vectorString = fileString.split(",").toVector
    val mo : Vector[Option[Map[String,Json]]] = vectorString.map( s => toJson(s) match { //mo = map object
      case None => None
      case Some(j) => Some(j.asObject.get.toMap)
    }).filter(m => m.isDefined && m.get.nonEmpty)
    // each object per line was converted to a JSON object and then to a Map. Any empty objects or None were filtered out.
    val modelVector = mo.flatMap(m => map2Model(m.get))
    modelVector.map(_.asJson.noSpaces)
  }

  def map2Model ( m: Map[String,Json]) : Vector[dataFormat] = m.map{case (k,v) => dataFormat(v.asString,Some(CFNMappingCook.getKeyFromVal(k)),Some(k))}.toVector




  def saveToS3(v:Vector[String],dest:S3Bucket,fname:String) = {
    val f = new File(s"$fname.json") //TODO: Figure out file naming convention
    val bw = new BufferedWriter(new FileWriter(f))
    v.foreach(s => bw.write( s + "\n"))
    bw.close()
//    val f = v.map(_.toByte).toArray  //TODO:Figure out to stream the content (v) back to S3.
    DtlS3Cook.apply.saveFile(dest.bucket,dest.folderPath.getOrElse(""),f)
  }

  def batchSave(v:Vector[String],dest:S3Bucket,label:String) : Unit = {
    val splitIdx = Seq.range(1,v.length/rowsPerFile).toVector.map( _ * rowsPerFile)
    val splitV = v.grouped(rowsPerFile).toVector.zipWithIndex
    splitV.map{ case (vec,idx) => saveToS3(vec,dest,label + "-" + idx.toString)}
  }

}
