import java.io.{BufferedReader, InputStreamReader}

import io.circe.{Json, JsonObject}

case class objectModel(data:Option[String],label:Option[String]) //TODO:rename this. too generic

object HeadChef extends JsonConverter {
  /** HeadChef is the main resource of Entree and will direct every other resource. */
  private val cfnMap:Map[String,Array[String]] = Map (
    "email_address" -> Array("email_address","email","emailaddress")
  )

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

  def aggregateFiles(flist:Vector[String],s3bucket:S3Bucket) = flist.foreach( f => readFile(f,s3bucket))


  def readFile(fileName:String,s3Bucket: S3Bucket) = {
    println(s3Bucket.folderPath.getOrElse("") + fileName)
    val input = DtlS3Cook.apply.getFileStream(s3Bucket.bucket,s3Bucket.folderPath.getOrElse("") + fileName)
    val reader = new BufferedReader(new InputStreamReader(input))
    val fileString = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString(",")
    val vectorString = fileString.split(",").toVector
    vectorString.map { s => toJson(s) match {
      case None => println("None")
      case Some(j) => println(j.asObject.get.toMap) //this is now a map TODO:continue here

    }}

    reader.close()
  }
  
}
