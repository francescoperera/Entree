import awscala.File
import awscala.s3.{Bucket, S3}
import com.amazonaws.services.s3.model.{ObjectMetadata, S3ObjectInputStream}

case class S3Bucket(bucket:String,folderPath:Option[String])

class S3Cook(val accessKeyId: String, val secretAccessKey: String){

  implicit val region = awscala.Region.US_EAST_1
  implicit val s3 = S3(accessKeyId, secretAccessKey)



  def listFiles(bucketName:String) : Vector[String] = s3.objectSummaries(Bucket(bucketName)).map(_.getKey).toVector

  def createS3Bucket(url:String) : S3Bucket =  {
    val uv = url.split("/").toVector //uv = url vector
    val bi = uv.indexWhere(_.contains(".com")) + 1 //bi = bucket index, it comes after s3.amazonaws.com/bucket/...
    val fp = Some(uv.takeRight(uv.size - bi-1).mkString("/") + "/") //fp = file path
    S3Bucket(uv(bi),fp)
  }

  def getFileStream(b: String, f: String): S3ObjectInputStream = s3.getObject(b, f).getObjectContent

  def saveFile(b: String, key: String, file:Array[Byte]) = s3.put(Bucket(b), key, file, new ObjectMetadata())

  def saveFile(bucketName:String,folderPath:String,file: File) = {
    val path = folderPath + file.getName
    s3.put(Bucket(bucketName), path, file)
  }
}

object DtlS3Cook {
  /** S3Cook represents an S3Client that will interface with S3 and fetch the data in an S3 bucket */
//  private val accessKeyId = sys.env.getOrElse("S3_KEY_ID", "AKIAJZUC55ZOLZSRM7RQ")
//  private val secretAccessKey = sys.env.getOrElse("S3_KEY_SECRET", "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")
  private val accessKeyId = sys.env.getOrElse("S3_KEY_ID", "AKIAJMVPH47YHSLXVXTA") //dev
  private val secretAccessKey = sys.env.getOrElse("S3_KEY_SECRET", "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") //dev

  implicit val region = awscala.Region.US_EAST_1
  implicit val s3 = S3(accessKeyId, secretAccessKey)

  def apply = new S3Cook(accessKeyId,secretAccessKey)

}
