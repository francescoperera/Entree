import awscala.File
import awscala.s3.{Bucket, S3}
import com.amazonaws.services.s3.model.{ObjectMetadata, S3ObjectInputStream}

case class S3Bucket(bucket:String,folderPath:Option[String])

class S3Cook(val accessKeyId: String, val secretAccessKey: String){

  /** S3Cook is a class that acts as the interface between Entree and S3. It delineates all needs/methods to
    * get, explore or push files to S3*/

  implicit val region = awscala.Region.US_EAST_1
  implicit val s3 = S3(accessKeyId, secretAccessKey)

  /**
    *  list all the objects/keys present in a bucket
    * @param bucketName - name of the bucket
    * @return - vector of objects in the bucket
    */
  def listFiles(bucketName:String) : Vector[String] = s3.objectSummaries(Bucket(bucketName)).map(_.getKey).toVector

  /**
    * Given an S3 url, it extracts the bucket name and folder path to create an S3Bucket object.
    * @param url - valid S3 url .i.e  https://amazonaws.com/bucket/folder1/folder2
    * @return - s3Bucket object detailing bucket and folder path
    */
  def createS3Bucket(url:String) : S3Bucket =  {
    val uv:Vector[String] = url.split("/").toVector //uv = url vector
    val bi : Int = uv.indexWhere(_.contains(".com")) + 1 //bi = bucket index, it comes after s3.amazonaws.com/bucket/...
    val folders :String = uv.takeRight(uv.size - bi-1).mkString("/")
    val fp : Option[String] = folders.isEmpty match {
      case true => Some("")
      case false => Some(uv.takeRight(uv.size - bi-1).mkString("/") + "/")
    } //fp = folder path
    S3Bucket(uv(bi),fp)
  }

  /**
    * Given a bucket name and file name, it retrieves an input stream
    * @param b - bucket name
    * @param f - filename
    * @return - S3 file input stream
    */
  def getFileStream(b: String, f: String): S3ObjectInputStream = s3.getObject(b, f).getObjectContent

  /**
    * Takesa bucket name, key/file name, file content as Array of Bytes.It saves the file content in the specified bucket, following the path
    * determined by the key.
    * @param b - bucket name where to save.
    * @param key - path/ file name where to save the contents to
    * @param file - array of bytes carrying the file contents to be saved
    */
  def saveFile(b: String, key: String, file:Array[Byte]) ={
    //println(file.length)
    val metadata = new ObjectMetadata()
    metadata.setContentLength(file.length.toLong)
    //println(metadata.getContentLength)
    s3.put(Bucket(b), key, file, metadata)}

  /**
    * Takesa bucket name, folder path , and a file .It saves the file content in the specified bucket, following the path
    * determined by the folderPath
    * @param bucketName - bucket name where to save the file
    * @param folderPath - folder path to follow and ultimately save the file in.
    * @param file - file to be saved
    */
  def saveFile(bucketName:String,folderPath:String,file: File) = {
    val path = folderPath + file.getName
    s3.put(Bucket(bucketName), path, file)
  }
}

object DtlS3Cook {
  /** DtlS3Cook is S3Cook that only works for Datalogue and uses Datalogue creds. */
//  private val accessKeyId = sys.env.getOrElse("S3_KEY_ID", "AKIAJZUC55ZOLZSRM7RQ")
//  private val secretAccessKey = sys.env.getOrElse("S3_KEY_SECRET", "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")
  private val accessKeyId = sys.env.getOrElse("S3_KEY_ID", "AKIAJMVPH47YHSLXVXTA") //test
  private val secretAccessKey = sys.env.getOrElse("S3_KEY_SECRET", "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") //test

  implicit val region = awscala.Region.US_EAST_1
  implicit val s3 = S3(accessKeyId, secretAccessKey)

  def apply = new S3Cook(accessKeyId,secretAccessKey)

}
