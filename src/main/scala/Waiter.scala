
object Waiter {
  /** Is the main interface of Entree and will collect orders from the client/user and deliver it back */

  def main(args:Array[String]) : Unit = {
    val s3Source : S3Bucket= DtlS3Cook.apply.createS3Bucket(args(0))
    val s3Dest : S3Bucket= DtlS3Cook.apply.createS3Bucket(args(1))
    val label = args(2)
    HeadChef.getFilesWithLabel(s3Source,s3Dest,label)

  }

}
