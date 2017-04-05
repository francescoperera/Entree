import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

object Waiter extends LazyLogging {
  /** Is the main interface of Entree and will collect orders from the client/user and deliver it back */

  def main(args:Array[String]) : Unit = {
    val s3Source : S3Bucket= DtlS3Cook.apply.createS3Bucket(args(0))
    val s3Dest : S3Bucket= DtlS3Cook.apply.createS3Bucket(args(1))
    val label = args(2)
    CFNMappingCook.cfnMap.keySet.contains(label) match {
      case false => logger.error(s"$label is an unrecognized label. Entree does not know it.")
      case true =>
        logger.info(s"Waiter is going to send your order -> Aggregate all files with label :$label")
        HeadChef.getFilesWithLabel(s3Source,s3Dest,label)
    }



  }

}
