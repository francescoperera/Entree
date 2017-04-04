import com.typesafe.config.ConfigFactory

trait ConfigReader {
  val conf = ConfigFactory.load()
//  val rpf = conf.getInt("local.ROWS_PER_FILE")
//  val dfSchema = conf.getObject("local.DATA_FORMAT").unwrapped().asInstanceOf[java.util.Map[String,String]]
}
