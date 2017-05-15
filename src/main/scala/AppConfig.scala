import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  // HOCON Config for tokens etc..
  val mode: String = Mode.dev
  val conf: Config = ConfigFactory.load().getConfig(mode)
  val S3ClientID: String = conf.getString("aws.s3.clientId")
  val S3ClientSecret: String = conf.getString("aws.s3.clientSecret")
}

object Mode {
  val test = "test"
  val dev = "dev"
}
