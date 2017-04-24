import io.circe.JsonObject

object FilteringCook extends ConfigReader {
  /**
    * filterDataFormat takes a vector of dataFormat objects and filters out any object
    * that returns true to either the "isDataInvalid" or the "isDataEmpty" methods.
    * @param dfv - vector of dataFormat objects
    * @return - vector of dataFormat objects.
    */
  def filterDataFormat(dfv:Vector[JsonObject]): Vector[JsonObject] = {

    def isDataInvalid(d:Option[String]): Boolean = d.getOrElse("").toLowerCase() match {
      case "na"| "n/a"|"n/d"|"none"|""|"[redacted]"|"unfilled"|"address redacted"|"redacted address"|"redacted"|
           "unknown"|"null"|"no registra"|"no informa"|"no reporta"|"no aporta"|"no tiene"| "no"=> true
      case _ => false
    }

    def isDataEmpty (d:Option[String]) : Boolean = d.getOrElse("").trim().isEmpty //true if d.get is only whitespace i.e "   "

    def filterData[A](a:A,f1: A=>Boolean,f2: A => Boolean) : Boolean = f1(a) || f2(a)
    //TODO: Expand this to handle a list of functions

    //def filterData[A](a:A):Boolean = {
    // val lfn: List[A => Boolean] = List(isDataInvalid _, isDataEmpty _) //lfn = List of Functions
    // val fr : Boolean = lfn.map ( f => f(a)) fr = filter results
    // fr.find(_ == true) match {
    // case Some(bool) => bool
    // case None => false
    // }
    // }
    //TODO: Use for loop here

    val dataKeyName: String =  getKeyName(Actions.value)
    dfv.filterNot(df =>
      filterData(df.apply(dataKeyName).get.asString,isDataInvalid _, isDataEmpty _))
    //.get here is reasonable bc you if you dataKeyName then, there is a key in df with that name.
  }
}
