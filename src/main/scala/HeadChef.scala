import java.io._

import com.typesafe.config.{ConfigObject, ConfigRenderOptions}
import io.circe._
import io.circe.syntax._

import scala.collection.JavaConverters._
import collection.JavaConverters._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable


//case class DataFormat(data:Option[String], label:Option[String],
//                      column_header:Option[String], column_description:String)
case class ValidNDJSONFile(filename:String, source:S3Bucket, valid:Boolean) //TODO: susbtitute valid with type field ( i.e type: JSON, CSV)
case class KeyAndAction(key:String, action: String)
case class FieldAndMap(field: String, bdm: Map[String, Properties])

object FieldAndMap{
  def apply(tp: (String, Map[String, Properties])) : FieldAndMap = FieldAndMap(tp._1,tp._2)
}
//
//object  DataFormat{
//  implicit val encoder: Encoder[DataFormat] = io.circe.generic.semiauto.deriveEncoder
//  implicit val decoder: Decoder[DataFormat] = io.circe.generic.semiauto.deriveDecoder
//}


object HeadChef extends JsonConverter with LazyLogging with ConfigReader {
  /** HeadChef is the main resource of Entree and will direct every other resource. */

    private val defaultRPF: Int = 100000

  private val rowsPerFile: Int = userInputRPF match {
    case None => defaultRPF//default value if no value is found in the config file
    case Some(num) => num.toInt.getOrElse(defaultRPF)
  }//conf.getInt("local.ROWS_PER_FILE")//Important: this value determines the size of the output files.

  /**
    * takes the source S3 bucket and gets all the filenames according to the label. It then aggregates all the files.
    *
    * @param source -  input S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @param destination - output S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @param label - greater ontology/column field name for which data needs to be aggregated. Could be "all" vs "specific_label"
    */
  def getFilesWithLabel(source:S3Bucket,destination:S3Bucket,label:String) = {
    logger.info(s"Getting files from S3 Bucket: ${source.bucket}")
    logger.info(s"Following Folder Path : ${source.folderPath.getOrElse("")}")
    val files = DtlS3Cook.apply.listFiles(source.bucket).filterNot( fp => fp.endsWith("/")).filter(_.contains(source.folderPath.getOrElse("")))
    val fileNames = files.map(_.split("/").last)
    label match {
      case "all" =>
        aggregateFiles(fileNames,source,destination,"all")
      case _ =>
        println(label) //TODO: this case needs to be looked at more carefully, filtering files based on content not filename
        val lf = fileNames.filterNot(CFNMappingCook.isValPresentWithKey(label,_)) // lf = labeled files or files whose name are under the designated lf //TODO: filter based on content not file name
        aggregateFiles(lf,source,destination,label)
    }
  }

  /**
    * Takes a vector of filesnames, the source S3bucket, destination S3Bucket and the label for which data needs to be aggregated for.
    * It checks whether each file is a valid NDJSON file or not and creates a validNDSJON object. The vector of NDSJON objects is then
    * used to read the files and aggregate all the data based on the input label. The vector of dataFormat objects it is then passed on
    * and saved to S3.
    *
    * @param fv - vector of filenames
    * @param source - input S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @param destination - output S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @param label - greater ontology/column field name for which data needs to be aggregated. Could be "all" vs "specific_label"
    */
  def aggregateFiles(fv:Vector[String],source:S3Bucket,destination:S3Bucket,label:String) = {
    //for each f in flist, create a validNDJSON object
    //depending on the valid type in validNDSJON object read normally or just call toNDJSON and then do what you do now
    val ndjson = fv.map(f => isNDJSON(f,source))
    logger.info(s"Aggregating data from ${fv.length} files")
    val jsonVec : Vector[Json] = ndjson.flatMap( j => readFile(j)).flatten // flatMap flattens the Options and flatten turns Vec[Vec] into just Vec. Does it makes sense?
    logger.info(s"Entree detected ${jsonVec.size} possible objects")
    val dataFormatVec  = jsonVec.flatMap(createDataFormat(_)).flatten //Vector[JsonObject]
//    logger.info(s"${dataFormatVec.size} dataFormat objects were created out of ${jsonVec.size} Json objects")
//    val filteredDataFormatVec : Vector[JsonObject]  = filterDataFormat(dataFormatVec)
//    logger.info(s"The vector dataFormat objects was sized down to ${filteredDataFormatVec.size}")
//    val unknownsDataFormat = createUnknowns(filteredDataFormatVec)
//    logger.info(s"Create vector of ${unknownsDataFormat.size} unknown data format objects")
//    val dataModels = (filteredDataFormatVec ++ unknownsDataFormat).map(_.asJson.noSpaces)
//    logger.info(s"Saving ${dataModels.size}  data format objects")
//    logger.info(s"Saving to Bucket: ${destination.bucket}, Path:${destination.folderPath}")
//    batchSave(dataModels,destination,label)
  }

  /**
    * Takes a filename and its source S3 bucket, reads the first line of the file and determines whether the file has a NDJSON or just
    * regular JSON. It then creates a validNDSJONFile object for the file
    *
    * @param f - filename
    * @param source - input S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @return - validNDSJONFile object. Look at HeadChef for validNDSJONFile implementation
    */
  def isNDJSON(f:String,source:S3Bucket): ValidNDJSONFile = {
    val input = DtlS3Cook.apply.getFileStream(source.bucket,source.folderPath.getOrElse("") + f)
    val reader = new BufferedReader(new InputStreamReader(input))
    val fileString = Stream.continually(reader.readLine()).take(1).mkString("")
    reader.close()
    val isValid = fileString.startsWith("{") && fileString.endsWith("}") //NDJSON object start with { and end with } on the same line
    ValidNDJSONFile(f,source,isValid)
  }

  /**
    * readFile takes a validNDJSONFile object streams the content of the file in the object,
    * and maps to a Vector of Json. Conversion to JSON differs between and NDJSON and JSON.
    *
    * @param vnf - validNDSJONFile object. See HeadChef for implementation
    * @return - Optional vector of Json where Json represents an object in the file.
    */
  def readFile(vnf:ValidNDJSONFile):Option[Vector[Json]] = {
    logger.info(s"Reading file - ${vnf.filename}")
    val input = DtlS3Cook.apply.getFileStream(vnf.source.bucket,vnf.source.folderPath.getOrElse("") + vnf.filename)
    val reader = new BufferedReader(new InputStreamReader(input))
    vnf.valid match {
        case true =>
          // reading NDJSON
          val fileVec = Stream.continually(reader.readLine()).takeWhile(_ != null).toVector //.mkString("")
          reader.close()
          Some(fileVec.map(toJson(_)))
        case false =>
          // reading JSON
          val fileString = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("")
          reader.close()
          toJson(fileString).asArray
    }
  }

  /**
    * createDataFormat takes Json casts it as a JsonObject and traverses its keys,creating
    * a data format object for each key (if the key has a label).
    * @param j - Json
    * @return Optional vector of dataFormat objects.
    */
  def createDataFormat (j: Json)  = { //Option[Vector[JsonObject]]
    j.asObject match {
      case None => None
      case Some(obj) =>
        val keys = obj.fields
        val dfv: Vector[Option[JsonObject]] = keys.map{k => //dfv = data format vector
          if (CFNMappingCook.isLabelWithKeyPresent(k)) {
            createDataObject(obj,k)
          } else {
            None
          }
        }
        Some(dfv.flatten)
    }
  }



  def createDataObject(obj:JsonObject,k:String):Option[JsonObject] = {
    val label: String = CFNMappingCook.getKeyFromVal(k)
    userInputDF match {
      case None =>
        logger.error(s"user-input.json was not properly formatted. Check docs for proper formatting")
        //TODO: move this log.error in ConfigReader if there is a Decoding Failure
        None
      case Some(df) =>
        val dataObject: Map[String, Json] = df.map{case (key,properties) =>
          getKeyValuePair(properties,key,Some(label),Some(obj),Some(k),None)
        }
        dataObject.asJson.asObject
    }
  }

  def getKeyValuePair(p: Properties, dfKey: String, label: Option[String],
                      obj: Option[JsonObject], k: Option[String], compositeField:Option[String]) : (String,Json) = {
    p.action match {
      case Actions.value =>
        obj match {
          case None => dfKey -> Json.Null
          case Some(o) =>
            val dataVal = Some(o.apply(k.getOrElse("")).getOrElse(Json.Null).asString.getOrElse("").trim)
            // More nested pattern matching and eliminate the "" with the getOrElse ?
            dfKey -> dataVal.asJson
        }
      case Actions.label => dfKey -> label.asJson
      case Actions.column => dfKey -> k.asJson
      case Actions.description =>
        obj match {
          case None => dfKey -> Json.Null
          case Some(o) =>
            val colDesc: String = o.apply("column_description").getOrElse(Json.Null).asString.getOrElse("")
            // More nested pattern matching and eliminate the "" with the getOrElse ?
            dfKey -> Some(colDesc).asJson
        }
      case Actions.subLabel => dfKey -> compositeField.asJson
      case Actions.emptyValue => dfKey -> "".asJson
      case Actions.decomposition => getBreakdown(dfKey, p, label.getOrElse(""))
      //ok to use getOrElse here bc label is checked in BreakdownMap, if not present nothing happens.
      case _ => dfKey -> Json.Null
    }
  }

  /**
    * createDataObjects takes the jsonObject and a key and creates a data format object
    * for the key. It then casts the object as Json.
    * @param obj - Json Object
    * @param k - key for which an object should be created
    * @return - Option[Json], where Json represents the data format object.
    */
//  def createDataObject(obj: JsonObject, k: String): Option[JsonObject] = {
//    val label: String = CFNMappingCook.getKeyFromVal(k)
//    userInputDF match {
//      case None =>
//        logger.error(s"user-input.json was not properly formatted. Check docs for proper formatting")
//        None
//      case Some(ui) =>
//        val uiMap: Map[String,Json] = ui.toMap
//        val dataObject: Map[String, Json] = uiMap.map{case (key,keyMap) =>
//            val kv: (String, Json) = keyMap.asObject match {
//              case None => key -> Json.Null
//              case Some(km) =>
//                val action: Option[String] = km.apply("action").get.asString //check get here, assuming action is always present
//                val kvPair: (String,Json) =  action match {
//                  case Some("value") =>
//                    val dataVal = Some(obj.apply(k).getOrElse(Json.Null).asString.getOrElse("").trim)
//                    key -> dataVal.asJson
//                  case Some("label") => key -> Some(label).asJson
//                  case Some("column") => key -> Some(k).asJson
//                  case Some("description") =>
//                    val colDesc: String = obj.apply("column_description").getOrElse(Json.Null).asString.getOrElse("")
//                    key -> Some(colDesc).asJson
//                  case Some("decomposition") =>
//                     getBreakdown(key,keyMap,label)
//                  case _ =>
//                    logger.error(s" $action does not match known actions")
//                    key ->  Json.Null
//                }
//                kvPair
//            }
//            kv
//        }
//        println(dataObject.asJson) //TODO: Remove this
//        dataObject.asJson.asObject
//    }
//  }



//  //TODO: Refactor this
//  def getBreakdown(key: String, keyMap: Json, label: String): (String, Json) = {
//    if (BreakdownCook.isKeyPresent(label)) {
//      //use get here because when you call this function, keyMap has already been casted as JsonObject successfully
//      val components: Option[Vector[Json]] = keyMap.asObject.get.apply("components").get.asArray
//      val breakdownSchema: Option[JsonObject] = components.get(0).asObject //Assumption: Vector is not empty and only has 1 element
//      val breakdown: Json = breakdownSchema match {
//        case None => Json.Null
//        case Some(bds) =>
//          val fields : Vector[String] = BreakdownCook.getCompositeFields(label)
//          val bdMap: Map[String, Json] = bds.toMap
//          val bdMapVector: Vector[Map[String, Json]] = Vector.fill(fields.size)(bdMap)
//          val fbdComposition: Vector[(String, Map[String, Json])] = fields.zip(bdMapVector)
//          val bd: Vector[Json] = fbdComposition.map {composite =>
//            val field: String = composite._1
//            val m: Map[String,Json] = composite._2
//            val newM:Map[String,Json] = m.map{case(bdk,bdv) =>
//                val newKV: (String, Json) = bdv.asObject match{
//                  case None => bdk -> Json.Null
//                  case Some(properties) =>
//                    val action: Option[String] = properties.apply("action").get.asString
//                    val kv: (String,Json) = action match {
//                      case Some("sub_label") => bdk -> field.asJson
//                      case _ => bdk -> "".asJson
//                    }
//                    kv
//                }
//                newKV //the new key-value pair that will substitute bdk,ddv
//            }
//            newM.asJson
//          }
//          bd.asJson
//      }
//      key -> breakdown
//    } else {
//      key -> Vector[Map[String, String]]().asJson
//    }
//  }

  def getBreakdown(dfKey:String, p: Properties,label: String): (String, Json) = {
    if(BreakdownCook.isKeyPresent(label)){
      val bdSchema: Map[String, Properties] = p.breakdown_schema.get //assumption: if you are doing a breakdown, then p.breakdown_schema needs to be defined
      val bdFields: Vector[String] = BreakdownCook.getCompositeFields(label) //the length of this vector dictates how many sub labels will be present in the breakdown
      val bdMapVector: Vector[Map[String, Properties]] = Vector.fill(bdFields.size)(bdSchema)
      val bdComposition: Vector[FieldAndMap] = bdFields.zip(bdMapVector) map{ (tp:(String, Map[String, Properties])) => FieldAndMap(tp) }
      val breakdown: Json = bdComposition.map{ fam =>
        val updatedBDMap: Map[String, Json] = fam.bdm.map { case(bdk,bdp) => getKeyValuePair(bdp, bdk,None,None,None,Some(fam.field))
        }
        updatedBDMap.asJson
      }.asJson
      dfKey -> breakdown
    }else{
      dfKey -> Vector[Map[String,String]]().asJson
    }
  }
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
    def filterData[A](a:A,f1: A=>Boolean,f2: A => Boolean) : Boolean = f1(a) || f2(a) //TODO: Expand this to handle a list of functions
    //def filterData[A](a:A):Boolean = {
    // val lfn: List[A => Boolean] = List(isDataInvalid _, isDataEmpty _) //lfn = List of Functions
    // val fr : Boolean = lfn.map ( f => f(a)) fr = filter results 
    // fr.find(_ == true) match {
    // case Some(bool) => bool
    // case None => false
    // }
    // }
    val dataKeyName: String =  "" //getNameForDataKey()
    dfv.filterNot(df =>
      filterData(df.apply(dataKeyName).get.asString,isDataInvalid _, isDataEmpty _))
    //.get here is reasonable bc you if you dataKeyName then, there is a key in df with that name.
  }

  //TODO: re-look at implementation here.Too many assumptions are made here on what the data format will have.
//  def getNameForDataKey(): String = {
//    val df = userInputDF.get
//    val keys: Vector[String] = df.fields
//    val keyActionVec: Vector[KeyAndAction] = keys.map { k =>
//      val kp = df.apply(k).get.asObject.get //watch out for the .get here
//      val action: String = kp.apply("action").get.asString.get // an action should always exist and it be a String value
//      KeyAndAction(k,action)
//    }
//    val filteredKey: Vector[KeyAndAction] = keyActionVec.filter(ka => ka.action.contains("value"))
//    if ( filteredKey.size > 1){
//      logger.warn("Entree detected that the Data Format schema in user-input.json contains multiple" +
//        "keys with the action: value. The first key will be used in the filtering process.")
//      filteredKey.head.key
//    } else {
//      filteredKey.head.key
//    }
//  }

  //TODO: this almost a copy of create Data Object. Think of how to combine, if possible
//  def createUnknowns(dfv: Vector[JsonObject]): Vector[JsonObject] = {
//    val unknownsVector: Vector[Option[JsonObject]] = dfv.map {df =>
//      val unknownLabel: String = "unknown" // the label and column valus for unknowns is the same
//      val colDesc: String = "" //unknowns have empty column description.
//      val unknown: Option[JsonObject] = userInputDF match {
//        case None =>
//          logger.error(s"user-input.json was not properly formatted. Check docs for proper formatting")
//          None
//        case Some(ui) =>
//          val uiMap: Map[String,Json] = ui.toMap
//          val unknownObject: Map[String, Json] = uiMap.map{case (key,keyMap) =>
//            val kv: (String, Json) = keyMap.asObject match {
//              case None => key -> Json.Null
//              case Some(km) =>
//                val action: Option[String] = km.apply("action").get.asString //check get here, assuming action is always present
//                val kvPair: (String,Json) =  action match {
//                    case Some("value") =>
//                      val fn = util.Random.shuffle(UnknownCook.generators).head //fn = random function from Unknown.generators
//                      val unknownData = fn(df.apply(key).get.asString.get) //TODO: watch out for .get here
//                      key -> unknownData.asJson
//                    case Some("label") => key -> unknownLabel.asJson
//                    case Some("column") => key -> unknownLabel.asJson
//                    case Some("description") =>
//                      key -> colDesc.asJson
//                    case Some("decomposition") =>
//                      key ->  Vector[Map[String,String]]().asJson // unknowns have no decomposition
//                    case _ =>
//                      logger.error(s" $action does not match known actions")
//                      key ->  Json.Null
//                }
//                kvPair
//            }
//            kv
//          }
//          println(unknownObject.asJson) //TODO: Remove this
//          unknownObject.asJson.asObject
//      }
//      unknown
//    }
//    unknownsVector.flatten
//  }

  /**
    * Takes a vector of strings and saves it to a File and then pushes the file to S3.
    *
    * @param v - vector of strings/ file content
    * @param dest - output/destination S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @param fname - filename used to save the file contents
    */
  def saveToS3(v:Vector[String],dest:S3Bucket,fname:String) : Unit = {
    val f = new File(s"$fname.json")
    val bw = new BufferedWriter(new FileWriter(f))
    v.foreach(s => bw.write( s + "\n"))
    bw.close()
//    val f = v.map(_.toByte).toArray  //TODO:Figure out to stream the content (v) back to S3.
    logger.info(s"Saving to S3:$fname")
    DtlS3Cook.apply.saveFile(dest.bucket,dest.folderPath.getOrElse(""),f)
  }

  /**
    * Takes all aggregated data in the form of a vector of strings and saves a batch of the strings at a time. The batch size
    * is determined by rowsPerFile.
    *
    * @param v
    * @param dest
    * @param label
    */
  def batchSave(v:Vector[String],dest:S3Bucket,label:String) : Unit = {
    val randomV: Vector[String] = util.Random.shuffle(v)
    val splitV = randomV.grouped(rowsPerFile).toVector.zipWithIndex
    splitV.foreach{ case (vec,idx) => saveToS3(vec,dest,label + "_" + idx.toString)}
  }
}
