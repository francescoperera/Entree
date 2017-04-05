import java.io._

import com.typesafe.config.ConfigObject
import io.circe.{Json, JsonObject}
import io.circe.{Decoder, Encoder}
import io.circe.syntax._

import scala.collection.JavaConversions.mapAsScalaMap
import collection.JavaConverters._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.concurrent.JavaConversions


case class dataFormat(data:Option[String],label:Option[String],column_header:Option[String],column_description:String)
case class validNDJSONFile(filename:String,source:S3Bucket,valid:Boolean) //TODO: susbtitute valid with type field ( i.e type: JSON, CSV)

object  dataFormat{
  implicit val encoder: Encoder[dataFormat] = io.circe.generic.semiauto.deriveEncoder
  implicit val decoder: Decoder[dataFormat] = io.circe.generic.semiauto.deriveDecoder
}

object HeadChef extends JsonConverter with LazyLogging with ConfigReader {
  /** HeadChef is the main resource of Entree and will direct every other resource. */

  private val rowsPerFile = conf.getInt("local.ROWS_PER_FILE")//Important: this value determines the size of the output files.

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
    val dataFormatVec = jsonVec.flatMap(createDataFormat(_)).flatten
//    logger.info(s"${dataFormatVec.size} dataFormat objects were created out of ${jsonVec.size} Json objects")
//    val filteredDataFormatVec : Vector[dataFormat]  = filterDataFormat(dataFormatVec)
//    logger.info(s"The vector dataFormat objects was sized down to ${filteredDataFormatVec.size}")
//    val unknownsDataFormat = createUnknows(filteredDataFormatVec)
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
  def isNDJSON(f:String,source:S3Bucket): validNDJSONFile = {
    val input = DtlS3Cook.apply.getFileStream(source.bucket,source.folderPath.getOrElse("") + f)
    val reader = new BufferedReader(new InputStreamReader(input))
    val fileString = Stream.continually(reader.readLine()).take(1).mkString("")
    reader.close()
    val isValid = fileString.startsWith("{") && fileString.endsWith("}") //NDJSON object start with { and end with } on the same line
    validNDJSONFile(f,source,isValid)
  }

  /**
    * readFile takes a validNDJSONFile object streams the content of the file in the object,
    * and maps to a Vector of Json. Conversion to JSON differs between and NDJSON and JSON.
    *
    * @param vnf - validNDSJONFile object. See HeadChef for implementation
    * @return - Optional vector of Json where Json represents an object in the file.
    */
  def readFile(vnf:validNDJSONFile):Option[Vector[Json]] = {
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
    * a dataFormat object for each key.
    *
    * @param j - Json
    * @return Optional vector of dataFormat objects.
    */
  def createDataFormat (j:Json) = {
    //
    //println(dfSchemaMap)
    j.asObject match {
      case None => None
      case Some(obj) =>
        val keys = obj.fields
        val dfv = keys.map{k => //dfv = data format vector
          if (CFNMappingCook.isValPresent(k)){
            createDataMap(obj,k)
          }else{
            None
          }
        }
        Some(dfv.flatten)
    }
  }

  def createDataMap(obj:JsonObject, k:String): Option[Json] = {

    val dfSchema: mutable.Map[String,AnyRef] = mapAsScalaMap(conf.getObject("local.DATA_FORMAT").unwrapped())
    val dfSchemaMap: Map[String,Map[String,AnyRef]] = dfSchema.toMap.map{case (schemaK,schemaV) => schemaK -> mapAsScalaMap(schemaV.asInstanceOf[java.util.HashMap[String,AnyRef]]).toMap}

    //Call label here?
    val label: String = CFNMappingCook.getKeyFromVal(k)


    //NEW , make this its own function
    //TODO: traverse config object for DATA_FORMAT
    //TODO: for each object based on action, do one action and get value
    //TODO: create new Map/Object/data format.
    //TODO: cast Map/object/thing as Json
    val dataMap: Map[String,Json]  = dfSchemaMap.map{case (key,keyMap) =>

      val action = keyMap.get("action").asInstanceOf[Option[String]]
      action.getOrElse("") match {
        case "value" =>
          val dataVal = Some(obj.apply(k).getOrElse(Json.Null).asString.getOrElse("").trim)
          key -> dataVal.asJson
        case "label" => key -> Some(label).asJson
        case "column" => key -> Some(k).asJson
        case "description" =>
          val colDesc = obj.apply("column_description").getOrElse(Json.Null).asString.getOrElse("")
          key -> Some(colDesc).asJson
        case "decomposition" =>

          BreakdownCook.isKeyPresent(label) match {
            case false => key -> Vector[Map[String,String]]().asJson
            case true =>
              /** Converting Object in config to HasMap and then to Map */

              // 1
              val parts = keyMap.getOrElse("components",new java.util.ArrayList[java.util.HashMap[String,AnyRef]]()).asInstanceOf[java.util.ArrayList[java.util.HashMap[String,AnyRef]]]
              val partsVec: Vector[java.util.HashMap[String,AnyRef]] = parts.asScala.toVector
              //2
              val scPartsVec: Vector[mutable.Map[String,AnyRef]] = partsVec.map(m => mapAsScalaMap(m)) //scPartsVec = scala Parts Vector

              //TODO:Clusterfucky code , refactor this
              // Map( key -> action) where key is the key inside components and action is the property for key.
              //TODO: Be careful of doing scPartsVec(0). What if that Vector is empty? Correct this!
              //3
              val partsMap: mutable.Map[String,Option[String]] = scPartsVec(0).map{case (pk,pv) =>
                pk -> mapAsScalaMap(pv.asInstanceOf[java.util.HashMap[String,AnyRef]]).get("action")
                  .asInstanceOf[Option[String]]}
              //4
              val fields : Vector[String] = BreakdownCook.getCompositeFields(label)
              /** ASSUMPTION HERE that since we are doing  decomposition and label is composed of elements, we
              can create N number of partsMap where N number of fields in composite fields */
              //5
              val partsMapVector: Vector[mutable.Map[String,Option[String]]] = Vector.fill(fields.size)(partsMap)
              //6
              val fieldsMapComposition: Vector[(String,mutable.Map[String,Option[String]])]= fields.zip(partsMapVector)
              //7
              val breakdown: Vector[Map[String,String]] = fieldsMapComposition.map{ composition =>
                val m : mutable.Map[String,Option[String]] = composition._2 //m = partsMap
                val field : String = composition._1 //field = composite field/element to be adde in m under the key that has action set as "sub_label"
                val newMap: mutable.Map[String,String] = m.map{case (npk,npv) =>
                  npv.getOrElse("") match {
                    case "sub_label" => npk -> field
                    case  _ => npk -> ""
                  }
                }
                newMap.toMap //this is what composition is "transformed to"
              }
              //8
              key -> breakdown.asJson
          }
        case _  =>
          logger.error(s" $action does not match known actions")
          key ->  Json.Null
      }
    }

    println(dataMap.asJson)
    println()

    Some(dataMap.asJson)

  }
  /**
    * filterDataFormat takes a vector of dataFormat objects and filters out any object
    * that returns true to either the "isDataInvalid" or the "isDataEmpty" methods.
    *
    * @param mv - vector of dataFormat objects
    * @return - vector of dataFormat objects.
    */
  def filterDataFormat(mv:Vector[dataFormat]): Vector[dataFormat] = {
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
    mv.filterNot(od => filterData(od.data,isDataInvalid _, isDataEmpty _))
  }

  def createUnknows(dfv: Vector[dataFormat]): Vector[dataFormat] = {
    dfv.map{ df =>
      val fn = util.Random.shuffle(UnknownCook.generators).head //fn = random function from Unknown.generators
      val unknownData = fn(df.data.getOrElse(""))
      dataFormat(Some(unknownData),Some("unknown"),Some("unknown"),"")
    }
  }

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
