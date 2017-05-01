import java.io._


import com.typesafe.config.{ConfigObject, ConfigRenderOptions}
import io.circe._
import io.circe.syntax._

import scala.collection.JavaConverters._
import collection.JavaConverters._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable

sealed trait FileType
case object JSON extends FileType
case object NDJSON extends FileType

case class FileMetaData(filename:String, source:S3Bucket, format: FileType)
case class KeyAndAction(key:String, action: String)
case class FieldAndMap(field: String, bdm: Map[String, Properties])

object FieldAndMap{
  def apply(tp: (String, Map[String, Properties])) : FieldAndMap = FieldAndMap(tp._1,tp._2)
}



object HeadChef extends JsonConverter with LazyLogging with ConfigReader {
  /** HeadChef is the main resource of Entree and will direct every other resource. */

  private val defaultRPF: Int = 100000

  private val defaultCS: Int = 10000

  private val rowsPerFile: Int = userInputRPF match {
    case None => defaultRPF//default value if no value is found in the config file
    case Some(num) => num.toInt.getOrElse(defaultRPF)
  }//conf.getInt("local.ROWS_PER_FILE")//Important: this value determines the size of the output files.

  private val classSize: Int = userInputCS match {
    case None => defaultCS
    case Some(num) => num.toInt.getOrElse(defaultRPF)
  }

  /**
    * takes the source S3 bucket and gets all the filenames according to the label. It then aggregates all the files.
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
    * @param fv - vector of filenames
    * @param source - input S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @param destination - output S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @param label - greater ontology/column field name for which data needs to be aggregated. Could be "all" vs "specific_label"
    */
  def aggregateFiles(fv:Vector[String],source:S3Bucket,destination:S3Bucket,label:String) = {
    //for each f in flist, create a validNDJSON object
    //depending on the valid type in validNDSJON object read normally or just call toNDJSON and then do what you do now
    val ndjson: Vector[FileMetaData] = fv.map(f => getFileMetaData(f,source))
    logger.info(s"Aggregating data from ${fv.length} files")
    val jsonVec : Vector[Json] = ndjson.flatMap( j => readFile(j)).flatten // flatMap flattens the Options and flatten turns Vec[Vec] into just Vec. Does it makes sense?
    logger.info(s"Entree detected ${jsonVec.size} possible objects")
    val dataFormatVec: Vector[JsonObject]  = jsonVec.flatMap(createDataObjectVector(_)).flatten



    logger.info(s"${dataFormatVec.size} dataFormat objects were created out of ${jsonVec.size} Json objects")
    val filteredDataFormatVec : Vector[JsonObject]  = FilteringCook.filterDataFormat(dataFormatVec)
    logger.info(s"The vector dataFormat objects was sized down to ${filteredDataFormatVec.size}")

//    val labelKey: String = getKeyName(Actions.label)
//    val chunkedDataFormat: Map[ Option[Json], Vector[JsonObject]] = filteredDataFormatVec.groupBy(df =>
//      df.apply(labelKey)
//    )
//    //For warning purposes
//    chunkedDataFormat.foreach {case (k,v)=>
//        if (v.size < classSize) {
//          val className: String = k.get.asString.get //TODO: is this good? .get?
//          logger.warn(s"$className has less than $classSize data points.")
//        }
//    }
//    //
//    val balancedDataClasses:Vector[JsonObject] = chunkedDataFormat.flatMap{case(k,v) =>
//        // rather shuffle than random indices because we don't know how many random elements we need and the size of v
//        val shuffledData: Vector[JsonObject] = util.Random.shuffle(v)
//        shuffledData.take(classSize)
//    }.toVector
//    val unknownsDataFormat = UnknownCook.createUnknowns(balancedDataClasses)
//    logger.info(s"Create vector of ${unknownsDataFormat.size} unknown data format objects")
//    val dataModels = (balancedDataClasses ++ unknownsDataFormat).map(_.asJson.noSpaces)
    val dataModels = filteredDataFormatVec.map(_.asJson.noSpaces)
    logger.info(s"Saving ${dataModels.size}  data format objects")
    logger.info(s"Saving to Bucket: ${destination.bucket}, Path:${destination.folderPath}")
    batchSave(dataModels,destination,label)
  }

  /**
    * Takes a filename and its source S3 bucket, reads the first line of the file and determines whether the file has a NDJSON or just
    * regular JSON. It then creates a validNDSJONFile object for the file
    * @param f - filename
    * @param source - input S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @return - validNDSJONFile object. Look at HeadChef for validNDSJONFile implementation
    */
  def getFileMetaData(f:String,source:S3Bucket): FileMetaData= {
    val input = DtlS3Cook.apply.getFileStream(source.bucket,source.folderPath.getOrElse("") + f)
    val reader = new BufferedReader(new InputStreamReader(input))
    val fileString = Stream.continually(reader.readLine()).take(1).mkString("")
    reader.close()
    val ft: FileType = if (fileString.startsWith("{") && fileString.endsWith("}")) {
      NDJSON
    } else {
      JSON
    }
    FileMetaData(f,source,ft)
  }

  /**
    * readFile takes a validNDJSONFile object streams the content of the file in the object,
    * and maps to a Vector of Json. Conversion to JSON differs between and NDJSON and JSON.
    * @param fmd - validNDSJONFile object. See HeadChef for implementation
    * @return - Optional vector of Json where Json represents an object in the file.
    */
  def readFile(fmd:FileMetaData):Option[Vector[Json]] = {
    logger.info(s"Reading file - ${fmd.filename}")
    val input = DtlS3Cook.apply.getFileStream(fmd.source.bucket,fmd.source.folderPath.getOrElse("") + fmd.filename)
    val reader = new BufferedReader(new InputStreamReader(input))
    val fileContent:Option[Vector[Json]] = fmd.format match {
        case NDJSON =>
          // reading NDJSON
          val fileVec = Stream.continually(reader.readLine()).takeWhile(_ != null).toVector //.mkString("")
          reader.close()
          Some(fileVec.map(toJson(_)))
        case JSON =>
          // reading JSON
          val fileString = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("")
          reader.close()
          toJson(fileString).asArray
    }
    fileContent
  }

  /**
    * createDataFormat takes Json casts it as a JsonObject and traverses its keys,creating
    * a data format object for each key (if the key has a label).
    * @param j - Json
    * @return Optional vector of dataFormat objects.
    */
  def createDataObjectVector (j: Json) : Option[Vector[JsonObject]] = {
    val dov:Option[Vector[JsonObject]] =  j.asObject match {
      case None => None
      case Some(obj) =>
        val keys: Vector[String] = obj.fields
        val objectBDLabels: Vector[String] = keys.flatMap( k => BreakdownCook.rbdMap.get(k))
        val objectBDLabelsSet: Set[String] = objectBDLabels.toSet
        /** if list of breakdown(BD) labels size is not the same as the set of BD labels, then multiple keys
          * have the same BD label. This is hierarchical data. Also, hierarchical objects are only created
          * if there is one BD label (objectBDLabelsSet.size == 1).
          * If more than one unique BD label is present, then object is hierarchically ambiguous,
          * so its going to be parsed key by key.*/
        val dataObjectVector:Option[Vector[JsonObject]] =
          if (objectBDLabels.size != objectBDLabelsSet.size & objectBDLabelsSet.size == 1 ) {
          // Hierarchical object gets created
          val bdl : String = objectBDLabelsSet.head //bdl = breakdown label
          val bdargs: Vector[String] = BreakdownCook.bdMap(bdl) // components of breakdown label
          val objValue:Option[String] = bdl match {
            //TODO: Fix.assuming risky because the breakdown label is set by the user name might not always be set as "full_name"
            case "full_name" =>
              val fullNameVal: Vector[Json] = bdargs.flatMap( arg => obj.apply(arg))
              Some(fullNameVal.flatMap(n => n.asString).mkString(" "))
            case "address" =>
              val addressVal: Vector[Json] = bdargs.flatMap( arg => obj.apply(arg))
              Some(addressVal.flatMap( av => av.asString).mkString(","))
            case _ =>  None
          } //Assumption: the order of elements in user-input.json is how full_name and address  will be composed
          val bdData: Map[String,String] = keys.map{k =>
            BreakdownCook.rbdMap.get(k) match {
              case Some(`bdl`) =>
                obj.apply(k) match {
                  case None => k -> None
                  case Some(v) => k -> v.asString
                }
              case _ => k -> None
            }
          }.toMap.filter( kv => kv._2.isDefined).map{case (k,v) => k -> v.get}

          // in the case of aggregate point, the original column is set as an empty String
          val colName: String = "" //TODO: substitute this with nulls.
          val dataObject: Option[JsonObject] = createDataObject(obj,colName,Some(bdl),objValue,Some(bdData))
          Some(Vector(dataObject).flatten)
        } else {
          // create a data object for each column/key.
          val dfv: Vector[Option[JsonObject]] = keys.map{ k => //dfv = data format vector
            if (CFNMappingCook.isLabelWithKeyPresent(k)) {
              createDataObject(obj, k)
            } else {
              None
            }
          }
          Some(dfv.flatten)
        }
        dataObjectVector
    }
    dov
  }

  def createDataObject(obj: JsonObject,
                       colName: String ,
                       aggLabel: Option[String] = None,
                       objVal: Option[String] = None,
                       bd: Option[Map[String, String]] = None): Option[JsonObject] = {
    val dataObject: Option[JsonObject] = userInputDF match {
      case None =>
        logger.error(s"user-input.json was not properly formatted. Check docs for proper formatting")
        //TODO: move this log.error in ConfigReader if there is a Decoding Failure
        None
      case Some(df) =>
        val dataMap: Map[String, Json] = df.map { case (key, properties) =>
          val dataVal = objVal match {
            case None => Some(obj.apply(colName).getOrElse(Json.Null).asString.getOrElse("").trim)
            case someVal => someVal
          }
          //val dataVal: Option[String] = Some(obj.apply(colName).getOrElse(Json.Null).asString.getOrElse("").trim)
          val colDesc: String = obj.apply("column_description").getOrElse(Json.Null).asString.getOrElse("")
          val label: Option[String] = aggLabel match {
            case None => Some(CFNMappingCook.getKeyFromVal(colName))
            case someLabel => someLabel
          }
          getKeyValuePair(properties, key, dataVal, label, Some(colName), colDesc, None, bd)
        }
        dataMap.asJson.asObject
    }
    dataObject
  }




  /** k = refers to column in original JsonObject*/
  //TODO: re-order arguments! Too many args? dataVal, colDesc etc.. can be generated inside the function.
  def getKeyValuePair(p: Properties, dfKey: String, dataVal: Option[String],label:Option[String],
                      colName: Option[String], colDesc:String, compositeField:Option[String],
                      bd:Option[Map[String,String]]) : (String,Json) = {

    val kvPair: (String,Json) = p.action match {
      case Actions.value => dfKey -> dataVal.asJson
      case Actions.label => dfKey -> label.asJson
      case Actions.column => dfKey -> colName.asJson
      case Actions.description => dfKey -> colDesc.asJson
      case Actions.subLabel => dfKey -> compositeField.asJson
      case Actions.emptyValue => dfKey -> "".asJson
      case Actions.decomposition => getBreakdown(dfKey, p, label.getOrElse(""))
      case Actions.sublabelList => dfKey -> BreakdownCook.getSubLabelList(label.getOrElse("")).asJson
      case Actions.breakdown => dfKey -> bd.asJson
      //ok to use getOrElse here bc label is checked in BreakdownMap, if not present nothing happens.
      case _ => dfKey -> Json.Null
    }
    kvPair
  }

  /**
    * takes the key that maps to the breakdown object, its properties and the label associated with the data point in question.
    * It checks if the label is present in the breakdown map and then uses the properties to create the breakdown object that
    * will be mapped to the key.
    * @param dfKey - key in the data format object that maps to the breakdown object
    * @param p - dfKey's properties
    * @param label - label associated with the data point that is getting transformed
    * @return - key value pair ( String,Json)
    */
  def getBreakdown(dfKey:String, p: Properties,label: String): (String, Json) = {
    if(BreakdownCook.isKeyPresent(label)){
      val bdSchema: Map[String, Properties] = p.breakdown_schema.get //assumption: if you are doing a breakdown, then p.breakdown_schema needs to be defined
      val bdFields: Vector[String] = BreakdownCook.getCompositeFields(label) //the length of this vector dictates how many sub labels will be present in the breakdown
      val bdMapVector: Vector[Map[String, Properties]] = Vector.fill(bdFields.size)(bdSchema)
      val bdComposition: Vector[FieldAndMap] = bdFields.zip(bdMapVector).map{
        (tp:(String, Map[String, Properties])) => FieldAndMap(tp)
      }
      val breakdown: Json = bdComposition.map{ fam =>
        val updatedBDMap: Map[String, Json] = fam.bdm.map{ case(bdk,bdp) =>
          getKeyValuePair(bdp, bdk,None,None,None,"",Some(fam.field),None)
        }
        updatedBDMap.asJson
      }.asJson
      dfKey -> breakdown
    } else {
      dfKey -> Vector[Map[String,String]]().asJson
    }
  }

  def createUnknownObjects(dfv: Vector[JsonObject]): Vector[JsonObject] = {
    val unknownsVector: Vector[Option[JsonObject]] = dfv.map {df =>
      val unknownLabel: Option[String] = Some("unknown") // the label and column values for unknowns is the same
      val colDesc: String = "" //unknowns have empty column description.
      val unknown: Option[JsonObject] = userInputDF match {
        case None =>
          logger.error(s"user-input.json was not properly formatted. Check docs for proper formatting")
          None
        case Some(ui) =>
          val unKnownMap: Map[String, Json] = ui.map{case (k,p) =>
          val fn: String => String = util.Random.shuffle(UnknownCook.generators).head
          val dkn : String = getKeyName(Actions.value) //dkn = data key name
          val unKnownVal: Option[String] = Some(fn(df.apply(dkn).getOrElse(Json.Null).asString.get))
            getKeyValuePair(p,k,unKnownVal,unknownLabel,None,colDesc,None,None)
          }
          unKnownMap.asJson.asObject
      }
      unknown
    }
    unknownsVector.flatten
  }

  /**
    * Takes a vector of strings and saves it to a File and then pushes the file to S3.
    * @param v - vector of strings/ file content
    * @param dest - output/destination S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @param fname - filename used to save the file contents
    */
  def saveToS3(v:Vector[String],dest:S3Bucket,fname:String) : Unit = {
    val f = new File(s"$fname.json")
    val bw = new BufferedWriter(new FileWriter(f))
    v.foreach(s => bw.write( s + "\n"))
    bw.close()
    //TODO:Figure out to stream the content (v) back to S3.
    logger.info(s"Saving to S3:$fname")
    DtlS3Cook.apply.saveFile(dest.bucket,dest.folderPath.getOrElse(""),f)
  }

  /**
    * Takes all aggregated data in the form of a vector of strings and saves a batch of the strings at a time. The batch size
    * is determined by rowsPerFile.
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
