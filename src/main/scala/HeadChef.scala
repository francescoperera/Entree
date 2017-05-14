import java.io._
import java.nio.charset.StandardCharsets

import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.typesafe.scalalogging.LazyLogging
import io.circe._
import io.circe.syntax._

import scala.collection.mutable.ListBuffer
import scala.util.Random
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
    case None => defaultRPF //default value if no value is found in the config file
    case Some(num) => num.toInt.getOrElse(defaultRPF)
  } //conf.getInt("local.ROWS_PER_FILE")//Important: this value determines the size of the output files.

  private val classSize: Int = userInputCS match {
    case None => defaultCS
    case Some(num) => num.toInt.getOrElse(defaultRPF)
  }

  private val numFilesProcessed = 3

  /**
    * takes the source S3 bucket and gets all the filenames according to the label. It then aggregates all the files.
    * @param source -  input S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @param destination - output S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @param label - greater ontology/column field name for which data needs to be aggregated. Could be "all" vs "specific_label"
    */
  def getFilesWithLabel(source: S3Bucket, destination: S3Bucket, label: String): Unit = {
    logger.info(s"Getting files from S3 Bucket: ${source.bucket}")
    logger.info(s"Following Folder Path : ${source.folderPath.getOrElse("")}")
    val files = DtlS3Cook.apply.listFiles(source.bucket).filterNot(fp => fp.endsWith("/")).filter(_.contains(source.folderPath.getOrElse("")))
    val fileNames = files.map(_.split("/").last)
    label match {
      case "all" =>
        //aggregateFiles(fileNames,source,destination,"all")
        val vfmd: Vector[FileMetaData] = fileNames.map(n => getFileMetaData(n, source))
        val filesPartition: Vector[Vector[FileMetaData]] = Random.shuffle(vfmd).grouped(numFilesProcessed).toVector
        filesPartition.zipWithIndex.foreach(fv => downloadAndTransformFiles(fv._1, fv._2, source, destination))
      case _ =>
        println(label) //TODO: this case needs to be looked at more carefully, filtering files based on content not filename
      val lf = fileNames.filterNot(CFNMappingCook.isValPresentWithKey(label, _)) // lf = labeled files or files whose name are under the designated lf //TODO: filter based on content not file name
      //aggregateFiles(lf,source,destination,label)
    }
  }

  /**
    * Takes a filename and its source S3 bucket, reads the first line of the file and determines whether the file has a NDJSON or just
    * regular JSON. It then creates a validNDSJONFile object for the file
    * @param f - filename
    * @param source - input S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @return - validNDSJONFile object. Look at HeadChef for validNDSJONFile implementation
    */
  def getFileMetaData(f: String, source: S3Bucket): FileMetaData = {
    val input = DtlS3Cook.apply.getFileStream(source.bucket, source.folderPath.getOrElse("") + f)
    val reader = new BufferedReader(new InputStreamReader(input))
    val fileString = Stream.continually(reader.readLine()).take(1).mkString("")
    reader.close()
    val ft: FileType = if (fileString.startsWith("{") && fileString.endsWith("}")) {
      NDJSON
    } else {
      JSON
    }
    FileMetaData(f, source, ft)
  }

  /**
    * Takes the vector of s3 file names,streams their content and writes them to temporary files in tmp directory.
    * Readers for the tmp files are transformed to iterators. Iterators are chosen at random and a line(object)
    * is extracted and transformed in a standard data format object. This object is written to a file
    * in the output directory. Once all iterators are done , the file in the output directory
    * @param fmdv
    * @param idx
    * @param source
    * @param dest
    * @return
    */
  def downloadAndTransformFiles(fmdv: Vector[FileMetaData], idx: Int, source: S3Bucket, dest: S3Bucket) = {
    // save all fnv files from source into the tmp folder
    logger.info(s"Downloading files : ${fmdv.map(_.filename).mkString(",")}")
    fmdv.foreach { fmd =>
      fmd.format match {
        case JSON =>
          val input = DtlS3Cook.apply.getFileStream(fmd.source.bucket, fmd.source.folderPath.getOrElse("") + fmd.filename)
          val reader = new BufferedReader(new InputStreamReader(input))
          val fileString: String = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("")
          reader.close()
          val jsonArray: Option[Vector[Json]] = toJson(fileString).asArray
          jsonArray match {
            case None =>
            case Some(arr) =>
              val f = new File(s"tmp/${fmd.filename}")
              val w = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(f), StandardCharsets.UTF_8))
              arr.foreach(obj => w.write(obj.noSpaces + "\n"))
              w.close()
          }
        case NDJSON =>
          val s3Stream: S3ObjectInputStream = DtlS3Cook.apply.getFileStream(source.bucket,
            source.folderPath.getOrElse("") + fmd.filename)
          val reader = new BufferedReader(new InputStreamReader(s3Stream, StandardCharsets.UTF_8))
          val f = new File(s"tmp/${fmd.filename}")
          val writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream(f), StandardCharsets.UTF_8))
          val sourceIterator: java.util.Iterator[String] = reader.lines().iterator()
          while (sourceIterator.hasNext) {
            writer.write(sourceIterator.next())
            writer.newLine()
          }
          reader.close()
          writer.close()
      }
    }
    logger.info("Saved all S3 files to tmp directory.")
    //Read all files from tmp using iterators. For each line in an iterator create a number of data format objects
    //Save to output folder and stream all files in output to S3.
    val outputFile = new File(s"output/all_$idx.json")
    val outputWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8))
    val tmpFiles: Vector[File] = fmdv.map(fmd => new File(s"tmp/${fmd.filename}"))
    val done: ListBuffer[Boolean] = ListBuffer.fill(tmpFiles.size)(false)
    val fileReaders: Vector[BufferedReader] = tmpFiles.map { f =>
      new BufferedReader(new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))
    }
    val tmpIterators: Vector[java.util.Iterator[String]] = fileReaders.map(_.lines().iterator())
    // randomly pick the next line from the iterators until all iterators have parsed all lines.
    logger.info(s"Writing line from tmp files to ${outputFile.getName}")
    while (done.contains(false)) {
      val availableIdxs: Vector[Int] = done.zipWithIndex.filter(_._1 == false).map(_._2).toVector
      val idx: Int = Random.shuffle(availableIdxs).head
      val iter: java.util.Iterator[String] = tmpIterators(idx)
      if (iter.hasNext) {
        val obj: String = iter.next()
        val jo: Json = toJson(obj)
        val optDataObjects:Option[Vector[JsonObject]] = createDataObjectVector(jo).filter(_.nonEmpty)
         optDataObjects match {
          case None =>
          case Some(dataObjects) =>
            val filteredDataObjects: Vector[JsonObject] = FilteringCook.filterDataFormat(dataObjects)
            val unknownObjects: Vector[JsonObject] = UnknownCook.createUnknownObjects(filteredDataObjects)
            val dataModels: Vector[String] = (filteredDataObjects ++ unknownObjects).map(_.asJson.noSpaces)
            dataModels.foreach(m => outputWriter.write(m + "\n"))
        }
      } else {
        done(idx) = true //TODO: add logging to notify when iterator in one file is done ?
      }
    }
    fileReaders.foreach(r => r.close())
    tmpFiles.foreach(f => f.delete())
    outputWriter.close()
    DtlS3Cook.apply.saveFile(dest.bucket, dest.folderPath.getOrElse(""), outputFile)
    logger.info(s"Uploaded ${outputFile.getName} (${outputFile.length() * 0.000001} MB) to S3")
    outputFile.delete()
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
              case HierarchicalLabel.fullName =>
                val fullNameVal: Vector[Json] = bdargs.flatMap( arg => obj.apply(arg))
                Some(fullNameVal.flatMap(n => n.asString).mkString(" "))
              case HierarchicalLabel.address =>
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

  /**
    * createDataObjects takes the jsonObject and a key and creates a data format object
    * for the key. It then casts the object as Json.
    * @param obj - Json Object
    * @param colName - key/column for which an object should be created
    * @return - Option[Json], where Json represents the data format object.
    */
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
          val colDesc: String = obj.apply("column_description").getOrElse(Json.Null).asString.getOrElse("")
          val label: Option[String] = aggLabel match {
            case None => Some(CFNMappingCook.getKeyFromVal(colName))
            case someLabel => someLabel
          }
          getKeyValuePair(properties, key, dataVal, label,colDesc,Some(colName),None, bd)
        }
        dataMap.asJson.asObject
    }
    dataObject
  }

  /**
    * Given a Property,p, getKeyValuePair generates a key-value pair depending on the action defined in the Property.
    *
    * @param p - Properties Object
    * @param dok - data object key(the key in the key-value pair that will be returned by the method)
    * @param dataVal - value from the source Json object
    * @param label - label or tag detected by looking at the original key from the source Json object
    * @param colDesc - column description if any is present in the source Json object
    * @param k - key( column) from source Json object for which you are creating the Data Object.
    * @param subLabel - if a label is hierarchical, it will have sub labels. This is a string.
    * @param bd - breakdown of hierarchical (labeled) data.
    * @return - Tuple ( key - value pair) that will compose the Data Object.
    */
  def getKeyValuePair(p: Properties, dok: String, dataVal: Option[String], label: Option[String], colDesc: String,
                      k: Option[String], subLabel: Option[String],bd:Option[Map[String,String]]): (String, Json) = {
    p.action match {
      case Actions.value => dok -> dataVal.asJson
      case Actions.label => dok -> label.asJson
      case Actions.column => dok -> k.asJson
      case Actions.description => dok -> colDesc.asJson
      case Actions.subLabel => dok -> subLabel.asJson
      case Actions.emptyValue => dok -> "".asJson
      case Actions.decomposition => getBreakdown(dok, p, label.getOrElse(""))
      //ok to use getOrElse here bc label is checked in BreakdownMap, if not present nothing happens.
      case Actions.sublabelList => dok -> BreakdownCook.getSubLabelList(label.getOrElse("")).asJson
      case Actions.breakdown => dok -> bd.asJson
      case _ => dok -> Json.Null
    }
  }

  /**
    * Given a Data Object key, the property and label for the object, this method creates  a breakdown of the label
    * into its sub components if they exist. I.e The label "full_name" can be broken down into "first_name",
    * "middle_name" and "last_name". This method returns this breakdown(Map) in a Tuple(key-value pair) with the
    * Data Object key.
    * @param dok   - Data Object key(the key in the key-value pair that will be returned by the method)
    * @param p     - Properties Object
    * @param label - label or tag detected by looking at the original key from the source Json object
    * @return Tuple ( key - value pair) with the breakdown of the label, if one exists. This pair will be part
    *         of the Data Object.
    */
  def getBreakdown(dok: String, p: Properties, label: String): (String, Json) = {
    if (BreakdownCook.isKeyPresent(label)) {
      val bdSchema: Map[String, Properties] = p.breakdown_schema.get //assumption: if you are doing a breakdown, then p.breakdown_schema needs to be defined
      val bdFields: Vector[String] = BreakdownCook.getCompositeFields(label) //the length of this vector dictates how many sub labels will be present in the breakdown
      val bdMapVector: Vector[Map[String, Properties]] = Vector.fill(bdFields.size)(bdSchema)
      val bdComposition: Vector[FieldAndMap] = bdFields.zip(bdMapVector) map { (tp: (String, Map[String, Properties])) => FieldAndMap(tp) }
      val breakdown: Json = bdComposition.map { fam =>
        val updatedBDMap: Map[String, Json] = fam.bdm.map { case (bdk, bdp) =>
          getKeyValuePair(bdp, bdk, None, None, "", None, Some(fam.field),None)
        }
        updatedBDMap.asJson
      }.asJson
      dok -> breakdown
    } else {
      dok -> Vector[Map[String, String]]().asJson
    }
  }
}





