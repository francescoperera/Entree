import java.io._
import java.nio.charset.StandardCharsets

import HeadChef.numFilesProcessed
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.typesafe.config.{ConfigObject, ConfigRenderOptions}
import io.circe._
import io.circe.syntax._

import scala.collection.JavaConverters._
import collection.JavaConverters._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

case class ValidNDJSONFile(filename:String, source:S3Bucket, valid:Boolean) //TODO: susbtitute valid with type field ( i.e type: JSON, CSV)
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

  private val numFilesProcessed = 3

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
        //aggregateFiles(fileNames,source,destination,"all")
        val filesPartition: Vector[Vector[String]] = Random.shuffle(fileNames).grouped(numFilesProcessed).toVector
        filesPartition.zipWithIndex.foreach( fv => downloadAndTransformFiles(fv._1,fv._2,source,destination))
      case _ =>
        println(label) //TODO: this case needs to be looked at more carefully, filtering files based on content not filename
        val lf = fileNames.filterNot(CFNMappingCook.isValPresentWithKey(label,_)) // lf = labeled files or files whose name are under the designated lf //TODO: filter based on content not file name
        //aggregateFiles(lf,source,destination,label)
    }
  }

  /**
    * Takes the vector of s3 file names,streams their content and writes them to temporary files in tmp directory.
    * Readers for the tmp files are transformed to iterators. Iterators are chosen at random and a line(object)
    * is extracted and transformed in a standard data format object. This object is written to a file
    * in the output directory. Once all iterators are done , the file in the output directory
    * @param fnv
    * @param idx
    * @param source
    * @param dest
    * @return
    */
  def downloadAndTransformFiles(fnv: Vector[String],idx:Int,source:S3Bucket,dest: S3Bucket) = {
    // save all fnv files from source into the tmp folder
    fnv.foreach{fn =>
      val s3Stream: S3ObjectInputStream = DtlS3Cook.apply.getFileStream(source.bucket,
        source.folderPath.getOrElse("") + fn)
      val reader = new BufferedReader(new InputStreamReader(s3Stream,StandardCharsets.UTF_8))
      val f = new File(s"tmp/$fn")
      val writer = new BufferedWriter(new FileWriter(f))

      val sourceIterator: java.util.Iterator[String] = reader.lines().iterator()
      while (sourceIterator.hasNext){
        writer.write(sourceIterator.next())
        writer.newLine()
      }
      reader.close()
      writer.close()
    }

    //Read all files from tmp using iterators. Randomly pick lines in each file and create data objects from line.
    //Save to output folder and stream all files in output to S3.
    val outputFile = new File(s"output/all_$idx.json") //TODO: fix this
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile),StandardCharsets.UTF_8))
    val tmpFiles: Vector[File] = fnv.map( f => new File(s"tmp/$f"))
    val done: Vector[Boolean] = Vector.fill(tmpFiles.size)(false)
    val fileReaders: Vector[BufferedReader] = tmpFiles.map{ f =>
      new BufferedReader(new InputStreamReader(new FileInputStream(f),StandardCharsets.UTF_8 ))}
    val tmpIterators: Vector[java.util.Iterator[String]] = fileReaders.map( _.lines().iterator())
    while (done.contains(false)){
      val availableIdxs: Vector[Int] = done.zipWithIndex.filter(_._1 == false ).map(_._2)
      val idx: Int = Random.shuffle(availableIdxs).head
      val iter: java.util.Iterator[String] = tmpIterators(idx)
      if (iter.hasNext) {
        val obj: String = iter.next()
        val jo: Json = toJson(obj)
        val dataObjects: Vector[JsonObject] = createDataFormat(jo).filter(_.nonEmpty)
        val filteredDataObjects: Vector[JsonObject] = FilteringCook.filterDataFormat(dataObjects)
        val unknownObjects: Vector[JsonObject] = UnknownCook.createUnknowns(filteredDataObjects)
        val dataModels: Vector[String] = (filteredDataObjects ++ unknownObjects).map(_.asJson.noSpaces)
        //println(dataModels)
        dataModels.foreach(m => bw.write( m + "\n"))
      } else {
        done.updated(idx,true)
        fileReaders(idx).close()
      }
    }
    tmpFiles.foreach( f => f.delete())
    bw.close()
    DtlS3Cook.apply.saveFile(dest.bucket,dest.folderPath.getOrElse(""),outputFile)
    outputFile.delete()


  }

  /**
    * Takes a filename and its source S3 bucket, reads the first line of the file and determines whether the file has a NDJSON or just
    * regular JSON. It then creates a validNDSJONFile object for the file
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
  def createDataFormat (j: Json) : Vector[JsonObject] = {
    j.asObject match {
      case None => Vector[JsonObject]()
      case Some(obj) =>
        val keys = obj.fields
        val dfv: Vector[Option[JsonObject]] = keys.map{k => //dfv = data format vector
          if (CFNMappingCook.isLabelWithKeyPresent(k)) {
            val o = createDataObject(obj, k)//TODO: consider moving method body her
            o
          } else {
            None
          }
        }
        dfv.flatten
    }
  }

  /**
    * createDataObjects takes the jsonObject and a key and creates a data format object
    * for the key. It then casts the object as Json.
    * @param obj - Json Object
    * @param k - key for which an object should be created
    * @return - Option[Json], where Json represents the data format object.
    */
  def createDataObject(obj: JsonObject, k: String) : Option[JsonObject] = {
    userInputDF match {
      case None =>
        logger.error(s"user-input.json was not properly formatted. Check docs for proper formatting")
        //TODO: move this log.error in ConfigReader if there is a Decoding Failure
        None
      case Some(df) =>
        val dataObject: Map[String, Json] = df.map{case (key,properties) =>
          val dataVal: Option[String] = Some(obj.apply(k).getOrElse(Json.Null).asString.getOrElse("").trim)
          val colDesc: String = obj.apply("column_description").getOrElse(Json.Null).asString.getOrElse("")
          val label: String = CFNMappingCook.getKeyFromVal(k)
          getKeyValuePair(properties,key,dataVal,Some(label),colDesc,Some(obj),Some(k),None)
        }
        dataObject.asJson.asObject
    }
  }

  //TODO: re-order arguments! Too many args?
  def getKeyValuePair(p: Properties, dfKey: String, dataVal:Option[String], label: Option[String], colDesc: String,
                      obj: Option[JsonObject], k: Option[String], compositeField:Option[String]) : (String,Json) = {
    p.action match {
      case Actions.value => dfKey -> dataVal.asJson
      case Actions.label => dfKey -> label.asJson
      case Actions.column => dfKey -> k.asJson
      case Actions.description => dfKey -> colDesc.asJson
      case Actions.subLabel => dfKey -> compositeField.asJson
      case Actions.emptyValue => dfKey -> "".asJson
      case Actions.decomposition => getBreakdown(dfKey, p, label.getOrElse(""))
      //ok to use getOrElse here bc label is checked in BreakdownMap, if not present nothing happens.
      case _ => dfKey -> Json.Null
    }
  }

  def getBreakdown(dfKey:String, p: Properties,label: String): (String, Json) = {
    if(BreakdownCook.isKeyPresent(label)){
      val bdSchema: Map[String, Properties] = p.breakdown_schema.get //assumption: if you are doing a breakdown, then p.breakdown_schema needs to be defined
      val bdFields: Vector[String] = BreakdownCook.getCompositeFields(label) //the length of this vector dictates how many sub labels will be present in the breakdown
      val bdMapVector: Vector[Map[String, Properties]] = Vector.fill(bdFields.size)(bdSchema)
      val bdComposition: Vector[FieldAndMap] = bdFields.zip(bdMapVector) map{ (tp:(String, Map[String, Properties])) => FieldAndMap(tp) }
      val breakdown: Json = bdComposition.map{ fam =>
        val updatedBDMap: Map[String, Json] = fam.bdm.map { case(bdk,bdp) =>
          getKeyValuePair(bdp, bdk,None,None,"",None,None,Some(fam.field))
        }
        updatedBDMap.asJson
      }.asJson
      dfKey -> breakdown
    }else{
      dfKey -> Vector[Map[String,String]]().asJson
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
