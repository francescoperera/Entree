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
        val vfmd: Vector[FileMetaData] = fileNames.map( n => getFileMetaData(n,source))
        val filesPartition: Vector[Vector[FileMetaData]] = Random.shuffle(vfmd).grouped(numFilesProcessed).toVector
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
    * @param fmdv
    * @param idx
    * @param source
    * @param dest
    * @return
    */
  def downloadAndTransformFiles(fmdv: Vector[FileMetaData],idx:Int,source:S3Bucket,dest: S3Bucket) = {
    // save all fnv files from source into the tmp folder
    logger.info(s"Downloading files : ${fmdv.map(_.filename).mkString(",")}")
    fmdv.foreach{fmd => fmd.format match {
      case JSON =>
        val input = DtlS3Cook.apply.getFileStream(fmd.source.bucket,fmd.source.folderPath.getOrElse("") + fmd.filename)
        val reader = new BufferedReader(new InputStreamReader(input))
        val fileString: String = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("")
        reader.close()
        val jsonArray: Option[Vector[Json]] = toJson(fileString).asArray
        jsonArray match {
          case None =>
          case Some(arr) =>
            val f = new File(s"tmp/${fmd.filename}")
            val w = new BufferedWriter(new OutputStreamWriter(
              new FileOutputStream(f),StandardCharsets.UTF_8))
            arr.foreach(obj => w.write( obj.noSpaces + "\n"))
            w.close()
        }
      case NDJSON =>
        val s3Stream: S3ObjectInputStream = DtlS3Cook.apply.getFileStream(source.bucket,
          source.folderPath.getOrElse("") + fmd.filename)
        val reader = new BufferedReader(new InputStreamReader(s3Stream,StandardCharsets.UTF_8))
        val f = new File(s"tmp/${fmd.filename}")
        val writer = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(f),StandardCharsets.UTF_8))
        val sourceIterator: java.util.Iterator[String] = reader.lines().iterator()
        while (sourceIterator.hasNext){
          writer.write(sourceIterator.next())
          writer.newLine()
        }
        reader.close()
        writer.close()
    }}

    logger.info("Saved all S3 files to tmp directory.")
    //Read all files from tmp using iterators. Randomly pick lines in each file and create data objects from line.
    //Save to output folder and stream all files in output to S3.
    val outputFile = new File(s"output/all_$idx.json")
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile),StandardCharsets.UTF_8))
    val tmpFiles: Vector[File] = fmdv.map( fmd => new File(s"tmp/${fmd.filename}"))
    val done: ListBuffer[Boolean] = ListBuffer.fill(tmpFiles.size)(false)
    val fileReaders: Vector[BufferedReader] = tmpFiles.map{ f =>
      new BufferedReader(new InputStreamReader(new FileInputStream(f),StandardCharsets.UTF_8 ))}
    val tmpIterators: Vector[java.util.Iterator[String]] = fileReaders.map( _.lines().iterator())
    // randomly pick the next line from the iterators until all iterators have parsed all lines.
    logger.info(s"Writing line from tmp files to ${outputFile.getName}")
    while (done.contains(false)){
      val availableIdxs: Vector[Int] = done.zipWithIndex.filter(_._1 == false ).map(_._2).toVector
      val idx: Int = Random.shuffle(availableIdxs).head
      val iter: java.util.Iterator[String] = tmpIterators(idx)
      if (iter.hasNext) {
        val obj: String = iter.next()
        val jo: Json = toJson(obj)
        val dataObjects: Vector[JsonObject] = createDataFormat(jo).filter(_.nonEmpty)
        val filteredDataObjects: Vector[JsonObject] = FilteringCook.filterDataFormat(dataObjects)
        val unknownObjects: Vector[JsonObject] = UnknownCook.createUnknowns(filteredDataObjects)
        val dataModels: Vector[String] = (filteredDataObjects ++ unknownObjects).map(_.asJson.noSpaces)
        dataModels.foreach(m => bw.write( m + "\n"))
      } else {
        done(idx) = true //TODO: add logging to notify when iterator in one file is done ?
      }
    }
    fileReaders.foreach( r => r.close())
    tmpFiles.foreach( f => f.delete())
    bw.close()
    DtlS3Cook.apply.saveFile(dest.bucket,dest.folderPath.getOrElse(""),outputFile)
    logger.info(s"Uploaded ${outputFile.getName} (${outputFile.length() * 0.000001} MB) to S3")
    outputFile.delete()


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


//  /**
//    * If the file is  JSON then read it, transform it to NDSJON and save it a temporary folder
//    * fileConversion
//    * @param fmd
//    */
//  def toNDJSON(fmd:FileMetaData) = {
//    fmd.format match {
//        case JSON =>
//          val input = DtlS3Cook.apply.getFileStream(fmd.source.bucket,fmd.source.folderPath.getOrElse("") + fmd.filename)
//          val reader = new BufferedReader(new InputStreamReader(input))
//          val fileString: String = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("")
//          reader.close()
//          val jsonArray: Option[Vector[Json]] = toJson(fileString).asArray
//          jsonArray match {
//            case None =>
//            case Some(arr) =>
//              val outputFile = new File(s"fileConversion/${fmd.filename}.json")
//              val bw = new BufferedWriter(new OutputStreamWriter(
//                new FileOutputStream(outputFile),StandardCharsets.UTF_8))
//              arr.foreach(obj => bw.write( obj.noSpaces + "\n"))
//              bw.close()
//          }
//        case _ =>
//    }
//  }

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
