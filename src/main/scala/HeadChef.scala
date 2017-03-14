import java.io._

import io.circe.{Json, JsonObject}
import io.circe.{Decoder, Encoder}
import io.circe.syntax._

import com.typesafe.scalalogging.LazyLogging


case class dataFormat(data:Option[String],label:Option[String],originalLabel:Option[String]) //TODO:rename this. too generic
case class validNDJSONFile(filename:String,source:S3Bucket,valid:Boolean)

object  dataFormat{
  implicit val encoder: Encoder[dataFormat] = io.circe.generic.semiauto.deriveEncoder
  implicit val decoder: Decoder[dataFormat] = io.circe.generic.semiauto.deriveDecoder
}

object HeadChef extends JsonConverter with LazyLogging {
  /** HeadChef is the main resource of Entree and will direct every other resource. */

  private val rowsPerFile = 10000

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
        fileNames.foreach(toNDSJON(_,source))
        val lf = fileNames.filterNot(CFNMappingCook.isValPresentWithKey(label,_)) // lf = labeled files or files whose name are under the designated lf //TODO: filter based on content not file name
        aggregateFiles(lf,source,destination,label)
    }
  }

  /**
    * Takes a filename and its source S3 bucket, reads the first line of the file and determines whether the file has a NDSJON or just
    * regular JSON. It then creates a validNDSJONFile object for the file
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
    val ndjson = fv.map(f => isNDJSON(f,source))
    logger.info(s"Aggregating data from ${fv.length} files")
    val dataModels = ndjson.flatMap( jf => readFile(jf))
    logger.info(s"Saving to Bucket: ${destination.bucket}, Path:${destination.folderPath}")
    batchSave(dataModels,destination,label)

  }

  /**
    * Takes a validNDJSONFile object,checks whether the file it refers to is a valid NDJSON file. If yes, it just reads the file as vector of strings.
    * If not, it converts to NDJSON format and then casts the it as a vector of strings. Then, each string is transformed to a Map and then to
    * a dataFormat object. Lastly each dataFormat object is encoded into JSON , specifically NDJSON format.
    * it reads the file and converts
    * @param ndjson - validNDSJONFile object ( contains filename, source and valid fields). Look at HeadChef for implementation.
    * @return - vector of string, where each stringified dataFormat object
    */
  def readFile(ndjson:validNDJSONFile) : Vector[String] = {
//    logger.info(s" Reading file: $fileName")
    val vectorString : Vector[String] = ndjson.valid match {
      case true =>readNDSJONFile(ndjson.filename,ndjson.source)
      case false => toNDSJON(ndjson.filename,ndjson.source)
    }
    val mo : Vector[Option[Map[String,Json]]] = vectorString.map( s => toJson(s) match {
      case None => Some(Map[String,Json]())
      case Some(j) =>Some(j.asObject.getOrElse(JsonObject.empty).toMap)
    }).filterNot(_.isEmpty) //each object per line was converted to a JSON object and then to a Map. Any empty objects or None were filtered out.
    val modelVector:Vector[Option[dataFormat]] = mo.flatMap{m => map2Model(m.get)}.filterNot(_.isEmpty) //.isEmpty filters None's.
    modelVector.map(_.get.asJson.noSpaces)
  }

  /**
    * Takes a filename and an S3bucket object pointing to the location of filename. It Streams the content of the S3 file and outputs
    * a vector of Strings
    * @param fileName - filename
    * @param s3Bucket - input S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @return - file content as a vector of strings
    */
  def readNDSJONFile(fileName:String,s3Bucket: S3Bucket):Vector[String] = {
    logger.info(s" Reading  NDJSON file: $fileName")
    val input = DtlS3Cook.apply.getFileStream(s3Bucket.bucket,s3Bucket.folderPath.getOrElse("") + fileName)
    val reader = new BufferedReader(new InputStreamReader(input))
    val fileString = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString(",")
    reader.close()
    fileString.split(",").toVector
  }

  /**
    * Takes a filename and its source S3 bucket and converts it into NDJSON( newline delimited JSON) and casts it as a Vector of strings, where
    * each string is the NDJSON object.
    * @param f - filename
    * @param source - input S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @return - vector of strings ( where each string is a NDJSON object)
    */
  def toNDSJON(f:String,source:S3Bucket):Vector[String] = {
    logger.info(s" Reading NON-NDJSON file: $f")
    val input = DtlS3Cook.apply.getFileStream(source.bucket,source.folderPath.getOrElse("") + f)
    val reader = new BufferedReader(new InputStreamReader(input))
    val fileString = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("")//.mkString(",") //only taking 5 at this stage
    reader.close()
    val fileJS = toJson(fileString) match {
      case None => None
      case Some(j) => j.asArray
    }
    fileJS.get.map(_.noSpaces)
  }

  /**
    *  Converts a Map to an Optional dataFormat object. It maps through each key-value pair and if the key is present in the column field name
    *  Map (specified in CFNMappingCook), then the conversion happens/
    * @param m - Map
    * @return - vector of Option[dataFormat]
    */
  def map2Model ( m: Map[String,Json]) : Vector[Option[dataFormat]] = m.map{ case (k,v) =>
      CFNMappingCook.isValPresent(k) match {
        case true => Some(dataFormat(v.asString,Some(CFNMappingCook.getKeyFromVal(k)),Some(k)))
        case false =>
          logger.info(s" The key $k is not present in cfnMap. Look at CFNMappingCook.")
          None
      }
  }.toVector

  /**
    * Takes a vector of strings and saves it to a File and then pushes the file to S3.
    * @param v - vector of strings/ file content
    * @param dest - output/destination S3Bucket ( bucket and path folder). Look at S3Cook for S3Bucket implementation
    * @param fname - filename used to save the file contents
    */
  def saveToS3(v:Vector[String],dest:S3Bucket,fname:String) = {
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
  def batchSave(v:Vector[String],dest:S3Bucket,label:String) = {
    val splitIdx = Seq.range(1,v.length/rowsPerFile).toVector.map( _ * rowsPerFile)
    val splitV = v.grouped(rowsPerFile).toVector.zipWithIndex
    splitV.foreach{ case (vec,idx) => saveToS3(vec,dest,label + "_" + idx.toString)}
  }

}
