import java.io._

import io.circe.{Json, JsonObject}
import io.circe.{Decoder, Encoder}
import io.circe.syntax._

import com.typesafe.scalalogging.LazyLogging


case class dataFormat(data:Option[String],label:Option[String],column_header:Option[String],column_description:String)
case class validNDJSONFile(filename:String,source:S3Bucket,valid:Boolean) //TODO: susbtitute valid with type field ( i.e type: JSON, CSV)

object  dataFormat{
  implicit val encoder: Encoder[dataFormat] = io.circe.generic.semiauto.deriveEncoder
  implicit val decoder: Decoder[dataFormat] = io.circe.generic.semiauto.deriveDecoder
}

object HeadChef extends JsonConverter with LazyLogging {
  /** HeadChef is the main resource of Entree and will direct every other resource. */

  private val rowsPerFile = 100000 // Important: this value determines the size of the output files.

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
        UnknownCook.wordSampler()
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
    val ndjson = fv.map(f => isNDJSON(f,source))
    logger.info(s"Aggregating data from ${fv.length} files")
    val jsonVec : Vector[Json] = ndjson.flatMap( j => readFile(j)).flatten // flatMap flattens the Options and flatten turns Vec[Vec] into just Vec. Does it makes sense?
    logger.info(s"Entree detected ${jsonVec.size} possible objects")
    val dataFormatVec : Vector[dataFormat] = jsonVec.flatMap(createDataFormat(_)).flatten
    logger.info(s"${dataFormatVec.size} dataFormat objects were created out of ${jsonVec.size} Json objects")
    val filteredDataFormatVec : Vector[dataFormat]  = filterDataFormat(dataFormatVec)
    logger.info(s" The vector dataFormat objects was sized down to ${filteredDataFormatVec.size}")
    val dataModels = filteredDataFormatVec.map(_.asJson.noSpaces)
    //Add unknowns here
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
    * @param j - Json
    * @return Optional vector of dataFormat objects.
    */
  def createDataFormat (j:Json): Option[Vector[dataFormat]] = {
    j.asObject match {
      case None => None
      case Some(obj) =>
        val keys = obj.fields
        val dfv = keys.map{k => //dfv = data format vector
          val colDesc = obj.apply("column_description").getOrElse(Json.Null).asString.getOrElse("")
          CFNMappingCook.isValPresent(k) match {
            case true =>
              val dataVal = Some(obj.apply(k).getOrElse(Json.Null).asString.getOrElse("").trim)
              Some(dataFormat(dataVal,Some(CFNMappingCook.getKeyFromVal(k)),Some(k),colDesc))
            case false => None
          }
        }
        Some(dfv.flatten)
    }
  }
  /**
    * filterDataFormat takes a vector of dataFormat objects and filters out any object
    * that returns true to either the "isDataInvalid" or the "isDataEmpty" methods.
    * @param mv - vector of dataFormat objects
    * @return - vector of dataFormat objects.
    */
  def filterDataFormat(mv:Vector[dataFormat]): Vector[dataFormat] = {
    def isDataInvalid(d:Option[String]): Boolean = d.getOrElse("").toLowerCase() match {
      case "na" => true
      case "n/a" => true
      case "n/d" => true
      case "none" => true
      case "" => true
      case "[redacted]" => true //address_socrata.json
      case "unfilled" => true //employee_socrata.json
      case "address redacted" => true //address_socrata.json
      case "redacted address" => true
      case "redacted" => true
      case "unknown" => true
      case "null" => true
      case "no registra" => true
      case "no informa" => true
      case "no reporta" => true
      case "no aporta" => true
      case "no tiene" => true
      case "no" => true
      case _ => false
    }
    def isDataEmpty (d:Option[String]) : Boolean = d.getOrElse("").trim().isEmpty //true if d.get is only whitespace i.e "   "
    def filterData[A](a:A,f1: A=>Boolean,f2: A => Boolean) : Boolean = f1(a) || f2(a) //TODO: Expand this to handle a list of functions
    mv.filterNot(od => filterData(od.data,isDataInvalid _, isDataEmpty _))
  }

//  def createUnknows(n:Int): Vector[dataFormat] = {
//    val l = List.range(0,n)
//    l.map()
//  }

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
    val randomV: Vector[String] = util.Random.shuffle(v)
    val splitV = randomV.grouped(rowsPerFile).toVector.zipWithIndex
    splitV.foreach{ case (vec,idx) => saveToS3(vec,dest,label + "_" + idx.toString)}
  }
}
