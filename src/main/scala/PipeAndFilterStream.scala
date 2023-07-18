import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Merge, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, OverflowStrategy, ThrottleMode}
import akka.{Done, NotUsed}
import ujson.Obj

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

case class NpmPackage(pack: String)

case class Package(name: String, stars: Double, test: Double, downloads: Double, releaseFrequency: Double)

object PipeAndFilterStream extends App {


  implicit val actorSystem: ActorSystem = ActorSystem("Assignment_1-Akka_Streams")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = materializer.executionContext


  val packageList: ListBuffer[NpmPackage] = new ListBuffer[NpmPackage]()

  val packageStream = new GZIPInputStream(new FileInputStream(new File("src/main/resources/packages.txt.gz")))

  /*
  Each line in the compressed file contains the name of a package,
  so mapping each line in the file to an object. */
  for (pack <- io.Source.fromInputStream(packageStream).getLines()) {
    packageList += NpmPackage(pack)
  }

  //the packages are converted into a Source to start the streaming process
  val source: Source[NpmPackage, NotUsed] = Source(packageList.toList)

  //Stream buffered into source stream with a maximum capacity of 25 packages with a backpressure strategy.
  val sourceBuff: Source[NpmPackage, NotUsed] = source.buffer(25, OverflowStrategy.backpressure)

  val extractionProcess: Flow[NpmPackage, Package, NotUsed] = Flow[NpmPackage].map(item => {
    val packName = item.pack
    val packageContent = apiRequest(packName)
    val name = packageContent("collected")("metadata")("name").str
    val starsCount = packageContent("collected")("github")("starsCount").num
    val test = packageContent("evaluation")("quality")("tests").num
    val downloads = packageContent("evaluation")("popularity")("downloadsCount").num
    val releaseFrequency = packageContent("evaluation")("maintenance")("releasesFrequency").num
    Package(name, starsCount, test, downloads, releaseFrequency)
  })

  private val pipeline: Flow[Package, Package, NotUsed] = Flow[Package].filter(packData => {
    packData.stars > 20 &&
      packData.test > 0.5 &&
      packData.downloads > 100 &&
      packData.releaseFrequency > 0.2
  }
  )

  /*
  * Keeping the number of API requests under control and not to stress the NPMS repository,
  Streaming one package each 3 seconds */
  private val throttlingFlow: Flow[NpmPackage, NpmPackage, NotUsed] = Flow[NpmPackage].throttle(
    // cap on the number of elements allowed
    elements = 1,
    // Interval slot
    per = 3.second,
    maximumBurst = 0,
    // stream will collapse if exceeding the number of elements /
    mode = ThrottleMode.Shaping
  )

  private val parallelStage: Flow[Package, Package, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits.*

    val dispatchPackages = builder.add(Balance[Package](3))
    val mergePackages = builder.add(Merge[Package](3))
    dispatchPackages.out(0) ~> pipeline ~> mergePackages.in(0)
    dispatchPackages.out(1) ~> pipeline ~> mergePackages.in(1)
    dispatchPackages.out(2) ~> pipeline ~> mergePackages.in(2)

    FlowShape(dispatchPackages.in, mergePackages.out)
  })

  private val sink: Sink[Package, Future[Done]] = Sink.foreach(packageInfo => {
    println(s"Package Name: ${packageInfo.name}, Stars: ${packageInfo.stars}, TestCoverage: ${packageInfo.test}, Downloads: ${packageInfo.downloads}, Release Frequency: ${packageInfo.releaseFrequency}")
  })

  private val runnableGraph = sourceBuff
    .via(throttlingFlow)
    .via(extractionProcess)
    .via(parallelStage)
    .to(sink)

  runnableGraph.run()


  // Requests a package to the API and returns the information about that package
  // Change this code at your convenience
  def apiRequest(pack: String) = {
    println(s"Analysing $pack")

    val url = s"https://api.npms.io/v2/package/$pack"
    val response = requests.get(url)
    val json = ujson.read(response.data.toString)
    json.obj
  }
}