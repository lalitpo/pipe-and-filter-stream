import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, Materializer, OverflowStrategy, ThrottleMode}
import akka.{Done, NotUsed}
import ujson.Obj

import java.io.{File, FileInputStream, PrintWriter}
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

case class NpmPackage(pack: String)

case class Package(name: String, stars: Double, test: Double, releases: Int, commitSum: Int)

object PipeAndFilterStream extends App {


  private implicit val actorSystem: ActorSystem = ActorSystem("Assignment_1-Akka_Streams")
  private implicit val materializer: Materializer = ActorMaterializer()
  private implicit val executionContext: ExecutionContextExecutor = materializer.executionContext


  private val packageList: ListBuffer[NpmPackage] = new ListBuffer[NpmPackage]()

  private val packageStream = new GZIPInputStream(new FileInputStream(new File("src/main/resources/packages.txt.gz")))

  /*
  Each line in the compressed file contains the name of a package,
  so mapping each line in the file to an object. */
  for (pack <- io.Source.fromInputStream(packageStream).getLines()) {
    packageList += NpmPackage(pack)
  }

  //the packages are converted into a Source to start the streaming process
  private val source: Source[NpmPackage, NotUsed] = Source(packageList.toList)

  //Stream buffered into source stream with a maximum capacity of 25 packages with a backpressure strategy.
  private val sourceBuff: Source[NpmPackage, NotUsed] = source.buffer(25, OverflowStrategy.backpressure)

  private val extractionProcess: Flow[NpmPackage, Package, NotUsed] = Flow[NpmPackage].map(item => {
    val packName = item.pack
    val packageContent = apiRequest(packName)
    val name = packageContent("collected")("metadata")("name").str
    val starsCount = packageContent("collected")("github")("starsCount").num
    val test = packageContent("evaluation")("quality")("tests").num
    val releaseCount = packageContent("collected")("metadata")("releases").arr.length
    //Sorting commitCounts and then picking top-3 commits count
    val top3ContribCommits = packageContent("collected")("github")("contributors").arr.sortBy(_.obj("commitsCount").num)(Ordering[Double].reverse).take(3)
    val commitSum = top3ContribCommits.map(_("commitsCount").num).sum.intValue()

    Package(name, starsCount, test, releaseCount, commitSum)
  })

  private val pipeline: Flow[Package, Package, NotUsed] = Flow[Package].filter(packData => {
    packData.stars > 20 &&
      packData.test > 0.5 &&
      packData.releases > 2 &&
      packData.commitSum > 150
  }
  )

  /*
  * Keeping the number of API requests under control and not to stress the NPMS repository,
  Streaming one package every 2 seconds */
  private val throttlingFlow: Flow[NpmPackage, NpmPackage, NotUsed] = Flow[NpmPackage].throttle(
    // cap on the number of elements allowed
    elements = 1,
    // Interval slot
    per = 2.second,
    maximumBurst = 0,
    mode = ThrottleMode.Shaping
  )

  private val parallelStage: Flow[Package, Package, NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits.*

    val dispatchPackages = builder.add(Broadcast[Package](3))
    val mergePackages = builder.add(Merge[Package](3))
    dispatchPackages.out(0) ~> pipeline ~> mergePackages.in(0)
    dispatchPackages.out(1) ~> pipeline ~> mergePackages.in(1)
    dispatchPackages.out(2) ~> pipeline ~> mergePackages.in(2)

    FlowShape(dispatchPackages.in, mergePackages.out)
  })

  private val sink: Sink[Package, Future[Done]] = Sink.foreach(packageInfo => {
    val message = s"Package Name: ${packageInfo.name}," +
      s" Stars: ${packageInfo.stars}," +
      s" TestCoverage: ${packageInfo.test}," +
      s" Release Count: ${packageInfo.releases}," +
      s" Commits Count: ${packageInfo.commitSum}"
    println(message)
    writeToLogFile(message)
  })

  // Function to store standard outputs in an external file
  private def writeToLogFile(message: String): Unit = {
    val writer = new PrintWriter(new java.io.FileWriter("Result.txt", true))
    writer.println(message)
    writer.close()
  }

  private val runnableGraph = sourceBuff
    .via(throttlingFlow)
    .via(extractionProcess)
    .via(parallelStage)
    .to(sink)

  runnableGraph.run()


  // Requests a package to the API and returns the information about that package
  // Change this code at your convenience
  private def apiRequest(pack: String) = {

    val url = s"https://api.npms.io/v2/package/$pack"
    val response = requests.get(url)
    val json = ujson.read(response.data.toString)
    json.obj
  }
}