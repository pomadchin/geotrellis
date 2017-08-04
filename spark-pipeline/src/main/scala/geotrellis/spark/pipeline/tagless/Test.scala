package geotrellis.spark.pipeline.tagless

import cats._
import cats.implicits._
import freestyle.tagless._
import geotrellis.raster.{MultibandTile, Tile}
import geotrellis.spark.TemporalProjectedExtent
import geotrellis.spark.pipeline.json.PipelineExpr
import geotrellis.spark.pipeline.json.read.{SpatialHadoop => JsonSpatialHadoop, SpatialMultibandHadoop => JsonSpatialMultibandHadoop, TemporalHadoop => JsonTemporalHadoop, TemporalMultibandHadoop => JsonTemporalMultibandHadoop}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.util.Try

trait BigRDD[F[_], Tuple]

  object m {
    def foo[T: ({type M[A] = BigRDD[A, (ProjectedExtent, Tile)] })#M] = ???
  }

@tagless trait SinglebandSpatialHadoopReader {
  def eval(arg: JsonSpatialHadoop, sc: SparkContext): FS[(ProjectedExtent, Tile)]
}

@tagless trait SinglebandTemporalHadoopReader {
  def eval(arg: JsonTemporalHadoop, sc: SparkContext): FS[(TemporalProjectedExtent, Tile)]
}

@tagless trait MultibandSpatialHadoopReader {
  def eval(arg: JsonSpatialMultibandHadoop, sc: SparkContext): FS[(ProjectedExtent, MultibandTile)]
}

@tagless trait MultibandTemporalHadoopReader {
  def eval(arg: JsonTemporalMultibandHadoop, sc: SparkContext): FS[(TemporalProjectedExtent, MultibandTile)]
}


@tagless trait Validation {
  def minSize(s: String, n: Int): FS[Boolean]
  def hasNumber(s: String): FS[Boolean]
}

@tagless trait Interaction {
  def tell(msg: String): FS[Unit]
  def ask[S](prompt: S): FS[S]
}

object Test {
  implicit val singlebandSpatialHadoopReaderHandler = new SinglebandSpatialHadoopReader.Handler[RDD] {
    override def eval(arg: JsonSpatialHadoop, sc: SparkContext): RDD[(ProjectedExtent, Tile)] = arg.eval(sc)
  }

  implicit val singlebandTemporalHadoopReaderHandler = new SinglebandTemporalHadoopReader.Handler[RDD] {
    override def eval(arg: JsonTemporalHadoop, sc: SparkContext): RDD[(TemporalProjectedExtent, Tile)] = arg.eval(sc)
  }

  implicit val multibandSpatialHadoopReaderHandler = new MultibandSpatialHadoopReader.Handler[RDD] {
    override def eval(arg: JsonSpatialMultibandHadoop, sc: SparkContext): RDD[(ProjectedExtent, MultibandTile)] = arg.eval(sc)
  }

  implicit val multibandTemporalHadoopReaderHandler = new MultibandTemporalHadoopReader.Handler[RDD] {
    override def eval(arg: JsonTemporalMultibandHadoop, sc: SparkContext): RDD[(TemporalProjectedExtent, MultibandTile)] = arg.eval(sc)
  }

  trait DispatchReaderHandler[T] {
    //def eval: RDD[T] =
  }

  implicit val validationHandler = new Validation.Handler[Try] {
    override def minSize(s: String, n: Int): Try[Boolean] = Try(s.size >= n)
    override def hasNumber(s: String): Try[Boolean] = Try(s.exists(c => "0123456789".contains(c)))
  }

  implicit val interactionHandler = new Interaction.Handler[Try] {
    override def tell(s: String): Try[Unit] = Try(println(s))
    override def ask[S](s: S): Try[S] = Try("This could have been user input 1".asInstanceOf[S])
  }

  def program[F[_]: Monad](implicit validation : Validation[F], interaction: Interaction[F]) =
    for {
      userInput <- interaction.ask("Give me something with at least 3 chars and a number on it")
      valid <- (validation.minSize(userInput, 3) |@| validation.hasNumber(userInput)).map(_ && _)
      _ <- if (valid)
        interaction.tell("awesomesauce!")
      else
        interaction.tell(s"$userInput is not valid")
    } yield ()



  def main(args: Array[String]): Unit = {
    program[Try]
  }
}