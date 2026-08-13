package fr.dpelab.common

import org.apache.spark.sql.SparkSession
import scopt.OParser

/** Paramètres communs aux jobs Spark, fournis en ligne de commande.
  *
  * Aucun chemin n'est codé en dur : le même jar tourne en local sur un
  * répertoire de test et sur MinIO/S3 en production, sans recompilation.
  */
final case class JobConfig(
    bronzePath: String = "data/bronze/dpe_existant",
    silverPath: String = "data/silver/dpe_courant",
    rejectsPath: String = "data/silver/dpe_rejets",
    metricsPath: String = "data/silver/_metrics",
    partitionOverwrite: Boolean = true,
    shufflePartitions: Int = 64
)

object JobConfig {

  private val builder = OParser.builder[JobConfig]

  private val parser = {
    import builder._
    OParser.sequence(
      programName("dpe-spark-jobs"),
      head("dpe-spark-jobs", "1.0.0"),
      opt[String]("bronze-path")
        .action((value, config) => config.copy(bronzePath = value))
        .text("Racine Parquet de la couche bronze"),
      opt[String]("silver-path")
        .action((value, config) => config.copy(silverPath = value))
        .text("Destination des DPE courants nettoyés"),
      opt[String]("rejects-path")
        .action((value, config) => config.copy(rejectsPath = value))
        .text("Destination des lignes écartées, avec leur motif"),
      opt[String]("metrics-path")
        .action((value, config) => config.copy(metricsPath = value))
        .text("Destination des métriques de qualité du run"),
      opt[Int]("shuffle-partitions")
        .action((value, config) => config.copy(shufflePartitions = value))
        .text("spark.sql.shuffle.partitions (défaut 64, adapté à une machine unique)"),
      help("help").text("Affiche cette aide")
    )
  }

  def parse(args: Array[String]): Option[JobConfig] =
    OParser.parse(parser, args, JobConfig())

  /** Construit une SparkSession configurée pour l'écriture idempotente.
    *
    * `partitionOverwriteMode=dynamic` est le réglage clé : sans lui, un
    * `overwrite` efface *toute* la table au lieu des seules partitions
    * réécrites. C'est la différence entre un job rejouable et un job qui détruit
    * l'historique à chaque exécution partielle.
    */
  def session(appName: String, config: JobConfig): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.shuffle.partitions", config.shufflePartitions.toString)
      .config("spark.sql.parquet.compression.codec", "snappy")
      // Évite les fichiers _SUCCESS/_committed superflus côté objet-store.
      .config("spark.sql.parquet.output.committer.class",
        "org.apache.parquet.hadoop.ParquetOutputCommitter")

    sys.env.get("AWS_ENDPOINT_URL").fold(builder) { endpoint =>
      builder
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    }.getOrCreate()
  }
}
