package fr.dpelab.warehouse

import fr.dpelab.common.JobConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scopt.OParser

import java.util.Properties

/** Charge la couche silver du lac vers PostgreSQL, où dbt prend le relais.
  *
  * Frontière de responsabilité du projet : Spark fait le gros œuvre sur le lac
  * (15 M de lignes, jointures larges, déduplication), l'entrepôt ne reçoit que
  * le résultat nettoyé et dbt y construit le modèle en étoile en SQL. Faire la
  * déduplication dans PostgreSQL serait possible mais bien plus lent, et faire
  * le modèle en étoile dans Spark priverait le projet du lignage et des tests
  * que dbt fournit gratuitement.
  */
object LoadWarehouseJob {

  final case class WarehouseConfig(
      silverPath: String = "data/silver/dpe_courant",
      rejectsPath: String = "data/silver/dpe_rejets",
      jdbcUrl: String = "jdbc:postgresql://localhost:5433/dpe",
      user: String = "dpe",
      password: String = "dpe",
      schema: String = "silver",
      batchSize: Int = 10000
  )

  private val parser = {
    val builder = OParser.builder[WarehouseConfig]
    import builder._
    OParser.sequence(
      programName("load-warehouse"),
      opt[String]("silver-path").action((v, c) => c.copy(silverPath = v)),
      opt[String]("rejects-path").action((v, c) => c.copy(rejectsPath = v)),
      opt[String]("jdbc-url").action((v, c) => c.copy(jdbcUrl = v)),
      opt[String]("user").action((v, c) => c.copy(user = v)),
      opt[String]("password").action((v, c) => c.copy(password = v)),
      opt[String]("schema").action((v, c) => c.copy(schema = v)),
      opt[Int]("batch-size").action((v, c) => c.copy(batchSize = v)),
      help("help")
    )
  }

  def main(args: Array[String]): Unit = {
    OParser.parse(parser, args, WarehouseConfig()) match {
      case None => sys.exit(2)
      case Some(config) =>
        val spark = JobConfig.session("dpe-load-warehouse", JobConfig())
        try {
          run(spark, config)
        } finally {
          spark.stop()
        }
    }
  }

  def run(spark: SparkSession, config: WarehouseConfig): Unit = {
    val props = new Properties()
    props.setProperty("user", config.user)
    props.setProperty("password", config.password)
    props.setProperty("driver", "org.postgresql.Driver")
    // Sans lot explicite, le pilote JDBC envoie ligne par ligne : le chargement
    // passe de quelques minutes à plusieurs heures.
    props.setProperty("batchsize", config.batchSize.toString)
    props.setProperty("rewriteBatchedStatements", "true")

    charger(spark, config.silverPath, s"${config.schema}.dpe_courant", config, props)
    charger(spark, config.rejectsPath, s"${config.schema}.dpe_rejets", config, props)
  }

  private def charger(
      spark: SparkSession,
      source: String,
      table: String,
      config: WarehouseConfig,
      props: Properties
  ): Unit = {
    val df: DataFrame = spark.read.parquet(source)
    val lignes = df.count()

    // Le nombre de partitions Spark détermine le nombre de connexions JDBC
    // simultanées. Au-delà d'une dizaine, PostgreSQL passe plus de temps à
    // arbitrer les connexions qu'à écrire.
    val partitions = math.max(1, math.min(8, (lignes / 500000).toInt + 1))

    println(s"[warehouse] $source -> $table : $lignes lignes sur $partitions partition(s)")

    df.repartition(partitions)
      .write
      .mode(SaveMode.Overwrite)
      .option("truncate", "true") // conserve la table et ses index au lieu de la recréer
      .jdbc(config.jdbcUrl, table, props)

    println(s"[warehouse] $table chargée")
  }
}
