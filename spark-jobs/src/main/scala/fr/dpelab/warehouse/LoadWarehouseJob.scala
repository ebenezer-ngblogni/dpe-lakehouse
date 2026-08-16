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

  /** Colonnes de `dpe_courant` réellement consommées par le modèle dbt.
    *
    * Silver en compte 78, `stg_dpe` n'en sélectionne que celles-ci. Charger les
    * 38 autres coûtait 2,4 Go dans PostgreSQL pour des colonnes qu'aucune
    * requête n'interroge — le lac reste la référence complète, l'entrepôt ne
    * porte que ce qui sert.
    */
  val ColonnesEntrepot: Seq[String] = Seq(
    // Identité et temps
    "numero_dpe", "numero_dpe_immeuble_associe", "date_etablissement_dpe",
    "date_fin_validite_dpe", "annee_etablissement", "mois_etablissement",
    "dpe_encore_valide",
    // Géographie
    "code_insee_ban", "nom_commune_ban", "code_postal_ban", "code_departement_ban",
    "code_region_ban", "coordonnee_cartographique_x_ban", "coordonnee_cartographique_y_ban",
    "geocodage_fiable", "geocodage_precis",
    // Bâti
    "type_batiment", "annee_construction", "tranche_age_batiment",
    "surface_habitable_logement", "tranche_surface", "nombre_niveau_logement",
    // Performance
    "etiquette_dpe", "etiquette_ges", "est_passoire_thermique", "est_performant",
    "conso_5_usages_par_m2_ep", "conso_5_usages_par_m2_ef",
    "emission_ges_5_usages_par_m2", "ubat_w_par_m2_k",
    // Coûts
    "cout_total_5_usages", "cout_chauffage", "cout_ecs",
    // Équipements et isolation
    "type_energie_principale_chauffage", "type_generateur_chauffage_principal",
    "type_energie_principale_ecs", "type_ventilation",
    "qualite_isolation_enveloppe", "qualite_isolation_murs", "qualite_isolation_menuiseries"
  )

  final case class WarehouseConfig(
      silverPath: String = "data/silver/dpe_courant",
      rejectsPath: String = "data/silver/dpe_rejets",
      jdbcUrl: String = "jdbc:postgresql://localhost:5434/dpe",
      user: String = "dpe",
      password: String = "dpe",
      schema: String = "silver",
      batchSize: Int = 10000,
      // Le lac conserve l'historique intégral depuis juillet 2021 ; l'entrepôt
      // ne sert qu'une fenêtre analytique. Séparer les deux est un choix
      // d'architecture courant : il évite de dimensionner l'entrepôt sur un
      // historique que les rapports n'interrogent pas.
      // `None` charge tout.
      depuisAnnee: Option[Int] = None
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
      opt[Int]("depuis-annee")
        .action((v, c) => c.copy(depuisAnnee = Some(v)))
        .text("Ne charge que les DPE établis à partir de cette année (défaut : tout)"),
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

    charger(spark, config.silverPath, s"${config.schema}.dpe_courant", config, props, ColonnesEntrepot)
    // Les rejets ne portent déjà que 5 colonnes : rien à projeter.
    charger(spark, config.rejectsPath, s"${config.schema}.dpe_rejets", config, props)
  }

  private def charger(
      spark: SparkSession,
      source: String,
      table: String,
      config: WarehouseConfig,
      props: Properties,
      colonnes: Seq[String] = Seq.empty
  ): Unit = {
    import org.apache.spark.sql.functions.col

    val brut: DataFrame = spark.read.parquet(source)

    // Le filtre porte sur la colonne de partition : Spark élague les répertoires
    // sans les lire, le coût du filtrage est donc nul.
    val filtre = config.depuisAnnee match {
      case Some(annee) if brut.columns.contains("annee_etablissement") =>
        println(s"[warehouse] fenêtre appliquée : annee_etablissement >= $annee")
        brut.filter(col("annee_etablissement") >= annee)
      case _ => brut
    }

    // Projection : Parquet étant orienté colonnes, les colonnes non retenues ne
    // sont même pas lues sur disque.
    val retenues = colonnes.filter(filtre.columns.contains)
    val df =
      if (retenues.isEmpty) filtre
      else {
        println(s"[warehouse] projection : ${retenues.size} colonnes sur ${filtre.columns.length}")
        filtre.select(retenues.map(col): _*)
      }

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
