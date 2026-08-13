package fr.dpelab.silver

import fr.dpelab.common.JobConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/** Job bronze -> silver.
  *
  * Produit trois sorties :
  *   - `silver/dpe_courant` : un DPE courant et valide par logement, typé et enrichi
  *   - `silver/dpe_rejets`  : les lignes écartées, avec leur motif — un pipeline qui
  *     jette silencieusement des données est un pipeline qu'on ne peut pas auditer
  *   - `silver/_metrics`    : les compteurs du run, exploitables dans Power BI
  *
  * Le job est rejouable : les partitions réécrites remplacent leurs homologues
  * et les autres restent intactes (`partitionOverwriteMode=dynamic`).
  */
object DpeSilverJob {

  def main(args: Array[String]): Unit = {
    JobConfig.parse(args) match {
      case None => sys.exit(2)
      case Some(config) =>
        val spark = JobConfig.session("dpe-silver", config)
        try {
          run(spark, config)
        } finally {
          spark.stop()
        }
    }
  }

  def run(spark: SparkSession, config: JobConfig): Unit = {
    val bronze = spark.read.parquet(config.bronzePath)
    val lignesBronze = bronze.count()
    println(s"[silver] bronze : $lignesBronze lignes lues depuis ${config.bronzePath}")

    val prepare = Transformations.preparer(bronze).cache()

    // Le DPE retenu pour l'analyse : valide au sens des règles qualité et non
    // périmé par un diagnostic plus récent sur le même logement.
    val courant = prepare.filter(col("est_valide") && !col("est_remplace"))
    val rejets = prepare.filter(!col("est_valide") || col("est_remplace"))

    val metriques = calculerMetriques(prepare, lignesBronze)
    afficherMetriques(metriques)

    ecrire(courant.drop("motif_rejet", "est_valide"), config.silverPath, partitions = Seq("code_region_ban", "annee_etablissement"))
    ecrire(rejets.select(
      col("numero_dpe"),
      col("date_etablissement_dpe"),
      col("code_region_ban"),
      col("annee_etablissement"),
      when(col("est_remplace"), lit("remplace_par_dpe_recent"))
        .otherwise(col("motif_rejet")).as("motif_rejet")
    ), config.rejectsPath, partitions = Seq("annee_etablissement"))

    ecrireMetriques(spark, metriques, config.metricsPath)

    prepare.unpersist()
    println("[silver] terminé")
  }

  // ------------------------------------------------------------------ //
  // Métriques de qualité
  // ------------------------------------------------------------------ //

  final case class Metriques(
      lignesBronze: Long,
      apresDeduplication: Long,
      remplaces: Long,
      rejetes: Long,
      retenus: Long,
      geocodageNonFiable: Long,
      motifs: Seq[(String, Long)]
  )

  def calculerMetriques(prepare: DataFrame, lignesBronze: Long): Metriques = {
    val apresDedup = prepare.count()
    val remplaces = prepare.filter(col("est_remplace")).count()
    val rejetes = prepare.filter(!col("est_valide")).count()
    val retenus = prepare.filter(col("est_valide") && !col("est_remplace")).count()
    val geoNonFiable = prepare.filter(col("est_valide") && !col("geocodage_fiable")).count()

    val motifs = prepare
      .filter(col("motif_rejet").isNotNull)
      .groupBy("motif_rejet")
      .count()
      .orderBy(col("count").desc)
      .collect()
      .map(row => (row.getString(0), row.getLong(1)))
      .toSeq

    Metriques(lignesBronze, apresDedup, remplaces, rejetes, retenus, geoNonFiable, motifs)
  }

  private def afficherMetriques(m: Metriques): Unit = {
    def pct(part: Long): String =
      if (m.lignesBronze == 0) "-" else f"${part * 100.0 / m.lignesBronze}%.2f%%"

    println("[silver] ---------------- qualité du run ----------------")
    println(f"[silver] lignes bronze            : ${m.lignesBronze}%,d")
    println(f"[silver] après déduplication      : ${m.apresDeduplication}%,d  (doublons retirés : ${m.lignesBronze - m.apresDeduplication}%,d)")
    println(f"[silver] remplacés par un DPE récent : ${m.remplaces}%,d  (${pct(m.remplaces)})")
    println(f"[silver] rejetés (règles qualité) : ${m.rejetes}%,d  (${pct(m.rejetes)})")
    println(f"[silver] retenus en silver        : ${m.retenus}%,d  (${pct(m.retenus)})")
    println(f"[silver] géocodage non fiable     : ${m.geocodageNonFiable}%,d")
    if (m.motifs.nonEmpty) {
      println("[silver] motifs de rejet :")
      m.motifs.foreach { case (motif, n) => println(f"[silver]   $motif%-32s ${n}%,d") }
    }
    println("[silver] ------------------------------------------------")
  }

  private def ecrireMetriques(spark: SparkSession, m: Metriques, path: String): Unit = {
    import spark.implicits._
    val base = Seq(
      ("lignes_bronze", m.lignesBronze),
      ("apres_deduplication", m.apresDeduplication),
      ("remplaces", m.remplaces),
      ("rejetes", m.rejetes),
      ("retenus", m.retenus),
      ("geocodage_non_fiable", m.geocodageNonFiable)
    )
    val motifs = m.motifs.map { case (motif, n) => (s"motif_$motif", n) }

    (base ++ motifs).toDF("metrique", "valeur")
      .withColumn("execute_le", current_timestamp())
      .write.mode(SaveMode.Overwrite).parquet(path)
  }

  // ------------------------------------------------------------------ //
  // Écriture
  // ------------------------------------------------------------------ //

  private def ecrire(df: DataFrame, path: String, partitions: Seq[String]): Unit = {
    val colonnesPresentes = partitions.filter(df.columns.contains)
    val writer = df.write.mode(SaveMode.Overwrite)
    val partitionne =
      if (colonnesPresentes.isEmpty) writer else writer.partitionBy(colonnesPresentes: _*)
    partitionne.parquet(path)
    println(s"[silver] écrit : $path (partitionné par ${colonnesPresentes.mkString(", ")})")
  }
}
