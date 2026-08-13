package fr.dpelab.silver

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}

/** Transformations bronze -> silver, écrites comme des fonctions pures
  * `DataFrame => DataFrame` pour être testables sans job complet.
  */
object Transformations {

  /** Étiquettes réglementaires valides (A à G). */
  val EtiquettesValides: Seq[String] = Seq("A", "B", "C", "D", "E", "F", "G")

  /** Seuil de confiance du géocodage BAN en dessous duquel la localisation est
    * jugée non fiable. 0,8 est le seuil usuel retenu par l'ADEME : en dessous,
    * l'adresse a été rapprochée de façon approximative (rue trouvée mais pas le
    * numéro, ou commune seule).
    */
  val SeuilScoreBan: Double = 0.8

  // ------------------------------------------------------------------ //
  // 1. Typage
  // ------------------------------------------------------------------ //

  private val ColonnesDate = Seq(
    "date_etablissement_dpe",
    "date_reception_dpe",
    "date_visite_diagnostiqueur",
    "date_fin_validite_dpe",
    "date_derniere_modification_dpe"
  )

  private val ColonnesDouble = Seq(
    "surface_habitable_logement", "surface_habitable_immeuble", "hauteur_sous_plafond",
    "conso_5_usages_par_m2_ep", "conso_5_usages_par_m2_ef", "emission_ges_5_usages_par_m2",
    "ubat_w_par_m2_k", "conso_5_usages_ep", "conso_5_usages_ef", "conso_chauffage_ef",
    "conso_ecs_ef", "emission_ges_5_usages", "cout_total_5_usages", "cout_chauffage",
    "cout_ecs", "besoin_chauffage", "besoin_ecs", "deperditions_enveloppe",
    "deperditions_murs", "deperditions_baies_vitrees", "score_ban",
    "coordonnee_cartographique_x_ban", "coordonnee_cartographique_y_ban",
    "production_electricite_pv_kwhep_par_an", "version_dpe"
  )

  private val ColonnesEntier = Seq(
    "annee_construction", "nombre_niveau_logement", "nombre_appartement"
  )

  /** Convertit les colonnes texte de bronze vers leurs types cibles.
    *
    * Le cast Spark renvoie `null` sur valeur non convertible plutôt que de faire
    * échouer le job : les valeurs aberrantes sont donc capturées par les règles
    * de qualité en aval, pas par une exception.
    */
  def typerColonnes(df: DataFrame): DataFrame = {
    val presentes = df.columns.toSet

    val avecDates = ColonnesDate.filter(presentes).foldLeft(df) { (acc, col) =>
      acc.withColumn(col, to_date(trim(acc(col)), "yyyy-MM-dd"))
    }
    val avecDoubles = ColonnesDouble.filter(presentes).foldLeft(avecDates) { (acc, col) =>
      acc.withColumn(col, trim(acc(col)).cast(DoubleType))
    }
    ColonnesEntier.filter(presentes).foldLeft(avecDoubles) { (acc, col) =>
      acc.withColumn(col, trim(acc(col)).cast(IntegerType))
    }
  }

  // ------------------------------------------------------------------ //
  // 2. Déduplication technique
  // ------------------------------------------------------------------ //

  /** Ne conserve qu'une ligne par `numero_dpe`.
    *
    * Un même DPE peut apparaître plusieurs fois : les partitions mensuelles se
    * recouvrent lorsqu'un DPE est corrigé après coup, et la source elle-même
    * expose ponctuellement des doublons. On garde la version la plus récemment
    * modifiée, puis la plus récemment reçue pour départager.
    */
  def dedupliquerParNumero(df: DataFrame): DataFrame = {
    val fenetre = Window
      .partitionBy(col("numero_dpe"))
      .orderBy(
        col("date_derniere_modification_dpe").desc_nulls_last,
        col("date_reception_dpe").desc_nulls_last
      )

    df.withColumn("_rang", row_number().over(fenetre))
      .filter(col("_rang") === 1)
      .drop("_rang")
  }

  // ------------------------------------------------------------------ //
  // 3. Chaînes de remplacement
  // ------------------------------------------------------------------ //

  /** Marque les DPE rendus obsolètes par un DPE plus récent.
    *
    * `numero_dpe_remplace` pointe du nouveau vers l'ancien. Un DPE est donc
    * périmé dès lors qu'il figure dans la colonne `numero_dpe_remplace` d'un
    * autre enregistrement — c'est exactement la logique d'un identifiant
    * déprécié fusionné vers un identifiant maître.
    *
    * On résout ici un seul niveau, ce qui suffit : la source chaîne
    * A -> B -> C en produisant deux liens (C remplace B, B remplace A), donc A
    * et B sont tous deux marqués obsolètes par la seule appartenance à la
    * colonne. Une fermeture transitive serait nécessaire uniquement si l'on
    * voulait rattacher A directement à C, ce dont l'analyse n'a pas besoin.
    */
  def marquerRemplaces(df: DataFrame): DataFrame = {
    val remplaces = df
      .select(trim(col("numero_dpe_remplace")).as("numero_remplace"))
      .filter(col("numero_remplace").isNotNull && col("numero_remplace") =!= "")
      .distinct()

    df.join(remplaces, trim(col("numero_dpe")) === col("numero_remplace"), "left_outer")
      .withColumn("est_remplace", col("numero_remplace").isNotNull)
      .drop("numero_remplace")
  }

  // ------------------------------------------------------------------ //
  // 4. Règles de qualité
  // ------------------------------------------------------------------ //

  /** Motif de rejet, ou `null` si la ligne est exploitable.
    *
    * Une seule colonne plutôt qu'un booléen par règle : elle alimente
    * directement un décompte des causes de rejet, lisible dans Power BI.
    */
  private def motifRejet: Column =
    when(col("numero_dpe").isNull || trim(col("numero_dpe")) === "", "identifiant_absent")
      .when(!col("etiquette_dpe").isin(EtiquettesValides: _*), "etiquette_dpe_invalide")
      .when(col("date_etablissement_dpe").isNull, "date_etablissement_absente")
      .when(col("code_insee_ban").isNull || trim(col("code_insee_ban")) === "", "commune_absente")
      .when(
        col("surface_habitable_logement").isNull || col("surface_habitable_logement") <= 0,
        "surface_invalide"
      )
      // Un logement de plus de 1 000 m² relève de la saisie erronée dans un jeu
      // portant sur des logements d'habitation.
      .when(col("surface_habitable_logement") > 1000, "surface_aberrante")
      .when(
        col("conso_5_usages_par_m2_ep").isNull || col("conso_5_usages_par_m2_ep") < 0,
        "consommation_invalide"
      )
      // Le barème réglementaire plafonne l'étiquette G à 420 kWh/m²/an ; on
      // tolère largement au-delà pour ne pas écarter des cas réels extrêmes,
      // mais 2 000 traduit une erreur d'unité ou de saisie.
      .when(col("conso_5_usages_par_m2_ep") > 2000, "consommation_aberrante")
      .otherwise(lit(null).cast(StringType))

  def appliquerQualite(df: DataFrame): DataFrame =
    df.withColumn("motif_rejet", motifRejet)
      .withColumn("est_valide", col("motif_rejet").isNull)
      .withColumn("geocodage_fiable", coalesce(col("score_ban") >= SeuilScoreBan, lit(false)))

  // ------------------------------------------------------------------ //
  // 5. Enrichissements analytiques
  // ------------------------------------------------------------------ //

  /** Ajoute les dimensions dérivées attendues par la couche gold. */
  def enrichir(df: DataFrame): DataFrame =
    df.withColumn("est_passoire_thermique", col("etiquette_dpe").isin("F", "G"))
      .withColumn("est_performant", col("etiquette_dpe").isin("A", "B"))
      .withColumn("annee_etablissement", year(col("date_etablissement_dpe")))
      .withColumn("mois_etablissement", month(col("date_etablissement_dpe")))
      .withColumn(
        "tranche_surface",
        when(col("surface_habitable_logement") < 30, "moins de 30 m2")
          .when(col("surface_habitable_logement") < 60, "30 a 60 m2")
          .when(col("surface_habitable_logement") < 90, "60 a 90 m2")
          .when(col("surface_habitable_logement") < 130, "90 a 130 m2")
          .otherwise("130 m2 et plus")
      )
      .withColumn(
        "tranche_age_batiment",
        when(col("annee_construction").isNull, "inconnue")
          .when(col("annee_construction") < 1948, "avant 1948")
          .when(col("annee_construction") < 1975, "1948-1974")
          // 1974 : première réglementation thermique, rupture nette de performance.
          .when(col("annee_construction") < 1989, "1975-1988")
          .when(col("annee_construction") < 2001, "1989-2000")
          .when(col("annee_construction") < 2013, "2001-2012")
          .otherwise("2013 et apres")
      )
      .withColumn("dpe_encore_valide", col("date_fin_validite_dpe") >= current_date())

  // ------------------------------------------------------------------ //
  // Chaîne complète
  // ------------------------------------------------------------------ //

  /** Applique l'ensemble des étapes bronze -> silver dans l'ordre. */
  def preparer(bronze: DataFrame): DataFrame =
    enrichir(appliquerQualite(marquerRemplaces(dedupliquerParNumero(typerColonnes(bronze)))))
}
