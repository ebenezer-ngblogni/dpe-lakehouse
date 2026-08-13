package fr.dpelab

import fr.dpelab.silver.Transformations
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests des transformations bronze -> silver.
  *
  * Les jeux d'essai reproduisent les anomalies réellement observées dans la base
  * ADEME : doublons d'identifiant, chaînes de remplacement, géocodage approximatif,
  * surfaces et consommations aberrantes.
  */
class TransformationsSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("transformations-spec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  /** Construit un DataFrame bronze : toutes les colonnes en texte, comme en sortie
    * d'ingestion.
    */
  private def bronze(lignes: (String, String, String, String, String, String, String, String)*): DataFrame = {
    // `spark` est un `var` (initialisé dans beforeAll) : Scala refuse d'importer
    // ses implicits directement, il faut passer par une référence stable.
    val session = spark
    import session.implicits._
    lignes.toDF(
      "numero_dpe",
      "numero_dpe_remplace",
      "date_etablissement_dpe",
      "date_derniere_modification_dpe",
      "date_reception_dpe",
      "etiquette_dpe",
      "code_insee_ban",
      "surface_habitable_logement"
    ).withColumn("conso_5_usages_par_m2_ep", org.apache.spark.sql.functions.lit("150"))
      .withColumn("score_ban", org.apache.spark.sql.functions.lit("0.95"))
      .withColumn("date_fin_validite_dpe", org.apache.spark.sql.functions.lit("2035-01-01"))
      .withColumn("annee_construction", org.apache.spark.sql.functions.lit("1980"))
  }

  // ------------------------------------------------------------------ //

  test("le typage convertit le texte vers date et numérique, sans faire échouer le job") {
    val df = bronze(
      ("A1", "", "2024-05-12", "2024-05-12", "2024-05-12", "D", "11069", "75.5")
    )
    val typed = Transformations.typerColonnes(df)

    typed.schema("date_etablissement_dpe").dataType.typeName shouldBe "date"
    typed.schema("surface_habitable_logement").dataType.typeName shouldBe "double"
    typed.select("surface_habitable_logement").head().getDouble(0) shouldBe 75.5
  }

  test("une valeur numérique illisible devient null au lieu de faire échouer le job") {
    val df = bronze(
      ("A1", "", "2024-05-12", "2024-05-12", "2024-05-12", "D", "11069", "non renseigné")
    )
    val typed = Transformations.typerColonnes(df)

    typed.select("surface_habitable_logement").head().isNullAt(0) shouldBe true
  }

  test("la déduplication garde la version la plus récemment modifiée") {
    val df = bronze(
      ("A1", "", "2024-05-12", "2024-05-12", "2024-05-12", "D", "11069", "75"),
      ("A1", "", "2024-05-12", "2024-09-30", "2024-09-30", "C", "11069", "75")
    )
    val result = Transformations.dedupliquerParNumero(Transformations.typerColonnes(df))

    result.count() shouldBe 1
    result.select("etiquette_dpe").head().getString(0) shouldBe "C"
  }

  test("un DPE cité dans numero_dpe_remplace est marqué comme remplacé") {
    val df = bronze(
      ("ANCIEN", "", "2022-01-10", "2022-01-10", "2022-01-10", "F", "11069", "80"),
      ("RECENT", "ANCIEN", "2024-03-15", "2024-03-15", "2024-03-15", "D", "11069", "80")
    )
    val result = Transformations.marquerRemplaces(Transformations.typerColonnes(df))

    val parNumero = result.collect().map(r => r.getAs[String]("numero_dpe") -> r.getAs[Boolean]("est_remplace")).toMap
    parNumero("ANCIEN") shouldBe true
    parNumero("RECENT") shouldBe false
  }

  test("une chaîne de remplacement à trois maillons ne laisse qu'un seul DPE courant") {
    // C remplace B, B remplace A : seul C doit rester courant.
    val df = bronze(
      ("A", "", "2021-09-01", "2021-09-01", "2021-09-01", "G", "11069", "80"),
      ("B", "A", "2022-09-01", "2022-09-01", "2022-09-01", "F", "11069", "80"),
      ("C", "B", "2023-09-01", "2023-09-01", "2023-09-01", "E", "11069", "80")
    )
    val result = Transformations.marquerRemplaces(Transformations.typerColonnes(df))
    val courants = result.filter("not est_remplace").collect().map(_.getAs[String]("numero_dpe"))

    courants should contain theSameElementsAs Seq("C")
  }

  test("les règles de qualité écartent surfaces et consommations aberrantes") {
    val df = bronze(
      ("OK", "", "2024-05-12", "2024-05-12", "2024-05-12", "D", "11069", "75"),
      ("SURFACE_NULLE", "", "2024-05-12", "2024-05-12", "2024-05-12", "D", "11069", "0"),
      ("SURFACE_ENORME", "", "2024-05-12", "2024-05-12", "2024-05-12", "D", "11069", "5000"),
      ("ETIQUETTE_KO", "", "2024-05-12", "2024-05-12", "2024-05-12", "Z", "11069", "75"),
      ("COMMUNE_VIDE", "", "2024-05-12", "2024-05-12", "2024-05-12", "D", "", "75")
    )
    val result = Transformations.appliquerQualite(Transformations.typerColonnes(df))
    val motifs = result.collect()
      .map(r => r.getAs[String]("numero_dpe") -> Option(r.getAs[String]("motif_rejet"))).toMap

    motifs("OK") shouldBe None
    motifs("SURFACE_NULLE") shouldBe Some("surface_invalide")
    motifs("SURFACE_ENORME") shouldBe Some("surface_aberrante")
    motifs("ETIQUETTE_KO") shouldBe Some("etiquette_dpe_invalide")
    motifs("COMMUNE_VIDE") shouldBe Some("commune_absente")
  }

  test("une consommation au-delà du seuil d'unité est rejetée") {
    import org.apache.spark.sql.functions.lit
    val df = bronze(("X", "", "2024-05-12", "2024-05-12", "2024-05-12", "G", "11069", "75"))
      .withColumn("conso_5_usages_par_m2_ep", lit("35000"))
    val result = Transformations.appliquerQualite(Transformations.typerColonnes(df))

    result.select("motif_rejet").head().getString(0) shouldBe "consommation_aberrante"
  }

  test("un score BAN faible marque le géocodage comme non fiable sans rejeter la ligne") {
    import org.apache.spark.sql.functions.lit
    val df = bronze(("X", "", "2024-05-12", "2024-05-12", "2024-05-12", "D", "11069", "75"))
      .withColumn("score_ban", lit("0.42"))
    val result = Transformations.appliquerQualite(Transformations.typerColonnes(df))
    val row = result.head()

    row.getAs[Boolean]("geocodage_fiable") shouldBe false
    row.getAs[Boolean]("est_valide") shouldBe true
  }

  test("un score BAN absent ne fait pas planter le drapeau de fiabilité") {
    import org.apache.spark.sql.functions.lit
    import org.apache.spark.sql.types.StringType
    val df = bronze(("X", "", "2024-05-12", "2024-05-12", "2024-05-12", "D", "11069", "75"))
      .withColumn("score_ban", lit(null).cast(StringType))
    val result = Transformations.appliquerQualite(Transformations.typerColonnes(df))

    result.head().getAs[Boolean]("geocodage_fiable") shouldBe false
  }

  test("l'enrichissement identifie les passoires thermiques et les tranches d'âge") {
    val df = bronze(
      ("PASSOIRE", "", "2024-05-12", "2024-05-12", "2024-05-12", "G", "11069", "75"),
      ("PERFORMANT", "", "2024-05-12", "2024-05-12", "2024-05-12", "A", "11069", "75")
    )
    val result = Transformations.enrichir(Transformations.typerColonnes(df))
    val lignes = result.collect().map(r => r.getAs[String]("numero_dpe") -> r).toMap

    lignes("PASSOIRE").getAs[Boolean]("est_passoire_thermique") shouldBe true
    lignes("PERFORMANT").getAs[Boolean]("est_performant") shouldBe true
    lignes("PASSOIRE").getAs[String]("tranche_age_batiment") shouldBe "1975-1988"
    lignes("PASSOIRE").getAs[String]("tranche_surface") shouldBe "60 a 90 m2"
    lignes("PASSOIRE").getAs[Int]("annee_etablissement") shouldBe 2024
  }

  test("la chaîne complète est idempotente : deux exécutions donnent le même résultat") {
    val df = bronze(
      ("A1", "", "2024-05-12", "2024-05-12", "2024-05-12", "D", "11069", "75"),
      ("A1", "", "2024-05-12", "2024-09-30", "2024-09-30", "C", "11069", "75"),
      ("ANCIEN", "", "2022-01-10", "2022-01-10", "2022-01-10", "F", "11069", "80"),
      ("RECENT", "ANCIEN", "2024-03-15", "2024-03-15", "2024-03-15", "D", "11069", "80")
    )
    val premier = Transformations.preparer(df).collect().map(_.mkString("|")).sorted
    val second = Transformations.preparer(df).collect().map(_.mkString("|")).sorted

    premier should contain theSameElementsInOrderAs second.toSeq
    // A1 dédoublonné, ANCIEN remplacé -> restent A1(C) et RECENT
    Transformations.preparer(df).filter("est_valide and not est_remplace").count() shouldBe 2
  }
}
