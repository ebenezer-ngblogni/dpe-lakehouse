#!/usr/bin/env python3
"""Version portrait 1080×1350 du canevas d'architecture, pour carrousel LinkedIn.

Le format paysage plie mal sur mobile, où LinkedIn est surtout consulté. On
déroule donc la chaîne verticalement : un nœud par ligne, le libellé à droite,
le fil qui descend. La lecture suit le pouce.

Réutilise les icônes et l'embarquement des polices du générateur paysage.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from genere_canvas_architecture import icone, polices_embarquees  # noqa: E402

ICI = Path(__file__).parent
LARGEUR, HAUTEUR = 1080, 1350

MARGE = 68
N = 82           # côté d'un nœud
T = 34           # diamètre d'un terminal
X_NOEUD = MARGE + 14         # bord gauche des nœuds
X_AXE = X_NOEUD + N / 2      # axe vertical des fils
X_TEXTE = X_NOEUD + N + 30   # début des libellés
X_REJETS = 300               # branche des rejets, indentée sous l'axe

Y_DEPART = 330   # centre du premier nœud
PAS = 104        # espacement vertical entre deux étapes
DETOUR = 64      # place réservée à la branche des rejets, sous l'étape Spark

etapes = [
    dict(id="ing", ico="down", titre="ingestion_bronze",
         sous="15,3 M lignes · Parquet", runtime="API Data Fair"),
    dict(id="spk", ico="funnel", titre="spark_bronze_vers_silver",
         sous="déduplication · règles qualité", runtime="Spark 3.5.3 · Scala 2.12",
         branche=("silver/dpe_rejets", "720 483 lignes écartées")),
    dict(id="load", ico="db", titre="chargement_entrepot",
         sous="7 849 191 lignes", runtime="PostgreSQL 16"),
    dict(id="run", ico="layers", titre="dbt_run",
         sous="schéma en étoile", runtime="dbt-core 1.9.1"),
    dict(id="test", ico="check", titre="dbt_test",
         sous="42 réussis · 2 assumés"),
    dict(id="docs", ico="doc", titre="dbt_docs_generate",
         sous="lignage régénéré"),
    dict(id="exp", ico="out", genre="manuel", titre="export_powerbi.py",
         sous="CSV + reprojection WGS84"),
    dict(id="bi", ico="chart", genre="manuel", titre="Power BI Service",
         sous="96 départements cartographiés"),
]

# Un peu d'air avant la zone hors DAG.
RESPIRATION = 26
IDX_SPK = next(i for i, e in enumerate(etapes) if e["id"] == "spk")
for i, e in enumerate(etapes):
    # Tout ce qui suit l'étape Spark descend d'un cran : la branche des rejets
    # occupe sa propre rangée au lieu de traverser le libellé voisin.
    e["cy"] = (Y_DEPART + i * PAS
               + (DETOUR if i > IDX_SPK else 0)
               + (RESPIRATION if e.get("genre") == "manuel" else 0))
    cote = T if e.get("genre") == "terminal" else N
    e["cote"] = cote
    e["y"] = e["cy"] - cote / 2
    e["x"] = X_AXE - cote / 2


def bloc_svg():
    d = []
    for a, b in zip(etapes, etapes[1:]):
        y1 = a["cy"] + a["cote"] / 2
        y2 = b["cy"] - b["cote"] / 2
        pointille = b.get("genre") == "manuel" and a.get("genre") != "manuel"
        classe = "fil pointille" if pointille else "fil"
        d.append(f'<path class="{classe}" d="M {X_AXE} {y1:.1f} L {X_AXE} {y2:.1f}"/>')
        d.append(f'<circle class="port" cx="{X_AXE}" cy="{y1:.1f}" r="5"/>')
        d.append(f'<circle class="port" cx="{X_AXE}" cy="{y2:.1f}" r="5"/>')

    # la bifurcation : sortie latérale du job Spark vers les rejets
    spk = next(e for e in etapes if e["id"] == "spk")
    ax, ay = X_AXE, spk["cy"] + N / 2
    bx, by = X_REJETS, spk["cy"] + N / 2 + 60
    d.append(f'<path class="fil" d="M {ax:.1f} {ay:.1f} C {ax:.1f} {by:.1f}, '
             f'{bx - 60:.1f} {by:.1f}, {bx:.1f} {by:.1f}"/>')
    return (f'<svg class="fils" width="{LARGEUR}" height="{HAUTEUR}" '
            f'viewBox="0 0 {LARGEUR} {HAUTEUR}">' + "".join(d) + "</svg>")


def bloc_corps():
    d = []
    for e in etapes:
        genre = e.get("genre", "")
        classes = "noeud" + (f" {genre}" if genre else "")
        d.append(f'<div class="{classes}" style="left:{e["x"]:.1f}px;top:{e["y"]:.1f}px;'
                 f'width:{e["cote"]}px;height:{e["cote"]}px">'
                 f'{icone(e["ico"], 16 if genre == "terminal" else 34)}</div>')

        d.append(f'<div class="etq" style="left:{X_TEXTE}px;top:{e["cy"] - 26:.1f}px">'
                 f'<div class="t">{e["titre"]}</div>'
                 f'<div class="s">{e["sous"]}</div>'
                 + (f'<div class="rt">{e["runtime"]}</div>' if e.get("runtime") else "")
                 + "</div>")

    spk = next(e for e in etapes if e["id"] == "spk")
    by = spk["cy"] + N / 2 + 60
    d.append(f'<div class="rejets" style="left:{X_REJETS}px;top:{by - 32:.1f}px">'
             f'{icone("slash", 22, "var(--accent)")}'
             f'<div><div class="rt-t">silver/dpe_rejets</div>'
             f'<div class="rt-s">720 483 lignes écartées</div></div></div>')

    exp = next(e for e in etapes if e["id"] == "exp")
    bi = next(e for e in etapes if e["id"] == "bi")
    zy = exp["cy"] - N / 2 - 30
    zh = (bi["cy"] + N / 2 + 46) - zy
    d.append(f'<div class="zone" style="left:{MARGE - 8}px;top:{zy:.1f}px;'
             f'width:{LARGEUR - 2 * (MARGE - 8)}px;height:{zh:.1f}px"></div>')
    d.append(f'<div class="zone-tag" style="left:{MARGE + 22}px;top:{zy:.1f}px">'
             f'Hors DAG · lancé à la main</div>')
    return "".join(d)


def page(theme):
    sombre = theme == "sombre"
    j = dict(
        fond="#0b0e13" if sombre else "#eef0f3",
        pointille="#212936" if sombre else "#c9cfd8",
        surface="#161c24" if sombre else "#ffffff",
        surface2="#1d242e" if sombre else "#f5f7f9",
        encre="#e9edf2" if sombre else "#11161c",
        encre2="#a5b0bc" if sombre else "#495562",
        encre3="#76818f" if sombre else "#79838f",
        trait="#2a3340" if sombre else "#d6dae1",
        trait_fort="#3d4857" if sombre else "#b4bdc9",
        fil="#4d5a68" if sombre else "#a4aeba",
        accent="#ffc42b" if sombre else "#e0940a",
        ombre=("0 2px 4px rgba(0,0,0,.55), 0 8px 20px rgba(0,0,0,.4)" if sombre
               else "0 1px 2px rgba(17,22,28,.07), 0 5px 14px rgba(17,22,28,.07)"),
    )
    jetons = "".join(f"--{k}:{v};" for k, v in j.items())

    return f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<style>
{polices_embarquees()}
:root{{{jetons}}}
*{{box-sizing:border-box;margin:0;padding:0}}
html,body{{width:{LARGEUR}px;height:{HAUTEUR}px;overflow:hidden}}
body{{background:
    radial-gradient(var(--pointille) 1.4px, transparent 1.4px) 0 0 / 30px 30px,
    var(--fond);
  color:var(--encre);font-family:'Public Sans',sans-serif;
  -webkit-font-smoothing:antialiased;position:relative}}

.tete{{position:absolute;left:{MARGE}px;top:74px;right:{MARGE}px}}
.kick{{font-family:'JetBrains Mono',monospace;font-size:15px;font-weight:500;
  letter-spacing:.22em;text-transform:uppercase;color:var(--encre3);margin-bottom:16px}}
h1{{font-family:'Bricolage Grotesque',sans-serif;font-weight:800;font-size:52px;
  line-height:1.03;letter-spacing:-.028em;margin-bottom:14px}}
.sous{{font-size:20px;line-height:1.45;color:var(--encre2);max-width:44ch}}

.fils{{position:absolute;inset:0;pointer-events:none}}
.fil{{fill:none;stroke:var(--fil);stroke-width:2.4}}
.fil.pointille{{stroke-dasharray:8 7;stroke-width:2}}
.port{{fill:var(--surface);stroke:var(--fil);stroke-width:2.2}}

.noeud{{position:absolute;border-radius:14px;background:var(--surface);
  border:1.5px solid var(--trait_fort);box-shadow:var(--ombre);
  display:flex;align-items:center;justify-content:center;color:var(--encre)}}
.noeud.terminal{{border-radius:50%;background:var(--surface2);color:var(--encre3)}}
.noeud.manuel{{border-style:dashed}}

.etq{{position:absolute}}
.etq .t{{font-family:'JetBrains Mono',monospace;font-size:22px;font-weight:500;
  letter-spacing:-.015em;color:var(--encre);line-height:1.25}}
.etq .s{{font-size:17px;color:var(--encre3);margin-top:4px;line-height:1.3}}
.etq .rt{{font-family:'JetBrains Mono',monospace;font-size:14px;color:var(--encre2);
  margin-top:8px;display:inline-block;border:1.2px solid var(--trait);
  border-radius:999px;padding:2px 11px;background:var(--surface)}}

.rejets{{position:absolute;display:flex;align-items:center;gap:12px;
  background:var(--surface);border:1.5px solid var(--trait);border-radius:12px;
  box-shadow:var(--ombre);padding:14px 18px}}
.rt-t{{font-family:'JetBrains Mono',monospace;font-size:17px;font-weight:500}}
.rt-s{{font-size:14px;color:var(--encre3);margin-top:3px}}

.zone{{position:absolute;border:1.6px dashed var(--trait_fort);border-radius:18px}}
.zone-tag{{position:absolute;transform:translateY(-50%);
  font-family:'JetBrains Mono',monospace;font-size:14px;letter-spacing:.14em;
  text-transform:uppercase;color:var(--encre3);background:var(--fond);padding:0 12px}}

.pied{{position:absolute;left:{MARGE}px;right:{MARGE}px;bottom:54px;
  display:flex;justify-content:space-between;align-items:baseline;
  font-family:'JetBrains Mono',monospace;font-size:15px;color:var(--encre3)}}
.pied b{{color:var(--encre);font-weight:500}}
</style></head><body>

<div class="tete">
  <div class="kick">DPE Lakehouse · orchestration Airflow</div>
  <h1>Six tâches,<br>une seule bifurcation</h1>
  <div class="sous">Le DAG tel qu'il tourne. Volumes relevés en base.</div>
</div>

{bloc_svg()}
{bloc_corps()}

<div class="pied">
  <span><b>Eben Ezer NGBLOGNI</b></span>
  <span>github.com/ebenezer-ngblogni/dpe-lakehouse</span>
</div>

</body></html>"""




# ── Rendu PNG ────────────────────────────────────────────────────────────
def rendre(source: Path, cible: Path, largeur: int, hauteur: int) -> None:
    """Capture la page avec Chrome headless, puis réduit à la taille cible.

    Deux précautions :

    1. La fenêtre est demandée plus haute que la page. Le viewport de Chrome
       headless fait une centaine de pixels de moins que `--window-size`, et
       tout ce qui dépasse n'est simplement pas peint — sans erreur, sans
       barre de défilement. On rend donc large puis on recadre.
    2. La capture se fait à l'échelle 2 et on rééchantillonne en Lanczos.
       Le texte reste net sans livrer une image de 4800 px de large.
    """
    import subprocess, shutil

    marge_viewport = 160
    brut = cible.with_name(cible.stem + "-brut.png")
    chrome = next((c for c in ("/opt/google/chrome/chrome", "google-chrome", "chromium")
                   if shutil.which(c) or Path(c).exists()), None)
    if chrome is None:
        raise SystemExit("Chrome introuvable : impossible de rendre le PNG.")

    subprocess.run([
        chrome, "--headless", "--disable-gpu", "--no-sandbox", "--hide-scrollbars",
        "--force-device-scale-factor=2",
        f"--window-size={largeur},{hauteur + marge_viewport}",
        "--virtual-time-budget=8000",
        f"--screenshot={brut}", source.resolve().as_uri(),
    ], check=True, capture_output=True)

    subprocess.run([
        "convert", str(brut),
        "-crop", f"{largeur * 2}x{hauteur * 2}+0+0", "+repage",
        "-filter", "Lanczos", "-resize", f"{largeur}x{hauteur}",
        "-strip", str(cible),
    ], check=True)
    brut.unlink()
    print(f"  {cible.name}  {largeur}x{hauteur}")


if __name__ == "__main__":
    sortie = ICI.parent / "docs" / "img"
    sortie.mkdir(parents=True, exist_ok=True)
    for theme in ("clair", "sombre"):
        html = ICI / f"portrait-{theme}.html"
        html.write_text(page(theme))
        rendre(html, sortie / f"architecture-portrait-{theme}.png", LARGEUR, HAUTEUR)
    dernier = etapes[-1]
    print(f"\nbas du dernier nœud : {dernier['cy'] + dernier['cote'] / 2:.0f} px "
          f"sur {HAUTEUR} — pied à {HAUTEUR - 54}")
