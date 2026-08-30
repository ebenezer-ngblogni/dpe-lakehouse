#!/usr/bin/env python3
"""Génère le canevas d'architecture du pipeline DPE en PNG.

Les positions sont calculées ici plutôt qu'écrites à la main : déplacer une
étape ne demande pas de redessiner les fils. Le HTML produit est entièrement
statique — aucun script à l'exécution — pour que le rendu Chrome soit
déterministe.
"""

from pathlib import Path

ICI = Path(__file__).parent


def polices_embarquees() -> str:
    """Renvoie les @font-face en data-URI, en les téléchargeant au besoin.

    Les polices sont embarquées plutôt que liées : le rendu doit être
    identique hors ligne, et Chrome headless ne doit pas dépendre du réseau
    au moment de la capture.
    """
    cache = ICI / "polices.css"
    if cache.exists():
        return cache.read_text()

    import re, base64, urllib.request
    ua = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/126.0.0.0 Safari/537.36")
    url = ("https://fonts.googleapis.com/css2?"
           "family=Bricolage+Grotesque:opsz,wght@12..96,600;12..96,800"
           "&family=Public+Sans:wght@400;600"
           "&family=JetBrains+Mono:wght@400;500;700&display=swap")
    css = urllib.request.urlopen(
        urllib.request.Request(url, headers={"User-Agent": ua}), timeout=30).read().decode()

    blocs, sortie = re.findall(r"/\*\s*([\w-]+)\s*\*/\s*@font-face\s*\{(.*?)\}", css, re.S), []
    for sous_ens, corps in blocs:
        if sous_ens not in {"latin", "latin-ext"}:
            continue
        fam = re.search(r"font-family:\s*'([^']+)'", corps).group(1)
        pds = re.search(r"font-weight:\s*([\d\s]+);", corps).group(1).strip()
        src = re.search(r"url\((https://[^)]+\.woff2)\)", corps).group(1)
        rng = re.search(r"unicode-range:\s*([^;]+);", corps).group(1).strip()
        data = urllib.request.urlopen(
            urllib.request.Request(src, headers={"User-Agent": ua}), timeout=30).read()
        sortie.append(
            f"@font-face{{font-family:'{fam}';font-style:normal;font-weight:{pds};"
            f"font-display:block;src:url(data:font/woff2;base64,"
            f"{base64.b64encode(data).decode()}) format('woff2');unicode-range:{rng};}}")
    cache.write_text("\n".join(sortie))
    return cache.read_text()
LARGEUR, HAUTEUR = 2400, 1350

# ── Géométrie ────────────────────────────────────────────────────────────
MARGE = 120
N = 132          # côté d'un nœud de tâche
T = 52           # diamètre d'un terminal
Y_SPINE = 580    # haut des nœuds de la ligne principale
Y_RUNTIME = 372  # haut des pastilles runtime
R = 84           # diamètre d'une pastille runtime
Y_BAS = 930      # haut des nœuds de la bande basse
DW, DH = 486, 104   # nœud de données (large)

utile = LARGEUR - 2 * MARGE
elements = [T, N, N, N, N, N, N, T]          # debut, 6 tâches, fin
ecart = (utile - sum(elements)) / (len(elements) - 1)

xs, curseur = [], MARGE
for taille in elements:
    xs.append(curseur)
    curseur += taille + ecart

# ── Icônes : formes géométriques, dessinées pour ce qu'elles font ────────
ICONES = {
    "dot":    '<circle cx="12" cy="12" r="3.6" fill="currentColor" stroke="none"/>',
    "down":   '<path d="M12 3v11m0 0 4.5-4.5M12 14l-4.5-4.5"/>'
              '<path d="M4 17v2a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-2"/>',
    "funnel": '<path d="M3 4h18l-7 8.2V19l-4 2v-8.8L3 4Z"/>',
    "db":     '<ellipse cx="12" cy="6" rx="7.6" ry="3"/>'
              '<path d="M4.4 6v12c0 1.7 3.4 3 7.6 3s7.6-1.3 7.6-3V6"/>'
              '<path d="M4.4 12c0 1.7 3.4 3 7.6 3s7.6-1.3 7.6-3"/>',
    "layers": '<path d="M12 3 3 7.5 12 12l9-4.5L12 3Z"/>'
              '<path d="m3 12.6 9 4.5 9-4.5"/><path d="m3 17.2 9 4.5 9-4.5"/>',
    "check":  '<circle cx="12" cy="12" r="8.6"/><path d="m8.2 12.3 2.6 2.6 5-5.4"/>',
    "doc":    '<path d="M6 3h8l5 5v13a1 1 0 0 1-1 1H6a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1Z"/>'
              '<path d="M14 3v5h5M8.6 13.4h6.8M8.6 17h6.8"/>',
    "out":    '<path d="M13 4H6a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-7"/>'
              '<path d="M20 4h-6m6 0v6m0-6-8.4 8.4"/>',
    "chart":  '<path d="M4 20V9.6M10 20V4M16 20v-7.2M22 20H2"/>',
    "slash":  '<circle cx="12" cy="12" r="8.6"/><path d="m7.4 16.6 9.2-9.2"/>',
}


def icone(cle, taille, couleur="currentColor"):
    return (f'<svg width="{taille}" height="{taille}" viewBox="0 0 24 24" fill="none" '
            f'stroke="{couleur}" stroke-width="1.7" stroke-linecap="round" '
            f'stroke-linejoin="round">{ICONES[cle]}</svg>')


# ── Les nœuds ────────────────────────────────────────────────────────────
noeuds = [
    dict(id="debut", x=xs[0], y=Y_SPINE + (N - T) / 2, w=T, h=T, genre="terminal",
         ico="dot", titre="debut", sous="EmptyOperator"),
    dict(id="ing", x=xs[1], y=Y_SPINE, w=N, h=N, ico="down",
         titre="ingestion_bronze", sous="15,3 M lignes · Parquet"),
    dict(id="spk", x=xs[2], y=Y_SPINE, w=N, h=N, ico="funnel",
         titre="spark_bronze_vers_silver", sous="déduplication · règles qualité"),
    dict(id="load", x=xs[3], y=Y_SPINE, w=N, h=N, ico="db",
         titre="chargement_entrepot", sous="7 849 191 lignes"),
    dict(id="run", x=xs[4], y=Y_SPINE, w=N, h=N, ico="layers",
         titre="dbt_run", sous="schéma en étoile"),
    dict(id="test", x=xs[5], y=Y_SPINE, w=N, h=N, ico="check",
         titre="dbt_test", sous="42 réussis · 2 assumés"),
    dict(id="docs", x=xs[6], y=Y_SPINE, w=N, h=N, ico="doc",
         titre="dbt_docs_generate", sous="lignage régénéré"),
    dict(id="fin", x=xs[7], y=Y_SPINE + (N - T) / 2, w=T, h=T, genre="terminal",
         ico="dot", titre="fin", sous="EmptyOperator"),
]

# bande basse : les rejets à gauche, la restitution manuelle à droite
X_REJ = xs[2] + N / 2 - DW / 2 + 40
noeuds.append(dict(id="rej", x=X_REJ, y=Y_BAS, w=DW, h=DH, genre="data", ico="slash",
                   dtitre="silver/dpe_rejets", dsous="720 483 lignes écartées, motif conservé"))

X_EXP = xs[5] + 96
X_BI = X_EXP + N + 200
noeuds.append(dict(id="exp", x=X_EXP, y=Y_BAS - 14, w=N, h=N, genre="manuel", ico="out",
                   titre="export_powerbi.py", sous="CSV + reprojection WGS84"))
noeuds.append(dict(id="bi", x=X_BI, y=Y_BAS - 14, w=N, h=N, genre="manuel", ico="chart",
                   titre="Power BI Service", sous="96 départements"))

par_id = {n["id"]: n for n in noeuds}

# ── Runtimes accrochés au-dessus des tâches ──────────────────────────────
runtimes = [
    dict(sous="ing", txt="ADEME", tag="API Data Fair"),
    dict(sous="spk", txt="Spark", tag="3.5.3 · Scala 2.12"),
    dict(sous="load", txt="PgSQL", tag="PostgreSQL 16"),
    dict(sous="run", txt="dbt", tag="dbt-core 1.9.1"),
]

# ── Les liens ────────────────────────────────────────────────────────────
liens = [
    ("debut", "ing", None, "plein"),
    ("ing", "spk", None, "plein"),
    ("spk", "load", "courant", "plein"),
    ("load", "run", None, "plein"),
    ("run", "test", None, "plein"),
    ("test", "docs", None, "plein"),
    ("docs", "fin", None, "plein"),
]


def port_droit(n):
    return n["x"] + n["w"], n["y"] + n["h"] / 2


def port_gauche(n):
    return n["x"], n["y"] + n["h"] / 2


def port_bas(n):
    return n["x"] + n["w"] / 2, n["y"] + n["h"]


def port_haut(n):
    return n["x"] + n["w"] / 2, n["y"]


chemins, ports, etiquettes_fil = [], [], []

for a_id, b_id, tag, style in liens:
    a, b = par_id[a_id], par_id[b_id]
    ax, ay = port_droit(a)
    bx, by = port_gauche(b)
    dx = max(50, abs(bx - ax) * 0.42)
    chemins.append((f"M {ax:.1f} {ay:.1f} C {ax + dx:.1f} {ay:.1f}, "
                    f"{bx - dx:.1f} {by:.1f}, {bx:.1f} {by:.1f}", style))
    ports += [(ax, ay), (bx, by)]
    if tag:
        etiquettes_fil.append(((ax + bx) / 2, ay - 26, tag))

# rejets : sortie basse du job Spark, la seule vraie bifurcation
sx, sy = port_bas(par_id["spk"])
rx, ry = port_haut(par_id["rej"])
chemins.append((f"M {sx:.1f} {sy:.1f} C {sx:.1f} {sy + 150:.1f}, "
                f"{rx:.1f} {ry - 150:.1f}, {rx:.1f} {ry:.1f}", "plein"))
ports += [(sx, sy), (rx, ry)]
etiquettes_fil.append((sx - 62, sy + 108, "rejets"))

# fin → export : hors DAG, donc en pointillés
fx, fy = port_bas(par_id["fin"])
ex, ey = port_haut(par_id["exp"])
chemins.append((f"M {fx:.1f} {fy:.1f} C {fx:.1f} {fy + 170:.1f}, "
                f"{ex:.1f} {ey - 170:.1f}, {ex:.1f} {ey:.1f}", "pointille"))
ports += [(fx, fy), (ex, ey)]

# export → Power BI
ax, ay = port_droit(par_id["exp"])
bx, by = port_gauche(par_id["bi"])
chemins.append((f"M {ax:.1f} {ay:.1f} C {ax + 90:.1f} {ay:.1f}, "
                f"{bx - 90:.1f} {by:.1f}, {bx:.1f} {by:.1f}", "plein"))
ports += [(ax, ay), (bx, by)]

# fils pointillés vers les runtimes
for r in runtimes:
    n = par_id[r["sous"]]
    x = n["x"] + n["w"] / 2
    chemins.append((f"M {x:.1f} {Y_RUNTIME + R:.1f} L {x:.1f} {n['y']:.1f}", "pointille"))


# ── Émission ─────────────────────────────────────────────────────────────
def bloc_svg():
    d = []
    for chemin, style in chemins:
        classe = "fil pointille" if style == "pointille" else "fil"
        d.append(f'<path class="{classe}" d="{chemin}"/>')
    for px, py in ports:
        d.append(f'<circle class="port" cx="{px:.1f}" cy="{py:.1f}" r="6"/>')
    return (f'<svg class="fils" width="{LARGEUR}" height="{HAUTEUR}" '
            f'viewBox="0 0 {LARGEUR} {HAUTEUR}">' + "".join(d) + "</svg>")


def bloc_noeuds():
    d = []
    for n in noeuds:
        genre = n.get("genre", "")
        classes = "noeud" + (f" {genre}" if genre else "")
        style = f"left:{n['x']:.1f}px;top:{n['y']:.1f}px;width:{n['w']}px;height:{n['h']}px"

        if genre == "data":
            gl = icone(n["ico"], 34, "var(--accent)")
            interieur = (f'{gl}<div class="dtxt"><div class="dt">{n["dtitre"]}</div>'
                         f'<div class="ds">{n["dsous"]}</div></div>')
        elif genre == "terminal":
            interieur = icone(n["ico"], 22)
        else:
            interieur = icone(n["ico"], 48)

        d.append(f'<div class="{classes}" style="{style}">{interieur}</div>')

        if n.get("titre"):
            cx = n["x"] + n["w"] / 2
            cy = Y_SPINE + N + 22 if n.get("genre") != "manuel" else n["y"] + n["h"] + 22
            d.append(f'<div class="etq" style="left:{cx:.1f}px;top:{cy:.1f}px">'
                     f'<div class="t">{n["titre"]}</div>'
                     f'<div class="s">{n["sous"]}</div></div>')

    for r in runtimes:
        n = par_id[r["sous"]]
        cx = n["x"] + n["w"] / 2
        d.append(f'<div class="runtime" style="left:{cx - R / 2:.1f}px;'
                 f'top:{Y_RUNTIME}px;width:{R}px;height:{R}px">{r["txt"]}</div>')
        d.append(f'<div class="etq" style="left:{cx:.1f}px;top:{Y_RUNTIME - 44}px">'
                 f'<div class="s">{r["tag"]}</div></div>')

    for x, y, txt in etiquettes_fil:
        d.append(f'<div class="fil-etq" style="left:{x:.1f}px;top:{y:.1f}px">{txt}</div>')

    # cartouche « hors DAG »
    zx, zy = X_EXP - 56, Y_BAS - 84
    zw, zh = (X_BI + N + 56) - zx, 296
    d.append(f'<div class="zone" style="left:{zx:.1f}px;top:{zy}px;'
             f'width:{zw:.1f}px;height:{zh}px"></div>')
    d.append(f'<div class="zone-tag" style="left:{zx + 34:.1f}px;top:{zy}px">'
             f'Hors DAG · lancé à la main</div>')
    return "".join(d)


def page(theme):
    sombre = theme == "sombre"
    jetons = dict(
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
        ombre=("0 2px 4px rgba(0,0,0,.55), 0 10px 26px rgba(0,0,0,.4)" if sombre
               else "0 1px 2px rgba(17,22,28,.07), 0 6px 18px rgba(17,22,28,.07)"),
    )
    css_jetons = "".join(f"--{k}:{v};" for k, v in jetons.items())
    polices = polices_embarquees()

    return f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<style>
{polices}
:root{{{css_jetons}}}
*{{box-sizing:border-box;margin:0;padding:0}}
html,body{{width:{LARGEUR}px;height:{HAUTEUR}px;overflow:hidden}}
body{{
  background:
    radial-gradient(var(--pointille) 1.6px, transparent 1.6px) 0 0 / 34px 34px,
    var(--fond);
  color:var(--encre);
  font-family:'Public Sans',sans-serif;
  -webkit-font-smoothing:antialiased;
  text-rendering:geometricPrecision;
  position:relative;
}}

/* ── en-tête ── */
.tete{{position:absolute;left:{MARGE}px;top:86px;right:{MARGE}px;
  display:flex;justify-content:space-between;align-items:flex-start;gap:60px}}
.kick{{font-family:'JetBrains Mono',monospace;font-size:19px;font-weight:500;
  letter-spacing:.24em;text-transform:uppercase;color:var(--encre3);margin-bottom:22px}}
h1{{font-family:'Bricolage Grotesque',sans-serif;font-weight:800;font-size:72px;
  line-height:1.02;letter-spacing:-.028em;margin-bottom:18px}}
.sous{{font-size:26px;line-height:1.5;color:var(--encre2);max-width:60ch}}
.chiffres{{display:flex;gap:52px;flex:none;padding-top:8px}}
.chiffre .n{{font-family:'Bricolage Grotesque',sans-serif;font-weight:800;font-size:44px;
  line-height:1;letter-spacing:-.03em;font-variant-numeric:tabular-nums}}
.chiffre .l{{font-family:'JetBrains Mono',monospace;font-size:15px;letter-spacing:.09em;
  text-transform:uppercase;color:var(--encre3);margin-top:10px}}

/* ── fils ── */
.fils{{position:absolute;inset:0;pointer-events:none}}
.fil{{fill:none;stroke:var(--fil);stroke-width:2.6}}
.fil.pointille{{stroke-dasharray:9 8;stroke-width:2.2}}
.port{{fill:var(--surface);stroke:var(--fil);stroke-width:2.4}}

/* ── nœuds ── */
.noeud{{position:absolute;border-radius:18px;background:var(--surface);
  border:1.6px solid var(--trait_fort);box-shadow:var(--ombre);
  display:flex;align-items:center;justify-content:center;color:var(--encre)}}
.noeud.terminal{{border-radius:50%;background:var(--surface2);color:var(--encre3)}}
.noeud.manuel{{border-style:dashed}}
.noeud.data{{border-radius:14px;justify-content:flex-start;gap:20px;padding:0 26px;
  border-color:var(--trait)}}
.dtxt{{display:flex;flex-direction:column;gap:6px}}
.dt{{font-family:'JetBrains Mono',monospace;font-size:23px;font-weight:500;
  letter-spacing:-.01em}}
.ds{{font-size:18px;color:var(--encre3);line-height:1.3}}

/* ── étiquettes ── */
.etq{{position:absolute;transform:translateX(-50%);text-align:center;white-space:nowrap}}
.etq .t{{font-family:'JetBrains Mono',monospace;font-size:22px;font-weight:500;
  letter-spacing:-.015em;color:var(--encre)}}
.etq .s{{font-size:18px;color:var(--encre3);margin-top:6px;line-height:1.3}}

.runtime{{position:absolute;border-radius:50%;background:var(--surface);
  border:1.6px solid var(--trait_fort);box-shadow:var(--ombre);
  display:flex;align-items:center;justify-content:center;
  font-family:'JetBrains Mono',monospace;font-size:19px;font-weight:700;
  color:var(--encre2);letter-spacing:-.02em}}

.fil-etq{{position:absolute;transform:translate(-50%,-50%);
  font-family:'JetBrains Mono',monospace;font-size:18px;color:var(--encre3);
  background:var(--fond);padding:2px 10px;white-space:nowrap}}

.zone{{position:absolute;border:1.8px dashed var(--trait_fort);border-radius:22px}}
.zone-tag{{position:absolute;transform:translateY(-50%);
  font-family:'JetBrains Mono',monospace;font-size:17px;letter-spacing:.14em;
  text-transform:uppercase;color:var(--encre3);background:var(--fond);padding:0 14px}}

/* ── légende + signature ── */
.legende{{position:absolute;left:{MARGE}px;bottom:110px;display:flex;gap:44px;
  align-items:center;font-size:19px;color:var(--encre2)}}
.li{{display:flex;align-items:center;gap:14px}}
.pion{{width:34px;height:24px;border-radius:7px;background:var(--surface);
  border:1.6px solid var(--trait_fort);flex:none}}
.pion.rond{{width:24px;border-radius:50%}}
.pion.tirets{{border-style:dashed}}
.pion.fil{{height:0;width:34px;border:0;border-top:2.6px solid var(--fil);border-radius:0}}
.pion.fil.tirets{{border-top-style:dashed}}

.signature{{position:absolute;right:{MARGE}px;bottom:110px;text-align:right;
  font-family:'JetBrains Mono',monospace;font-size:18px;color:var(--encre3);line-height:1.6}}
.signature b{{color:var(--encre);font-weight:500}}
</style></head><body>

<div class="tete">
  <div>
    <div class="kick">DPE Lakehouse · orchestration Airflow</div>
    <h1>Six tâches, une seule bifurcation</h1>
    <div class="sous">Le DAG tel qu'il tourne réellement. Les volumes sont relevés en base.</div>
  </div>
  <div class="chiffres">
    <div class="chiffre"><div class="n">15,3 M</div><div class="l">lignes ingérées</div></div>
    <div class="chiffre"><div class="n">7,85 M</div><div class="l">en entrepôt</div></div>
    <div class="chiffre"><div class="n">74</div><div class="l">tests</div></div>
  </div>
</div>

{bloc_svg()}
{bloc_noeuds()}

<div class="legende">
  <span class="li"><span class="pion"></span>Tâche Airflow</span>
  <span class="li"><span class="pion rond"></span>Runtime mobilisé</span>
  <span class="li"><span class="pion tirets"></span>Hors DAG</span>
  <span class="li"><span class="pion fil"></span>Dépendance</span>
  <span class="li"><span class="pion fil tirets"></span>Lancement manuel</span>
</div>

<div class="signature">
  <b>Eben Ezer NGBLOGNI</b><br>github.com/ebenezer-ngblogni/dpe-lakehouse
</div>

</body></html>"""


# Sous garde : le module est aussi importé par le générateur portrait,
# qui réutilise les icônes et l'embarquement des polices.


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
        html = ICI / f"canvas-{theme}.html"
        html.write_text(page(theme))
        rendre(html, sortie / f"architecture-{theme}.png", LARGEUR, HAUTEUR)

    print(f"\ngéométrie : {len(noeuds)} nœuds, {len(chemins)} fils, "
          f"écart horizontal {ecart:.0f} px")
