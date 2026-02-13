import os
import sys
import json
import textwrap
import sqlite3
from datetime import datetime
from dateutil.relativedelta import relativedelta
import urllib.request
import urllib.error

import pandas as pd
import qrcode
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox, filedialog, ttk


# =======================
#   CONFIG / PATHS
# =======================
def get_app_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


APP_DIR = get_app_dir()

EXCEL_PATH = os.path.join(APP_DIR, "productos.xlsx")
HOJA_PRODUCTOS = "productos"
CACHE_FILE = os.path.join(APP_DIR, "config.json")
DB_PATH = os.path.join(APP_DIR, "talca_qr.db")


# =======================
#   GOOGLE SHEETS (WEBHOOK)
# =======================
SHEETS_WEBAPP_URL = "https://script.google.com/macros/s/AKfycby1SckKP_PFOFs_ynw1yVnyxCnuJsvf3hi0jX92egBRLb3IHDkkitEyWLbk_II-A-_h/exec"
SHEETS_API_KEY = "TALCA-QR-2026"  # Debe coincidir con API_KEY en Apps Script


# =======================
#   CACHE
# =======================
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_cache(data: dict):
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except:
        pass


# =======================
#   NORMALIZACIONES
# =======================
def normalize_id_value(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return ""
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except:
        pass
    return s


def normalize_date_iso(s: str) -> str:
    if not s:
        return ""
    s = str(s).strip()
    if "/" in s:
        try:
            d = datetime.strptime(s, "%d/%m/%y").date()
            return d.isoformat()
        except:
            return s
    return s


# =======================
#   SQLITE
# =======================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    # Base de scans
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pallet_scans (
        descripcion   TEXT NOT NULL,
        nro_serie     INTEGER NOT NULL,
        id_producto   TEXT NOT NULL,
        lote          TEXT NOT NULL,
        creacion      TEXT NOT NULL,
        vencimiento   TEXT NOT NULL,
        UNIQUE(id_producto, nro_serie, lote)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lote ON pallet_scans(lote)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_id_producto ON pallet_scans(id_producto)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_nro_serie ON pallet_scans(nro_serie)")

    # Estado por pallet (completo/parcial + packs)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pallet_status (
        id_producto   TEXT NOT NULL,
        lote          TEXT NOT NULL,
        nro_serie     INTEGER NOT NULL,
        is_full       INTEGER NOT NULL,   -- 1 completo / 0 parcial
        packs_partial INTEGER NOT NULL,   -- packs si parcial, 0 si completo
        updated_at    TEXT NOT NULL,
        UNIQUE(id_producto, lote, nro_serie)
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_status_prod_lote ON pallet_status(id_producto, lote)")

    # outbox a sheets
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sheets_outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payload TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """)

    conn.commit()
    return conn


def outbox_count(conn):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM sheets_outbox")
    return int(cur.fetchone()[0])


def queue_outbox(conn, payload: dict):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO sheets_outbox(payload, created_at) VALUES (?, ?)",
        (json.dumps(payload, ensure_ascii=False), datetime.now().isoformat(timespec="seconds"))
    )
    conn.commit()


def send_to_sheets(payload: dict):
    if not SHEETS_WEBAPP_URL:
        raise RuntimeError("SHEETS_WEBAPP_URL no configurada.")

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        SHEETS_WEBAPP_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            txt = resp.read().decode("utf-8", errors="ignore")
            try:
                return json.loads(txt)
            except:
                return {"ok": False, "raw": txt}

    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except:
            body = ""
        raise RuntimeError(f"HTTP {e.code}: {body or e.reason}")


def flush_outbox(conn):
    cur = conn.cursor()
    cur.execute("SELECT id, payload FROM sheets_outbox ORDER BY id ASC LIMIT 50")
    rows = cur.fetchall()

    sent = 0
    for rid, payload_str in rows:
        payload = json.loads(payload_str)
        res = send_to_sheets(payload)
        if isinstance(res, dict) and res.get("ok") is True:
            cur.execute("DELETE FROM sheets_outbox WHERE id = ?", (rid,))
            conn.commit()
            sent += 1
        else:
            break
    return sent


# =======================
#   PARSEO QR
# =======================
def parse_qr_payload(raw: str) -> dict:
    """
    Formato:
    NS=000001|PRD=12|DSC=Descripcion...|LOT=090226|FEC=2026-02-09|VTO=2026-08-09
    """
    raw = raw.strip()

    if "|" in raw and "=" in raw:
        parts = raw.split("|")
        data = {}
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                data[k.strip()] = v.strip()

        required = ["NS", "PRD", "DSC", "LOT", "FEC", "VTO"]
        missing = [k for k in required if k not in data or not data[k]]
        if missing:
            raise ValueError(f"QR inválido, faltan campos: {', '.join(missing)}")

        return {
            "descripcion": data["DSC"],
            "nro_serie": int(data["NS"]),
            "id_producto": normalize_id_value(data["PRD"]),
            "lote": str(data["LOT"]).strip(),
            "creacion": normalize_date_iso(data["FEC"]),
            "vencimiento": normalize_date_iso(data["VTO"]),
        }

    raise ValueError("QR inválido: formato no reconocido.")


# =======================
#   GUARDAR SCAN + ESTADO
# =======================
def save_single_scan(conn: sqlite3.Connection, data: dict) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO pallet_scans (descripcion, nro_serie, id_producto, lote, creacion, vencimiento)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            data["descripcion"],
            int(data["nro_serie"]),
            data["id_producto"],
            data["lote"],
            data["creacion"],
            data["vencimiento"]
        ))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def upsert_pallet_status(conn: sqlite3.Connection, id_producto: str, lote: str, nro_serie: int, is_full: int, packs_partial: int):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pallet_status(id_producto, lote, nro_serie, is_full, packs_partial, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id_producto, lote, nro_serie) DO UPDATE SET
            is_full = excluded.is_full,
            packs_partial = excluded.packs_partial,
            updated_at = excluded.updated_at
    """, (
        id_producto, lote, int(nro_serie), int(is_full), int(packs_partial),
        datetime.now().isoformat(timespec="seconds")
    ))
    conn.commit()


def fetch_latest_scans(conn: sqlite3.Connection, limit=300):
    cur = conn.cursor()
    cur.execute("""
        SELECT descripcion, nro_serie, id_producto, lote, creacion, vencimiento
        FROM pallet_scans
        ORDER BY lote ASC, id_producto ASC, nro_serie ASC
        LIMIT ?
    """, (limit,))
    return cur.fetchall()


# =======================
#   TOTALES PARA SHEETS
# =======================
def get_base_desc_for_lote(conn: sqlite3.Connection, id_producto: str, lote: str) -> str:
    cur = conn.cursor()
    cur.execute("""
        SELECT descripcion
        FROM pallet_scans
        WHERE id_producto = ? AND lote = ?
        ORDER BY nro_serie ASC
        LIMIT 1
    """, (id_producto, lote))
    row = cur.fetchone()
    return (row[0] if row else "").strip()


def compute_totals_for_product_lote(conn: sqlite3.Connection, id_producto: str, lote: str):
    cur = conn.cursor()

    # Pallets: cuenta TODOS los pallets escaneados (completos + parciales)
    # Si querés solo completos: agregá AND is_full = 1
    cur.execute("""
        SELECT COUNT(*)
        FROM pallet_status
        WHERE id_producto = ? AND lote = ?
    """, (id_producto, lote))
    pallets_total = int(cur.fetchone()[0] or 0)

    # Packs aclarados: suma solo los parciales
    cur.execute("""
        SELECT COALESCE(SUM(packs_partial), 0)
        FROM pallet_status
        WHERE id_producto = ? AND lote = ? AND is_full = 0
    """, (id_producto, lote))
    packs_aclarados = int(cur.fetchone()[0] or 0)

    desc = get_base_desc_for_lote(conn, id_producto, lote)

    return pallets_total, packs_aclarados, desc


def build_payload_for_product_lote(conn, id_producto: str, lote: str):
    pallets_total, packs_aclarados, desc = compute_totals_for_product_lote(conn, id_producto, lote)
    return {
        "api_key": SHEETS_API_KEY,
        "type": "scan",
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "qr": {
            "id_producto": id_producto,
            "descripcion": desc,
            "lote": lote,
        },
        "stock": {
            "id_producto": id_producto,
            "descripcion": desc,
            "lote": lote,
            "stock_total": int(pallets_total),         # pallets
            "packs_aclarados": int(packs_aclarados)    # packs parciales declarados
        }
    }


def sync_product_lote_to_sheets(conn, id_producto: str, lote: str):
    payload = build_payload_for_product_lote(conn, id_producto, lote)
    queue_outbox(conn, payload)
    sent = flush_outbox(conn)
    return sent


def build_full_snapshot_rows(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT id_producto, lote
        FROM pallet_scans
        ORDER BY id_producto ASC, lote ASC
    """)
    pairs = cur.fetchall()

    rows = []
    for pid, lote in pairs:
        pid = normalize_id_value(pid)
        lote = str(lote).strip()
        pallets_total, packs_aclarados, desc = compute_totals_for_product_lote(conn, pid, lote)
        if pid and lote and desc:
            rows.append({
                "id_producto": pid,
                "lote": lote,
                "descripcion": desc,
                "pallets": int(pallets_total),
                "packs_aclarados": int(packs_aclarados)
            })
    return rows


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


# =======================
#   EXCEL PRODUCTOS
# =======================
def cargar_productos():
    if not os.path.exists(EXCEL_PATH):
        raise FileNotFoundError(f"No encuentro {EXCEL_PATH}. Debe estar junto al ejecutable/script.")
    df = pd.read_excel(EXCEL_PATH, sheet_name=HOJA_PRODUCTOS)
    df.columns = df.columns.str.strip()
    if "id_producto" in df.columns:
        df["id_producto"] = df["id_producto"].apply(normalize_id_value)
    return df


def guardar_productos(df):
    df.to_excel(EXCEL_PATH, sheet_name=HOJA_PRODUCTOS, index=False)


def obtener_productos():
    df = cargar_productos()
    return list(zip(df["id_producto"], df["descripcion"]))


def dividir_texto(texto, max_caracteres):
    return textwrap.wrap(str(texto), width=max_caracteres)


# =======================
#   PDF QRS
# =======================
def generar_y_imprimir_qrs(id_producto, descripcion, cantidad):
    df = cargar_productos()
    id_producto = normalize_id_value(id_producto)

    fila = df[df["id_producto"] == id_producto].index
    if fila.empty:
        messagebox.showerror("Error", "Producto no encontrado.")
        return

    nro_serie = int(df.loc[fila[0], "ultimo_nro_serie"])

    fecha_actual = datetime.now()
    fec_iso = fecha_actual.strftime("%Y-%m-%d")
    vto_iso = (fecha_actual + relativedelta(months=6)).strftime("%Y-%m-%d")

    fecha_str = fecha_actual.strftime("%d/%m/%y")
    fecha_venc_str = (fecha_actual + relativedelta(months=6)).strftime("%d/%m/%y")

    numero_lote = fecha_actual.strftime("%d%m%y")

    pdf_path = filedialog.asksaveasfilename(
        defaultextension=".pdf",
        filetypes=[("PDF", "*.pdf")],
        initialfile=f"qr_lote_{numero_lote}.pdf"
    )
    if not pdf_path:
        return

    c = canvas.Canvas(pdf_path, pagesize=A4)
    _, alto = A4

    y_positions = [alto - 230, alto - 430, alto - 630, alto - 830]
    x_qr = 40
    qr_size = 215
    text_x = x_qr + qr_size + 40
    posicion_actual = 0

    desc_clean = str(descripcion).replace("\n", " ").replace("|", "/").replace("=", "-").strip()
    if len(desc_clean) > 90:
        desc_clean = desc_clean[:90]

    for _ in range(cantidad):
        nro_serie += 1

        payload_qr = (
            f"NS={nro_serie:06d}"
            f"|PRD={id_producto}"
            f"|DSC={desc_clean}"
            f"|LOT={numero_lote}"
            f"|FEC={fec_iso}"
            f"|VTO={vto_iso}"
        )

        qr = qrcode.make(payload_qr)
        qr_path = f"temp_qr_{id_producto}_{nro_serie}.png"
        qr.save(qr_path)

        for _ in range(2):
            y = y_positions[posicion_actual]
            c.drawImage(qr_path, x_qr, y, width=qr_size, height=qr_size)

            titulo_lineas = dividir_texto(descripcion, 40)
            resto_lineas = [
                f"N° de serie: {nro_serie}",
                f"ID producto: {id_producto}",
                f"Lote: {numero_lote}",
                f"Creación: {fecha_str}",
                f"Vencimiento: {fecha_venc_str}",
            ]

            titulo_height = len(titulo_lineas) * 18
            resto_height = len(resto_lineas) * 15
            total_height = titulo_height + resto_height

            centro_qr_y = y + qr_size / 2
            text_y = centro_qr_y + total_height / 2

            c.setFont("Helvetica-Bold", 15)
            for i, linea_txt in enumerate(titulo_lineas):
                c.drawString(text_x, text_y - i * 20, linea_txt)

            offset = titulo_height

            c.setFont("Helvetica-Bold", 18)
            c.drawString(text_x, text_y - offset, resto_lineas[0])
            offset += 20

            c.setFont("Helvetica", 15)
            for linea_txt in resto_lineas[1:]:
                c.drawString(text_x, text_y - offset, linea_txt)
                offset += 15

            posicion_actual += 1
            if posicion_actual == 4:
                c.showPage()
                posicion_actual = 0

        os.remove(qr_path)

    c.save()

    df.loc[fila[0], "ultimo_nro_serie"] = nro_serie
    guardar_productos(df)

    messagebox.showinfo("PDF generado", f"El archivo se guardó correctamente:\n{pdf_path}")


# =======================
#   UI
# =======================
conn_db = init_db()

root = tb.Window(themename="minty")
root.title("Sistema QRs – Talca")
root.geometry("980x650")

notebook = tb.Notebook(root)
notebook.pack(fill="both", expand=True, padx=10, pady=10)

tab_gen = tb.Frame(notebook, padding=20)
tab_scan = tb.Frame(notebook, padding=20)
tab_view = tb.Frame(notebook, padding=20)

notebook.add(tab_gen, text="Generar QRs")
notebook.add(tab_scan, text="Escanear pallet")
notebook.add(tab_view, text="Registros")


# ----- TAB 1: Generar -----
tb.Label(tab_gen, text="Generador de QRs", font=("Segoe UI", 18, "bold")).pack(pady=10)
tb.Label(tab_gen, text="Seleccioná un producto:", font=("Segoe UI", 12)).pack(pady=5)

productos = obtener_productos()
producto_dict = {f"{d} (ID: {i})": (i, d) for i, d in productos}

combo = tb.Combobox(tab_gen, values=list(producto_dict.keys()), width=80)
combo.pack(pady=4)

tb.Label(tab_gen, text="Cantidad de números de serie:", font=("Segoe UI", 12)).pack(pady=10)
cantidad_entry = tb.Entry(tab_gen, width=12)
cantidad_entry.pack()

cache = load_cache()
if cache.get("gen_producto") in producto_dict:
    combo.set(cache.get("gen_producto"))
if cache.get("gen_cantidad"):
    cantidad_entry.insert(0, str(cache.get("gen_cantidad")))


def al_hacer_click_generar():
    if not combo.get():
        messagebox.showwarning("Aviso", "Seleccioná un producto.")
        return

    try:
        cantidad = int(cantidad_entry.get())
        if cantidad <= 0:
            raise ValueError
    except:
        messagebox.showwarning("Aviso", "Cantidad inválida.")
        return

    pid, desc = producto_dict[combo.get()]
    generar_y_imprimir_qrs(pid, desc, cantidad)

    cache2 = load_cache()
    cache2["gen_producto"] = combo.get()
    cache2["gen_cantidad"] = cantidad
    save_cache(cache2)


tb.Button(tab_gen, text="GENERAR", bootstyle=SUCCESS, command=al_hacer_click_generar).pack(pady=18)


# ----- TAB 3: Registros -----
tb.Label(tab_view, text="Registros escaneados", font=("Segoe UI", 18, "bold")).pack(pady=10)

count_var = tb.StringVar(value="Cargando…")
tb.Label(tab_view, textvariable=count_var, font=("Segoe UI", 11)).pack(pady=(0, 8))

table_frame = tb.Frame(tab_view)
table_frame.pack(fill="both", expand=True)

columns = ("descripcion", "nro_serie", "id_producto", "lote", "creacion", "vencimiento")
tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=16)
tree.pack(side="left", fill="both", expand=True)

tree.heading("descripcion", text="Descripción")
tree.heading("nro_serie", text="N° Serie")
tree.heading("id_producto", text="ID Producto")
tree.heading("lote", text="Lote")
tree.heading("creacion", text="Creación")
tree.heading("vencimiento", text="Vencimiento")

tree.column("descripcion", width=420, anchor="w")
tree.column("nro_serie", width=90, anchor="center")
tree.column("id_producto", width=110, anchor="center")
tree.column("lote", width=90, anchor="center")
tree.column("creacion", width=120, anchor="center")
tree.column("vencimiento", width=120, anchor="center")

scroll_y = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
scroll_y.pack(side="right", fill="y")
tree.configure(yscrollcommand=scroll_y.set)


def refresh_table(limit=300):
    for item in tree.get_children():
        tree.delete(item)

    rows = fetch_latest_scans(conn_db, limit=limit)
    for r in rows:
        tree.insert("", "end", values=r)

    count_var.set(f"Mostrando {len(rows)} registros (ordenados por lote/producto/serie)")


btns_frame = tb.Frame(tab_view)
btns_frame.pack(pady=10)

tb.Button(btns_frame, text="Refrescar", bootstyle=INFO, command=refresh_table).pack(side="left", padx=6)
tb.Button(btns_frame, text="Últimos 100", bootstyle=SECONDARY, command=lambda: refresh_table(100)).pack(side="left", padx=6)
tb.Button(btns_frame, text="Últimos 500", bootstyle=SECONDARY, command=lambda: refresh_table(500)).pack(side="left", padx=6)

refresh_table()


# ----- TAB 2: ESCANEO (toggle parcial) -----
tb.Label(tab_scan, text="Escaneo de pallets", font=("Segoe UI", 18, "bold")).pack(pady=10)
tb.Label(
    tab_scan,
    text="• Escaneá en el campo.\n"
         "• Si el toggle 'Pallet parcial' está OFF: se registra como COMPLETO y se envía automático.\n"
         "• Si el toggle está ON: aparece Packs -> cargás packs y con Enter se envía.\n",
    font=("Segoe UI", 10),
    justify="left"
).pack(pady=6)

scan_var = tb.StringVar()
entry_scan = tb.Entry(tab_scan, textvariable=scan_var, width=95, font=("Segoe UI", 14))
entry_scan.pack(pady=10)
entry_scan.focus_set()

# Toggle: OFF = completo / ON = parcial
is_partial_var = tb.BooleanVar(value=False)

toggle_frame = tb.Frame(tab_scan)
toggle_frame.pack(pady=6)

toggle_partial = tb.Checkbutton(
    toggle_frame,
    text="Pallet parcial (activar = parcial)",
    variable=is_partial_var,
    bootstyle="warning-round-toggle"
)
toggle_partial.pack()

packs_frame = tb.Frame(tab_scan)
packs_frame.pack(pady=8)

tb.Label(packs_frame, text="Packs (solo si parcial):", font=("Segoe UI", 11)).pack(side="left", padx=(0, 8))
packs_var = tb.StringVar(value="")
entry_packs = tb.Entry(packs_frame, textvariable=packs_var, width=10, font=("Segoe UI", 12))
entry_packs.pack(side="left")

status_var = tb.StringVar(value="Listo para escanear…")
status_lbl = tb.Label(tab_scan, textvariable=status_var, font=("Segoe UI", 11), justify="left")
status_lbl.pack(pady=8)

sheets_var = tb.StringVar(value=f"Sheets pendientes (outbox): {outbox_count(conn_db)}")
tb.Label(tab_scan, textvariable=sheets_var, font=("Segoe UI", 10)).pack(pady=4)

pending_data = {"data": None}  # scan pendiente (si parcial, queda esperando packs)


def show_or_hide_packs():
    if is_partial_var.get():
        entry_packs.configure(state="normal")
        packs_var.set("")
        # si ya hay un scan pendiente, mandamos foco al packs; sino, al scan
        if pending_data["data"] is not None:
            entry_packs.focus_set()
        else:
            entry_scan.focus_set()
    else:
        entry_packs.configure(state="disabled")
        packs_var.set("")
        entry_scan.focus_set()


def clear_pending():
    pending_data["data"] = None
    scan_var.set("")
    packs_var.set("")
    show_or_hide_packs()
    entry_scan.focus_set()


def commit_scan(is_full: int, packs_partial: int):
    data = pending_data["data"]
    if not data:
        return

    save_single_scan(conn_db, data)

    upsert_pallet_status(
        conn_db,
        id_producto=data["id_producto"],
        lote=data["lote"],
        nro_serie=data["nro_serie"],
        is_full=1 if is_full else 0,
        packs_partial=int(packs_partial) if not is_full else 0
    )

    pallets_total, packs_aclarados, _ = compute_totals_for_product_lote(conn_db, data["id_producto"], data["lote"])

    try:
        sent = sync_product_lote_to_sheets(conn_db, data["id_producto"], data["lote"])
        pending = outbox_count(conn_db)
        sheets_var.set(f"✅ Enviado(s): {sent} | Pendientes (outbox): {pending}")
    except Exception as e:
        pending = outbox_count(conn_db)
        sheets_var.set(f"⚠️ No se pudo enviar a Sheets: {e} | Pendientes (outbox): {pending}")

    refresh_table()

    tipo = "COMPLETO" if is_full else f"PARCIAL ({packs_partial} packs)"
    status_var.set(
        f"✅ Registrado {tipo}\n"
        f"{data['id_producto']} | Lote {data['lote']} | Serie {data['nro_serie']}\n"
        f"📦 Pallets (según escaneos): {pallets_total}\n"
        f"📦 Packs aclarados (parciales): {packs_aclarados}"
    )
    root.bell()
    clear_pending()


def on_scan_return(event=None):
    raw = scan_var.get().strip()
    if not raw:
        return

    try:
        data = parse_qr_payload(raw)
        pending_data["data"] = data

        if not is_partial_var.get():
            # COMPLETO -> enviar automático
            commit_scan(is_full=1, packs_partial=0)
        else:
            # PARCIAL -> esperar packs + Enter
            status_var.set(
                f"🟡 Parcial: ingresá packs y presioná Enter.\n"
                f"{data['id_producto']} | Lote {data['lote']} | Serie {data['nro_serie']}"
            )
            entry_packs.focus_set()

    except Exception as e:
        status_var.set(f"❌ ERROR scan: {e}")
        root.bell()
        clear_pending()


def on_packs_return(event=None):
    if pending_data["data"] is None:
        entry_scan.focus_set()
        return

    # Si alguien apagó el toggle antes de Enter, entonces ahora sería completo y se manda
    if not is_partial_var.get():
        commit_scan(is_full=1, packs_partial=0)
        return

    try:
        packs = int(packs_var.get())
        if packs < 1:
            raise ValueError
    except:
        status_var.set("❌ Packs inválido. Debe ser entero >= 1.")
        root.bell()
        entry_packs.focus_set()
        return

    commit_scan(is_full=0, packs_partial=packs)


def on_toggle_changed(*args):
    show_or_hide_packs()

    # Si hay scan pendiente y se apagó (completo), mandamos automático
    if pending_data["data"] is not None and not is_partial_var.get():
        commit_scan(is_full=1, packs_partial=0)


is_partial_var.trace_add("write", on_toggle_changed)

entry_scan.bind("<Return>", on_scan_return)
entry_packs.bind("<Return>", on_packs_return)

show_or_hide_packs()


def retry_sync_full_snapshot():
    try:
        sent_pending = flush_outbox(conn_db)

        all_rows = build_full_snapshot_rows(conn_db)
        if not all_rows:
            pending = outbox_count(conn_db)
            messagebox.showinfo("Sync Sheets", f"No hay datos en la BD.\nEnviados pendientes: {sent_pending}\nPendientes: {pending}")
            sheets_var.set(f"Sheets pendientes (outbox): {pending}")
            return

        total = 0
        for block in chunks(all_rows, 200):
            payload = {
                "api_key": SHEETS_API_KEY,
                "type": "bulk_snapshot",
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "rows": block
            }
            res = send_to_sheets(payload)
            if not isinstance(res, dict) or res.get("ok") is not True:
                raise RuntimeError(f"Respuesta inválida de Sheets: {res}")
            total += len(block)

        pending = outbox_count(conn_db)
        sheets_var.set(f"✅ Snapshot enviado ({total} filas). Pendientes (outbox): {pending}")
        messagebox.showinfo("Sync Sheets", f"✅ Snapshot completo enviado.\nFilas enviadas: {total}\nPendientes (outbox): {pending}")

    except Exception as e:
        messagebox.showerror("Sync Sheets", f"Error:\n{e}")


btn_frame_scan = tb.Frame(tab_scan)
btn_frame_scan.pack(pady=10)

tb.Button(btn_frame_scan, text="Reintentar envío a Sheets", bootstyle=WARNING, command=retry_sync_full_snapshot).pack(side="left", padx=6)


def on_tab_change(event=None):
    try:
        current = notebook.tab(notebook.select(), "text")
        if current == "Escanear pallet":
            entry_scan.focus_set()
    except:
        pass


notebook.bind("<<NotebookTabChanged>>", on_tab_change)


def on_close():
    try:
        conn_db.close()
    except:
        pass
    root.destroy()


root.protocol("WM_DELETE_WINDOW", on_close)
root.mainloop()
