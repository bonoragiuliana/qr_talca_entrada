import os
import sys
import json
import textwrap
from datetime import datetime
from dateutil.relativedelta import relativedelta
import urllib.request
import urllib.error

import qrcode
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox, filedialog, ttk

# ----------------------------
#   POSTGRES DRIVER
# ----------------------------
try:
    import psycopg2
    from psycopg2.extras import Json
except Exception:
    psycopg2 = None
    Json = None


# =======================
#   CONFIG / PATHS
# =======================
def get_app_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


APP_DIR = get_app_dir()
CACHE_FILE = os.path.join(APP_DIR, "config.json")

# =======================
#   GOOGLE SHEETS (WEBHOOK)
# =======================
SHEETS_WEBAPP_URL = "https://script.google.com/macros/s/AKfycbz3HnhMu8ylXKtiEVvcsIRc_VKJzxUQHotKDOHT74QgTgLIVbJPPiX3eJBly368Ad4/exec"
SHEETS_API_KEY = "TALCA-QR-2026"  # Debe coincidir con API_KEY en Apps Script

# =======================
#   POSTGRES DEFAULTS
# =======================
DEFAULT_PG = {
    "host": os.getenv("TALCA_PG_HOST", "localhost"),
    "port": int(os.getenv("TALCA_PG_PORT", "5432")),
    "dbname": os.getenv("TALCA_PG_DB", "postgres"),
    "user": os.getenv("TALCA_PG_USER", "postgres"),
    "password": os.getenv("TALCA_PG_PASS", ""),
    "client_encoding": os.getenv("TALCA_PG_ENCODING", ""),  # opcional
    "schema": "stock",
    "table_products": "productos",
    "table_pp": "stock_pp",
    "table_outbox": "sheets_outbox",
}


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


def get_pg_config():
    cfg = DEFAULT_PG.copy()
    cache = load_cache()
    if isinstance(cache.get("pg"), dict):
        for k, v in cache["pg"].items():
            if v is not None and v != "":
                cfg[k] = v
    # normalizar port por si viene string en json
    try:
        cfg["port"] = int(cfg["port"])
    except:
        cfg["port"] = 5432
    return cfg


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
#   POSTGRES
# =======================
def pg_connect():
    """
    ✅ Conecta a PostgreSQL usando config.json (sección pg)
    ✅ Aplica client_encoding si está definido (ej: WIN1252)
    """
    if psycopg2 is None:
        raise RuntimeError("Falta psycopg2. Instalá con: pip install psycopg2-binary")

    cfg = get_pg_config()
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
    )
    conn.autocommit = True

    # ✅ aplicar client_encoding si viene en config
    enc = cfg.get("client_encoding")
    if enc:
        try:
            conn.set_client_encoding(enc)
        except Exception as e:
            # No frenamos toda la app por encoding, pero lo avisamos
            raise RuntimeError(f"Conectó a PG pero falló client_encoding='{enc}': {e}")

    return conn


def init_pg(conn):
    """
    No recrea tus tablas principales si ya existen.
    Solo asegura la outbox para Sheets en el schema stock.
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    outbox = cfg["table_outbox"]

    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{outbox} (
                id BIGSERIAL PRIMARY KEY,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{outbox}_created_at ON {schema}.{outbox}(created_at DESC);")


def fetch_products(conn):
    cfg = get_pg_config()
    schema = cfg["schema"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id_producto, descripcion
            FROM {schema}.{prod}
            ORDER BY descripcion ASC;
        """)
        return cur.fetchall()


def get_product_row(conn, id_producto: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id_producto, descripcion, ultimo_nro_serie
            FROM {schema}.{prod}
            WHERE id_producto = %s;
        """, (int(id_producto),))
        return cur.fetchone()


def update_ultimo_nro_serie(conn, id_producto: int, ultimo: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {schema}.{prod}
            SET ultimo_nro_serie = %s
            WHERE id_producto = %s;
        """, (int(ultimo), int(id_producto)))


def insert_stock_pp(conn, id_producto: int, lote: str, serie_inicio: int, serie_fin: int, packs_fin: int):
    """
    Inserta un movimiento en stock.stock_pp:
      - serie_inicio, serie_fin
      - packs_fin: 0 si completo, >0 si el último es parcial (packs aclarados)
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tpp = cfg["table_pp"]

    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {schema}.{tpp}(created_at, id_producto, lote, serie_inicio, serie_fin, packs_fin)
            VALUES (now(), %s, %s, %s, %s, %s)
            RETURNING id, created_at;
        """, (int(id_producto), str(lote), int(serie_inicio), int(serie_fin), int(packs_fin)))
        return cur.fetchone()  # (id, created_at)


def fetch_latest_pp(conn, limit=300):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tpp = cfg["table_pp"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                pp.id,
                pp.created_at,
                pp.id_producto,
                p.descripcion,
                pp.lote,
                pp.serie_inicio,
                pp.serie_fin,
                pp.packs_fin,
                ((pp.serie_fin - pp.serie_inicio + 1) - CASE WHEN pp.packs_fin > 0 THEN 1 ELSE 0 END) AS pallets_contados
            FROM {schema}.{tpp} pp
            JOIN {schema}.{prod} p ON p.id_producto = pp.id_producto
            ORDER BY pp.created_at DESC
            LIMIT %s;
        """, (int(limit),))
        return cur.fetchall()


def compute_totals_for_product_lote(conn, id_producto: int, lote: str):
    """
    Pallets = SUM( (serie_fin-serie_inicio+1) - (packs_fin>0 ? 1 : 0) )
    Packs  = SUM(packs_fin)
    """
    cfg = get_pg_config()
    schema = cfg["schema"]
    tpp = cfg["table_pp"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"SELECT descripcion FROM {schema}.{prod} WHERE id_producto = %s;", (int(id_producto),))
        row = cur.fetchone()
        desc = row[0] if row else ""

        cur.execute(f"""
            SELECT
                COALESCE(SUM((serie_fin - serie_inicio + 1) - CASE WHEN packs_fin > 0 THEN 1 ELSE 0 END), 0) AS pallets,
                COALESCE(SUM(packs_fin), 0) AS packs
            FROM {schema}.{tpp}
            WHERE id_producto = %s AND lote = %s;
        """, (int(id_producto), str(lote)))
        pallets, packs = cur.fetchone()

    return int(pallets), int(packs), str(desc).strip()


def build_snapshot_rows(conn):
    cfg = get_pg_config()
    schema = cfg["schema"]
    tpp = cfg["table_pp"]
    prod = cfg["table_products"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                p.id_producto,
                p.descripcion,
                COALESCE(SUM((pp.serie_fin - pp.serie_inicio + 1) - CASE WHEN pp.packs_fin > 0 THEN 1 ELSE 0 END), 0) AS pallets,
                COALESCE(SUM(pp.packs_fin), 0) AS packs_aclarados
            FROM {schema}.{prod} p
            LEFT JOIN {schema}.{tpp} pp
              ON pp.id_producto = p.id_producto
            GROUP BY p.id_producto, p.descripcion
            ORDER BY p.id_producto ASC;
        """)
        rows = cur.fetchall()

    out = []
    for pid, desc, pallets, packs in rows:
        out.append({
            "id_producto": int(pid),
            "descripcion": str(desc),
            "pallets": int(pallets),
            "packs_aclarados": int(packs),
        })
    return out


# =======================
#   SHEETS OUTBOX (POSTGRES)
# =======================
def outbox_count(conn):
    cfg = get_pg_config()
    schema = cfg["schema"]
    outbox = cfg["table_outbox"]
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{outbox};")
        return int(cur.fetchone()[0] or 0)


def queue_outbox(conn, payload: dict):
    cfg = get_pg_config()
    schema = cfg["schema"]
    outbox = cfg["table_outbox"]
    with conn.cursor() as cur:
        cur.execute(f"INSERT INTO {schema}.{outbox}(payload) VALUES (%s);", (Json(payload),))


def pop_outbox_batch(conn, limit=50):
    cfg = get_pg_config()
    schema = cfg["schema"]
    outbox = cfg["table_outbox"]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, payload
            FROM {schema}.{outbox}
            ORDER BY id ASC
            LIMIT %s;
        """, (int(limit),))
        return cur.fetchall()


def delete_outbox_id(conn, rid: int):
    cfg = get_pg_config()
    schema = cfg["schema"]
    outbox = cfg["table_outbox"]
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {schema}.{outbox} WHERE id = %s;", (int(rid),))


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
    rows = pop_outbox_batch(conn, limit=50)
    sent = 0
    for rid, payload in rows:
        res = send_to_sheets(payload)
        if isinstance(res, dict) and res.get("ok") is True:
            delete_outbox_id(conn, rid)
            sent += 1
        else:
            break
    return sent


def build_payload_for_product_lote(conn, id_producto: int, lote: str):
    pallets, packs, desc = compute_totals_for_product_lote(conn, id_producto, lote)
    return {
        "api_key": SHEETS_API_KEY,
        "type": "scan_pp",
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "stock": {
            "id_producto": int(id_producto),
            "descripcion": desc,
            "lote": str(lote),
            "stock_pallets": int(pallets),
            "stock_packs": int(packs),
        }
    }


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
            "id_producto": int(normalize_id_value(data["PRD"])),
            "lote": str(data["LOT"]).strip(),
            "creacion": normalize_date_iso(data["FEC"]),
            "vencimiento": normalize_date_iso(data["VTO"]),
        }

    raise ValueError("QR inválido: formato no reconocido.")


# =======================
#   PDF QRS
# =======================
def dividir_texto(texto, max_caracteres):
    return textwrap.wrap(str(texto), width=max_caracteres)


def generar_y_imprimir_qrs(conn, id_producto: int, descripcion: str, cantidad: int):
    row = get_product_row(conn, id_producto)
    if not row:
        messagebox.showerror("Error", "Producto no encontrado en Postgres.")
        return

    _, _, ultimo = row
    nro_serie = int(ultimo or 0)

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
        qr_path = os.path.join(APP_DIR, f"temp_qr_{id_producto}_{nro_serie}.png")
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

        try:
            os.remove(qr_path)
        except:
            pass

    c.save()

    # ✅ actualizar ultimo_nro_serie en Postgres
    update_ultimo_nro_serie(conn, id_producto, nro_serie)

    messagebox.showinfo("PDF generado", f"El archivo se guardó correctamente:\n{pdf_path}")


# =======================
#   UI APP
# =======================
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def main():
    # Conexión PG
    try:
        conn_pg = pg_connect()
        init_pg(conn_pg)
    except Exception as e:
        messagebox.showerror(
            "PostgreSQL",
            "No pude conectar a PostgreSQL.\n\n"
            f"Error: {e}\n\n"
            "Tip: revisá host/puerto/db/user/pass en config.json (sección pg).\n"
            "Si usás client_encoding, asegurate que sea válido (ej: WIN1252)."
        )
        return

    # Carga productos
    try:
        productos = fetch_products(conn_pg)
    except Exception as e:
        messagebox.showerror("PostgreSQL", f"No pude leer stock.productos.\n\n{e}")
        return

    # UI
    root = tb.Window(themename="minty")
    root.title("Sistema QRs – Talca (PostgreSQL)")
    root.geometry("1050x680")

    notebook = tb.Notebook(root)
    notebook.pack(fill="both", expand=True, padx=10, pady=10)

    tab_gen = tb.Frame(notebook, padding=20)
    tab_scan = tb.Frame(notebook, padding=20)
    tab_view = tb.Frame(notebook, padding=20)

    notebook.add(tab_gen, text="Generar QRs")
    notebook.add(tab_scan, text="Escanear (inicio/fin)")
    notebook.add(tab_view, text="Registros")

    # -----------------------
    # TAB 1: Generar
    # -----------------------
    tb.Label(tab_gen, text="Generador de QRs", font=("Segoe UI", 18, "bold")).pack(pady=10)
    tb.Label(tab_gen, text="Seleccioná un producto:", font=("Segoe UI", 12)).pack(pady=5)

    producto_dict = {f"{desc} (ID: {pid})": (int(pid), str(desc)) for pid, desc in productos}

    combo = tb.Combobox(tab_gen, values=list(producto_dict.keys()), width=90)
    combo.pack(pady=4)

    tb.Label(tab_gen, text="Cantidad de números de serie:", font=("Segoe UI", 12)).pack(pady=10)
    cantidad_entry = tb.Entry(tab_gen, width=12)
    cantidad_entry.pack()

    cache = load_cache()
    if cache.get("gen_producto") in producto_dict:
        combo.set(cache.get("gen_producto"))
    if cache.get("gen_cantidad"):
        try:
            cantidad_entry.insert(0, str(int(cache.get("gen_cantidad"))))
        except:
            pass

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
        generar_y_imprimir_qrs(conn_pg, pid, desc, cantidad)

        cache2 = load_cache()
        cache2["gen_producto"] = combo.get()
        cache2["gen_cantidad"] = cantidad
        save_cache(cache2)

    tb.Button(tab_gen, text="GENERAR", bootstyle=SUCCESS, command=al_hacer_click_generar).pack(pady=18)

    # -----------------------
    # TAB 3: Registros (stock_pp)
    # -----------------------
    tb.Label(tab_view, text="Movimientos guardados (stock_pp)", font=("Segoe UI", 18, "bold")).pack(pady=10)

    count_var = tb.StringVar(value="Cargando…")
    tb.Label(tab_view, textvariable=count_var, font=("Segoe UI", 11)).pack(pady=(0, 8))

    table_frame = tb.Frame(tab_view)
    table_frame.pack(fill="both", expand=True)

    columns = ("id", "created_at", "id_producto", "descripcion", "lote", "serie_inicio", "serie_fin", "packs_fin", "pallets_contados")
    tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=18)
    tree.pack(side="left", fill="both", expand=True)

    headings = {
        "id": "ID",
        "created_at": "Fecha",
        "id_producto": "ID Producto",
        "descripcion": "Descripción",
        "lote": "Lote",
        "serie_inicio": "Serie inicio",
        "serie_fin": "Serie fin",
        "packs_fin": "Packs fin",
        "pallets_contados": "Pallets contados",
    }
    for k, h in headings.items():
        tree.heading(k, text=h)

    tree.column("id", width=60, anchor="center")
    tree.column("created_at", width=150, anchor="center")
    tree.column("id_producto", width=110, anchor="center")
    tree.column("descripcion", width=360, anchor="w")
    tree.column("lote", width=90, anchor="center")
    tree.column("serie_inicio", width=95, anchor="center")
    tree.column("serie_fin", width=95, anchor="center")
    tree.column("packs_fin", width=85, anchor="center")
    tree.column("pallets_contados", width=120, anchor="center")

    scroll_y = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
    scroll_y.pack(side="right", fill="y")
    tree.configure(yscrollcommand=scroll_y.set)

    def refresh_table(limit=300):
        for item in tree.get_children():
            tree.delete(item)

        rows = fetch_latest_pp(conn_pg, limit=limit)
        for r in rows:
            rid, created_at, pid, desc, lote, si, sf, packs, pallets = r
            tree.insert("", "end", values=(
                rid,
                created_at.strftime("%Y-%m-%d %H:%M:%S") if hasattr(created_at, "strftime") else str(created_at),
                pid, desc, lote, si, sf, packs, pallets
            ))

        count_var.set(f"Mostrando {len(rows)} movimientos (ordenados por fecha desc)")

    btns_frame = tb.Frame(tab_view)
    btns_frame.pack(pady=10)

    tb.Button(btns_frame, text="Refrescar", bootstyle=INFO, command=refresh_table).pack(side="left", padx=6)
    tb.Button(btns_frame, text="Últimos 100", bootstyle=SECONDARY, command=lambda: refresh_table(100)).pack(side="left", padx=6)
    tb.Button(btns_frame, text="Últimos 500", bootstyle=SECONDARY, command=lambda: refresh_table(500)).pack(side="left", padx=6)

    refresh_table()

    # -----------------------
    # TAB 2: Escaneo (inicio/fin) + parcial
    # -----------------------
    tb.Label(tab_scan, text="Escaneo por rango", font=("Segoe UI", 18, "bold")).pack(pady=10)
    tb.Label(
        tab_scan,
        text="Escaneá el QR de INICIO y luego el QR de FIN.\n"
             "Si el ÚLTIMO pallet es parcial, activá el toggle y cargá packs.",
        font=("Segoe UI", 10)
    ).pack(pady=6)

    scan_var = tb.StringVar()
    entry_scan = tb.Entry(tab_scan, textvariable=scan_var, width=95, font=("Segoe UI", 14))
    entry_scan.pack(pady=10)
    entry_scan.focus_set()

    is_partial_var = tb.BooleanVar(value=False)

    toggle_frame = tb.Frame(tab_scan)
    toggle_frame.pack(pady=6)

    toggle_partial = tb.Checkbutton(
        toggle_frame,
        text="Último pallet parcial (activar = parcial)",
        variable=is_partial_var,
        bootstyle="warning-round-toggle"
    )
    toggle_partial.pack()

    packs_frame = tb.Frame(tab_scan)
    packs_frame.pack(pady=8)

    tb.Label(packs_frame, text="Packs del último pallet parcial:", font=("Segoe UI", 11)).pack(side="left", padx=(0, 8))
    packs_var = tb.StringVar(value="")
    entry_packs = tb.Entry(packs_frame, textvariable=packs_var, width=10, font=("Segoe UI", 12))
    entry_packs.pack(side="left")

    status_var = tb.StringVar(value="🟢 Listo: escaneá QR de INICIO y presioná Enter.")
    tb.Label(tab_scan, textvariable=status_var, font=("Segoe UI", 11), justify="left").pack(pady=10)

    sheets_var = tb.StringVar(value=f"Sheets pendientes (outbox): {outbox_count(conn_pg)}")
    tb.Label(tab_scan, textvariable=sheets_var, font=("Segoe UI", 10)).pack(pady=4)

    flow = {"start": None, "end": None, "await": "start"}  # start | end | packs

    def ui_set_packs_state():
        if is_partial_var.get():
            entry_packs.configure(state="normal")
        else:
            entry_packs.configure(state="disabled")
            packs_var.set("")

    def reset_flow():
        flow["start"] = None
        flow["end"] = None
        flow["await"] = "start"
        scan_var.set("")
        packs_var.set("")
        ui_set_packs_state()
        status_var.set("🟢 Listo: escaneá QR de INICIO y presioná Enter.")
        entry_scan.focus_set()

    def commit_range(packs_fin: int):
        start = flow["start"]
        end = flow["end"]
        if not start or not end:
            return

        if int(start["id_producto"]) != int(end["id_producto"]) or str(start["lote"]) != str(end["lote"]):
            raise ValueError("INICIO y FIN no corresponden al mismo producto+lote.")

        pid = int(start["id_producto"])
        lote = str(start["lote"]).strip()

        si = int(start["nro_serie"])
        sf = int(end["nro_serie"])
        serie_inicio = min(si, sf)
        serie_fin = max(si, sf)

        mov_id, created_at = insert_stock_pp(conn_pg, pid, lote, serie_inicio, serie_fin, int(packs_fin))

        pallets_total, packs_total, desc = compute_totals_for_product_lote(conn_pg, pid, lote)

        try:
            payload = build_payload_for_product_lote(conn_pg, pid, lote)
            queue_outbox(conn_pg, payload)
            sent = flush_outbox(conn_pg)
            pending = outbox_count(conn_pg)
            sheets_var.set(f"✅ Enviado(s): {sent} | Pendientes (outbox): {pending}")
        except Exception as e:
            pending = outbox_count(conn_pg)
            sheets_var.set(f"⚠️ No se pudo enviar a Sheets: {e} | Pendientes (outbox): {pending}")

        refresh_table()
        root.bell()

        rango_total = (serie_fin - serie_inicio + 1)
        pallets_mov = rango_total - (1 if packs_fin > 0 else 0)

        tipo = "COMPLETO" if packs_fin <= 0 else f"PARCIAL (packs: {packs_fin})"
        status_var.set(
            f"✅ Movimiento guardado ({tipo})\n"
            f"ID {mov_id} | {pid} | {desc}\n"
            f"Lote {lote} | Series {serie_inicio}–{serie_fin} (rango {rango_total})\n"
            f"📦 Pallets en este movimiento: {pallets_mov}\n"
            f"📦 Stock total pallets (producto+lote): {pallets_total}\n"
            f"📦 Stock total packs (producto+lote): {packs_total}"
        )

        reset_flow()

    def on_scan_return(event=None):
        raw = scan_var.get().strip()
        if not raw:
            return
        scan_var.set("")

        try:
            data = parse_qr_payload(raw)

            if flow["await"] == "start":
                flow["start"] = data
                flow["await"] = "end"
                status_var.set(
                    f"🟡 INICIO OK: {data['id_producto']} | Lote {data['lote']} | Serie {data['nro_serie']}\n"
                    f"Ahora escaneá el QR de FIN y Enter."
                )
                entry_scan.focus_set()
                return

            if flow["await"] == "end":
                flow["end"] = data

                if not is_partial_var.get():
                    commit_range(packs_fin=0)
                    return

                flow["await"] = "packs"
                ui_set_packs_state()
                status_var.set(
                    f"🟠 FIN OK: {data['id_producto']} | Lote {data['lote']} | Serie {data['nro_serie']}\n"
                    f"Último es PARCIAL: escribí packs y presioná Enter."
                )
                entry_packs.focus_set()
                return

            status_var.set("⚠️ Estás en modo packs. Ingresá packs y Enter, o apagá el toggle para enviar como completo.")
            entry_packs.focus_set()

        except Exception as e:
            status_var.set(f"❌ ERROR scan: {e}")
            root.bell()
            reset_flow()

    def on_packs_return(event=None):
        if flow["await"] != "packs":
            entry_scan.focus_set()
            return

        if not is_partial_var.get():
            try:
                commit_range(packs_fin=0)
            except Exception as e:
                status_var.set(f"❌ ERROR: {e}")
                root.bell()
                reset_flow()
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

        try:
            commit_range(packs_fin=packs)
        except Exception as e:
            status_var.set(f"❌ ERROR: {e}")
            root.bell()
            reset_flow()

    def on_toggle_changed(*args):
        ui_set_packs_state()
        if flow["await"] == "packs" and not is_partial_var.get():
            try:
                commit_range(packs_fin=0)
            except Exception as e:
                status_var.set(f"❌ ERROR: {e}")
                root.bell()
                reset_flow()

    is_partial_var.trace_add("write", on_toggle_changed)

    entry_scan.bind("<Return>", on_scan_return)
    entry_packs.bind("<Return>", on_packs_return)
    ui_set_packs_state()

    def retry_sync_snapshot():
        try:
            sent_pending = flush_outbox(conn_pg)

            rows = build_snapshot_rows(conn_pg)
            if not rows:
                pending = outbox_count(conn_pg)
                messagebox.showinfo("Sync Sheets", f"No hay datos en stock_pp.\nEnviados pendientes: {sent_pending}\nPendientes: {pending}")
                sheets_var.set(f"Sheets pendientes (outbox): {pending}")
                return

            total_sent = 0
            for block in chunks(rows, 200):
                payload = {
                    "api_key": SHEETS_API_KEY,
                    "type": "bulk_snapshot_pp",
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "rows": block
                }
                res = send_to_sheets(payload)
                if not isinstance(res, dict) or res.get("ok") is not True:
                    raise RuntimeError(f"Respuesta inválida de Sheets: {res}")
                total_sent += len(block)

            pending = outbox_count(conn_pg)
            sheets_var.set(f"✅ Snapshot enviado ({total_sent} filas). Pendientes (outbox): {pending}")
            messagebox.showinfo("Sync Sheets", f"✅ Snapshot completo enviado.\nFilas enviadas: {total_sent}\nPendientes (outbox): {pending}")

        except Exception as e:
            messagebox.showerror("Sync Sheets", f"Error:\n{e}")

    btn_frame_scan = tb.Frame(tab_scan)
    btn_frame_scan.pack(pady=10)
    tb.Button(btn_frame_scan, text="Reintentar envío a Sheets (snapshot)", bootstyle=WARNING, command=retry_sync_snapshot).pack(side="left", padx=6)

    def on_tab_change(event=None):
        try:
            current = notebook.tab(notebook.select(), "text")
            if current == "Escanear (inicio/fin)":
                entry_scan.focus_set()
        except:
            pass

    notebook.bind("<<NotebookTabChanged>>", on_tab_change)

    def on_close():
        try:
            conn_pg.close()
        except:
            pass
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_close)
    root.mainloop()


if __name__ == "__main__":
    main()
