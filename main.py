import qrcode
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import pandas as pd
import textwrap
import json
import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import messagebox, filedialog

# ---------- CONFIGURACIÓN ----------
EXCEL_PATH = "productos.xlsx"
HOJA_PRODUCTOS = "productos"
CACHE_FILE = "config.json"

# ---------- FUNCIONES EXCEL ----------
def cargar_productos():
    df = pd.read_excel(EXCEL_PATH, sheet_name=HOJA_PRODUCTOS)
    df.columns = df.columns.str.strip()
    return df

def guardar_productos(df):
    df.to_excel(EXCEL_PATH, sheet_name=HOJA_PRODUCTOS, index=False)

def obtener_productos():
    df = cargar_productos()
    return list(zip(df["id_producto"], df["descripcion"]))

def dividir_texto(texto, max_caracteres):
    return textwrap.wrap(texto, width=max_caracteres)

# ---------- CACHÉ ----------
def guardar_config(seleccion, cantidad):
    with open(CACHE_FILE, "w") as f:
        json.dump({"producto": seleccion, "cantidad": cantidad}, f)

def cargar_config():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}

# ---------- GENERAR PDF ----------
def generar_y_imprimir_qrs(id_producto, descripcion, cantidad):
    df = cargar_productos()
    fila = df[df["id_producto"] == id_producto].index
    if fila.empty:
        messagebox.showerror("Error", "Producto no encontrado.")
        return

    nro_serie = int(df.loc[fila[0], "ultimo_nro_serie"])
    fecha_actual = datetime.now()
    fecha_str = fecha_actual.strftime("%d/%m/%y")
    numero_lote = fecha_actual.strftime("%d%m%y")
    fecha_vencimiento = (fecha_actual + relativedelta(months=6)).strftime("%d/%m/%y")

    pdf_path = filedialog.asksaveasfilename(
        defaultextension=".pdf",
        filetypes=[("PDF", "*.pdf")],
        initialfile=f"qr_lote_{numero_lote}.pdf"
    )
    if not pdf_path:
        return

    c = canvas.Canvas(pdf_path, pagesize=A4)
    ancho, alto = A4

    # Posiciones verticales (4 QRs por hoja)
    y_positions = [
        alto - 230,
        alto - 430,
        alto - 630,
        alto - 830,
    ]

    x_qr = 40
    qr_size = 215
    text_x = x_qr + qr_size + 40

    posicion_actual = 0

    for _ in range(cantidad):
        nro_serie += 1

        contenido_qr = (
            f"N° de serie: {nro_serie}\n"
            f"ID producto: {id_producto}\n"
            f"{descripcion}\n"
            f"Lote: {numero_lote}\n"
            f"Creación: {fecha_str}\n"
            f"Vencimiento: {fecha_vencimiento}"
        )

        qr = qrcode.make(contenido_qr)
        qr_path = f"temp_qr_{nro_serie}.png"
        qr.save(qr_path)

        # ---- DOS QRs POR NÚMERO DE SERIE ----
        for _ in range(2):
            y = y_positions[posicion_actual]

            # Dibujar QR
            c.drawImage(qr_path, x_qr, y, width=qr_size, height=qr_size)

            # Construir texto
            titulo_lineas = dividir_texto(descripcion, 40)
            resto_lineas = [
                f"N° de serie: {nro_serie}",
                f"ID producto: {id_producto}",
                f"Lote: {numero_lote}",
                f"Creación: {fecha_str}",
                f"Vencimiento: {fecha_vencimiento}",
            ]

            # Alturas reales
            titulo_height = len(titulo_lineas) * 18
            resto_height = len(resto_lineas) * 15
            total_height = titulo_height + resto_height

            centro_qr_y = y + qr_size / 2
            text_y = centro_qr_y + total_height / 2

            # TÍTULO (descripción)
            c.setFont("Helvetica-Bold", 15)
            for i, linea in enumerate(titulo_lineas):
                c.drawString(text_x, text_y - i * 20, linea)

            offset = titulo_height

            # N° de serie destacado
            c.setFont("Helvetica-Bold", 18)
            c.drawString(text_x, text_y - offset, resto_lineas[0])
            offset += 20

            # Resto de datos
            c.setFont("Helvetica", 15)
            for linea in resto_lineas[1:]:
                c.drawString(text_x, text_y - offset, linea)
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

# ---------- INTERFAZ ----------
root = tb.Window(themename="minty")
root.title("Generador de QRs – Talca")
root.geometry("600x420")

card = tb.Frame(root, padding=20)
card.place(relx=0.5, rely=0.5, anchor="center")

tb.Label(card, text="Generador de QRs", font=("Segoe UI", 18, "bold")).pack(pady=15)

tb.Label(card, text="Seleccioná un producto:", font=("Segoe UI", 12)).pack(pady=5)

productos = obtener_productos()
producto_dict = {f"{d} (ID: {i})": (i, d) for i, d in productos}
combo = tb.Combobox(card, values=list(producto_dict.keys()), width=55)
combo.pack()

tb.Label(card, text="Cantidad de números de serie:", font=("Segoe UI", 12)).pack(pady=10)
cantidad_entry = tb.Entry(card, width=10)
cantidad_entry.pack()

def al_hacer_click():
    if not combo.get():
        messagebox.showwarning("Aviso", "Seleccioná un producto.")
        return
    try:
        cantidad = int(cantidad_entry.get())
    except:
        messagebox.showwarning("Aviso", "Cantidad inválida.")
        return

    pid, desc = producto_dict[combo.get()]
    generar_y_imprimir_qrs(pid, desc, cantidad)
    guardar_config(combo.get(), cantidad)

tb.Button(card, text="GENERAR", bootstyle=SUCCESS, command=al_hacer_click).pack(pady=25)

#cfg = cargar_config()
#if "producto" in cfg:
#    combo.set(cfg["producto"])
#if "cantidad" in cfg:
#    cantidad_entry.insert(0, cfg["cantidad"])

root.mainloop()
