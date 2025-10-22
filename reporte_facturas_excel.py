
import os
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

# Ruta del directorio donde está este script
script_dir = os.path.dirname(os.path.abspath(__file__))

def obtener_folios_pagados_manualmente():
    # Ruta absoluta del directorio del script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "pagadas_manual.csv")

    if not os.path.exists(csv_path):
        return set()
    try:
        df = pd.read_csv(csv_path)
        return set(df["Folio"].astype(str).str.upper())
    except Exception as e:
        print(f"Error leyendo el archivo CSV de pagos manuales: {e}")
        return set()
    
def obtener_uuids_pagados(carpeta_complementos):
    ns = {
        'cfdi': 'http://www.sat.gob.mx/cfd/4',
        'pago20': 'http://www.sat.gob.mx/Pagos20'
    }
    uuids_pagados = set()
    for archivo in os.listdir(carpeta_complementos):
        if archivo.endswith(".xml"):
            ruta = os.path.join(carpeta_complementos, archivo)
            try:
                tree = ET.parse(ruta)
                root = tree.getroot()
                doctos = root.findall('.//pago20:DoctoRelacionado', ns)
                for d in doctos:
                    uuid = d.attrib.get("IdDocumento")
                    if uuid:
                        uuids_pagados.add(uuid.upper())
            except Exception as e:
                print(f"Error procesando complemento {archivo}: {e}")
    # print(f"🟢 UUIDs encontrados en complementos:\n{uuids_pagados}")
    return uuids_pagados

def convertir_a_dias(condiciones):
    condiciones = condiciones.strip().lower()
    if "semana" in condiciones:
        for palabra in condiciones.split():
            if palabra.isdigit():
                return int(palabra) * 7
            elif palabra in ["una", "un"]:
                return 7
        return 14
    try:
        return int(condiciones.split()[0])
    except:
        return 0

def procesar_facturas_emitidas(carpeta_facturas, uuids_pagados, folios_pagados_manual):
    ns = {
        'cfdi': 'http://www.sat.gob.mx/cfd/4',
        'tfd': 'http://www.sat.gob.mx/TimbreFiscalDigital'
    }
    detalles_por_cliente = defaultdict(list)
    resumen_clientes = {}

    for archivo in os.listdir(carpeta_facturas):
        if archivo.endswith(".xml"):
            ruta = os.path.join(carpeta_facturas, archivo)
            try:
                tree = ET.parse(ruta)
                root = tree.getroot()
                comprobante = root

                if comprobante.attrib.get("TipoDeComprobante", "").upper() != "I":
                    continue

                receptor = comprobante.find('cfdi:Receptor', ns)
                timbre = root.find('.//tfd:TimbreFiscalDigital', ns)

                uuid = timbre.attrib.get("UUID", "SIN_UUID").upper()
                folio = comprobante.attrib.get('Folio', '').upper()
                fecha_emision = comprobante.attrib.get('Fecha', '')
                total = float(comprobante.attrib.get('Total', '0'))
                moneda = comprobante.attrib.get('Moneda', '')
                metodo_pago = comprobante.attrib.get('MetodoPago', '')
                condiciones_pago = comprobante.attrib.get('CondicionesDePago', '0 DIAS')
                cliente_nombre = receptor.attrib.get('Nombre', 'SIN NOMBRE')
                cliente_rfc = receptor.attrib.get('Rfc', 'SIN RFC')

                fecha_emision_dt = datetime.strptime(fecha_emision[:10], '%Y-%m-%d')
                dias_credito = convertir_a_dias(condiciones_pago)
                fecha_vencimiento = fecha_emision_dt + timedelta(days=dias_credito)
                hoy = datetime.now()
                dias_por_vencer = (fecha_vencimiento - hoy).days
                condiciones_es_contado = condiciones_pago.strip().lower() == "contado"
                pagada = condiciones_es_contado or uuid in uuids_pagados or folio in folios_pagados_manual
                if pagada:
                    estatus = "PAGADA"
                else:
                    estatus = "VENCIDA" if dias_por_vencer < 0 else "POR PAGAR"
                vencida = estatus == "VENCIDA"

                total_mxn = total if not pagada and moneda == "MXN" else 0
                total_usd = total if not pagada and moneda == "USD" else 0
                vencidas_mxn = total if vencida and moneda == "MXN" else 0
                vencidas_usd = total if vencida and moneda == "USD" else 0

                detalles_por_cliente[(cliente_nombre, cliente_rfc)].append({
                    "UUID": uuid,
                    "Folio": folio,
                    "Fecha de Emisión": fecha_emision_dt.strftime('%d/%m/%Y'),
                    "Fecha de Vencimiento": fecha_vencimiento.strftime('%d/%m/%Y'),
                    "Moneda": moneda,
                    "Método de Pago": metodo_pago,
                    "Condiciones de Pago": condiciones_pago,
                    "Días por Vencer / Vencidos": dias_por_vencer,
                    "¿Pagada?": "Sí" if pagada else "No",
                    "Estatus": estatus,
                    "Total Factura": total,
                    "Total MXN": total_mxn,
                    "Total USD": total_usd
                })

                clave = (cliente_nombre, cliente_rfc)
                if clave not in resumen_clientes:
                    resumen_clientes[clave] = {
                        "MXN": 0, "USD": 0, "Facturas": 0,
                        "Vencidas_MXN": 0, "Vencidas_USD": 0
                    }

                if not pagada:
                    if moneda == "MXN":
                        resumen_clientes[clave]["MXN"] += total
                    elif moneda == "USD":
                        resumen_clientes[clave]["USD"] += total

                if vencida:
                    if moneda == "MXN":
                        resumen_clientes[clave]["Vencidas_MXN"] += total
                    elif moneda == "USD":
                        resumen_clientes[clave]["Vencidas_USD"] += total

                resumen_clientes[clave]["Facturas"] += 1

            except Exception as e:
                print(f"Error procesando factura {archivo}: {e}")
    return detalles_por_cliente, resumen_clientes

def generar_hoja_vencidas_y_proximas(detalles_por_cliente):
    registros = []
    for (cliente, _), facturas in detalles_por_cliente.items():
        for factura in facturas:
            dias = factura["Días por Vencer / Vencidos"]
            estatus = factura["Estatus"]
            if estatus == "VENCIDA" or (estatus == "POR PAGAR" and dias <= 7):
                registros.append({
                    "Cliente": cliente,
                    "Días vencidos / por vencer": dias,
                    "Fecha de Vencimiento": factura["Fecha de Vencimiento"],
                    "Número de Factura": factura["Folio"],
                    "Total": factura["Total Factura"],
                    "Moneda": factura["Moneda"]
                })
    return pd.DataFrame(registros)

def generar_excel(carpeta_facturas, carpeta_complementos):
    hoy = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    archivo_salida = f"/Users/amauryperezverdejo/Downloads/cuentas_clientes_{hoy}.xlsx"


    folios_pagados_manual = obtener_folios_pagados_manualmente()
    uuids_pagados = obtener_uuids_pagados(carpeta_complementos)
    detalles_por_cliente, resumen_clientes = procesar_facturas_emitidas(
        carpeta_facturas, uuids_pagados, folios_pagados_manual
    )

    resumen_data = []
    hojas_clientes = {}

    for (nombre, rfc), totales in resumen_clientes.items():
        resumen_data.append({
            "Cliente (Razón Social)": nombre,
            "RFC Cliente": rfc,
            "Total por Cobrar MXN": totales["MXN"],
            "Total por Cobrar USD": totales["USD"],
            "Nº Facturas": totales["Facturas"],
            "Vencidas MXN": totales["Vencidas_MXN"],
            "Vencidas USD": totales["Vencidas_USD"]
        })

    for (nombre, rfc), facturas in detalles_por_cliente.items():
        hoja_nombre = nombre[:31]
        df = pd.DataFrame(facturas)

        columnas_ordenadas = [
            "UUID", "Folio", "Fecha de Emisión", "Fecha de Vencimiento",
            "Moneda", "Método de Pago", "Condiciones de Pago",
            "Días por Vencer / Vencidos", "¿Pagada?", "Estatus",
            "Total Factura", "Total MXN", "Total USD"
        ]
        df = df[columnas_ordenadas]

        totales = {
            "UUID": "TOTAL",
            "Total Factura": df["Total Factura"].sum(),
            "Total MXN": df["Total MXN"].sum(),
            "Total USD": df["Total USD"].sum()
        }
        df = pd.concat([df, pd.DataFrame([totales])], ignore_index=True)

        hojas_clientes[hoja_nombre] = df

    hoja_alertas = generar_hoja_vencidas_y_proximas(detalles_por_cliente)

    with pd.ExcelWriter(archivo_salida, engine='xlsxwriter') as writer:
        hoja_alertas.to_excel(writer, sheet_name="Alertas Vencimientos", index=False)
        pd.DataFrame(resumen_data).to_excel(writer, sheet_name="Resumen", index=False)
        for hoja, df in hojas_clientes.items():
            df.to_excel(writer, sheet_name=hoja, index=False)

    print(f"✅ Archivo generado: {archivo_salida}")

if __name__ == "__main__":
    generar_excel(
        "/Users/amauryperezverdejo/Library/CloudStorage/OneDrive-Personal/DOCUMENTOS GUBA/FACTURAS GUBA/FACTURAS 2025",
        "/Users/amauryperezverdejo/Library/CloudStorage/OneDrive-Personal/DOCUMENTOS GUBA/FACTURAS GUBA/FACTURAS 2025"
    )
