# ============================================================================
# 📞 CDR OIKOST - MERGE INCREMENTAL DE DATOS CRUDOS (PARA GITHUB ACTIONS)
# ============================================================================

import os
import requests
import pandas as pd
from tqdm import tqdm
import urllib3
import time
import oracledb
import sys
from datetime import datetime, timedelta

urllib3.disable_warnings()

print("=" * 80)
print("🚀 INICIANDO MERGE INCREMENTAL DE DATOS CRUDOS OIKOST")
print("=" * 80)

# ============================================================================
# 🛠️ CONFIGURACIÓN CON VARIABLES DE ENTORNO (SECRETS)
# ============================================================================

# --- Credenciales de Oracle desde secrets ---
ORACLE_USER = os.environ.get('ORACLE_USER')
ORACLE_PASSWORD = os.environ.get('ORACLE_PASSWORD')
ORACLE_DSN = os.environ.get('ORACLE_DSN')

# --- API OIKOST (token desde secrets) ---
API_URL = "https://oikost.anw.cloud/api/integration/cdr/all"
TOKEN_BASIC = os.environ.get('OIKOST_TOKEN')

# --- Tabla destino ---
TABLE_NAME = "CDR_OIKOST_CRUDO"

# Verificar credenciales obligatorias
if not all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN, TOKEN_BASIC]):
    print("❌ FALTAN CREDENCIALES. Verifica los secrets:")
    if not ORACLE_USER: print("   - ORACLE_USER")
    if not ORACLE_PASSWORD: print("   - ORACLE_PASSWORD")
    if not ORACLE_DSN: print("   - ORACLE_DSN")
    if not TOKEN_BASIC: print("   - OIKOST_TOKEN")
    sys.exit(1)

print(f"🔌 API: {API_URL}")
print(f"🗄️ Tabla destino: {TABLE_NAME}")

# ============================================================================
# 📅 FUNCIÓN PARA OBTENER ÚLTIMA FECHA EN ORACLE
# ============================================================================

def obtener_ultima_fecha_oracle():
    """Obtiene el valor máximo de calldate (string) de la tabla Oracle"""
    try:
        connection = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=ORACLE_DSN
        )
        cursor = connection.cursor()
        
        # Verificar si la tabla existe
        cursor.execute("""
            SELECT COUNT(*) FROM ALL_TABLES 
            WHERE TABLE_NAME = UPPER(:1)
        """, [TABLE_NAME])
        tabla_existe = cursor.fetchone()[0] > 0
        
        if not tabla_existe:
            print("📅 Tabla no existe - será primera carga")
            cursor.close()
            connection.close()
            return None
        
        # Obtener máximo calldate
        cursor.execute(f'SELECT MAX("calldate") FROM "{TABLE_NAME}"')
        max_calldate_str = cursor.fetchone()[0]
        
        cursor.close()
        connection.close()
        
        if max_calldate_str:
            print(f"📅 Última fecha (string) en BD: {max_calldate_str}")
            return pd.to_datetime(max_calldate_str)
        else:
            print("📅 Tabla vacía - primera carga")
            return None
            
    except Exception as e:
        print(f"⚠️ Error verificando tabla: {e}")
        return None

# ============================================================================
# 📥 FUNCIÓN PARA DESCARGAR DATOS NUEVOS
# ============================================================================

def descargar_datos_nuevos(ultima_fecha):
    """Descarga todas las páginas y filtra localmente registros desde ultima_fecha - 3 días"""
    print("\n📥 Descargando datos nuevos desde oikost...")
    
    session = requests.Session()
    session.verify = False
    session.headers.update({
        'Authorization': TOKEN_BASIC,
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'application/json'
    })
    
    # Calcular fecha límite
    if ultima_fecha:
        fecha_limite = ultima_fecha - timedelta(days=3)
        print(f"📅 Buscando desde: {fecha_limite} (última fecha - 3 días)")
    else:
        fecha_limite = datetime.now() - timedelta(days=30)
        print(f"📅 Primera carga: desde {fecha_limite}")
    
    todos_datos = []
    pagina = 1
    total_paginas = None
    pbar = None
    
    try:
        while True:
            response = session.get(f"{API_URL}?page={pagina}", timeout=60)
            
            if response.status_code != 200:
                print(f"❌ Error {response.status_code} en página {pagina}")
                break
            
            data = response.json()
            registros = data.get('data', [])
            
            if pagina == 1:
                total_registros = data.get('total', 0)
                total_paginas = data.get('totalPages', 1)
                print(f"📊 API tiene {total_registros:,} registros totales")
                print(f"📑 Total páginas: {total_paginas}")
                
                if total_registros == 0:
                    print("⚠️ No hay datos")
                    return []
                
                pbar = tqdm(total=total_paginas, desc="Descargando páginas")
            
            todos_datos.extend(registros)
            if pbar:
                pbar.update(1)
            
            if pagina >= total_paginas:
                break
                
            pagina += 1
            time.sleep(0.3)
            
    except Exception as e:
        print(f"❌ Error en descarga: {e}")
    finally:
        if pbar:
            pbar.close()
    
    print(f"✅ Descargados {len(todos_datos):,} registros crudos")
    
    # Filtrar por fecha
    if todos_datos:
        df_temp = pd.DataFrame(todos_datos)
        df_temp['calldate_dt'] = pd.to_datetime(df_temp['calldate'], errors='coerce')
        
        df_filtrado = df_temp[df_temp['calldate_dt'] >= fecha_limite]
        print(f"🔍 Después de filtrar por fecha: {len(df_filtrado)} registros")
        
        if ultima_fecha:
            df_filtrado = df_filtrado[df_filtrado['calldate_dt'] > ultima_fecha]
            print(f"🔍 Después de filtrar > última fecha: {len(df_filtrado)} registros realmente nuevos")
        
        # Eliminar columna temporal
        df_filtrado = df_filtrado.drop(columns=['calldate_dt'])
        return df_filtrado.to_dict('records')
    
    return todos_datos

# ============================================================================
# 📦 FUNCIÓN DE MERGE EN ORACLE
# ============================================================================

def merge_en_oracle(datos):
    if not datos:
        print("⚠️ No hay datos nuevos para procesar")
        return 0
    
    print(f"\n📦 Procesando MERGE de {len(datos):,} registros en {TABLE_NAME}...")
    
    connection = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN
    )
    cursor = connection.cursor()
    
    try:
        # Obtener columnas
        columnas = list(datos[0].keys())
        print(f"   📋 Columnas detectadas: {columnas}")
        
        # Construir partes del MERGE
        set_clause = ", ".join([f'T."{col}" = S."{col}"' for col in columnas if col != 'uniqueid'])
        cols_insert = ", ".join([f'"{col}"' for col in columnas])
        vals_insert = ", ".join([f'S."{col}"' for col in columnas])
        
        # Crear tabla temporal
        temp_table = f"{TABLE_NAME}_TEMP_{int(time.time())}"
        cursor.execute(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {temp_table}'; EXCEPTION WHEN OTHERS THEN NULL; END;")
        
        col_defs = ", ".join([f'"{col}" VARCHAR2(4000)' for col in columnas])
        cursor.execute(f"CREATE TABLE {temp_table} ({col_defs})")
        
        # Insertar datos en temporal
        placeholders = ", ".join([f':{i+1}' for i in range(len(columnas))])
        insert_sql = f"INSERT INTO {temp_table} VALUES ({placeholders})"
        
        batch_size = 5000
        total = len(datos)
        with tqdm(total=total, desc="Insertando en temporal") as pbar:
            for i in range(0, total, batch_size):
                batch = datos[i:i+batch_size]
                batch_tuplas = []
                for reg in batch:
                    tupla = tuple(reg.get(col, None) for col in columnas)
                    batch_tuplas.append(tupla)
                cursor.executemany(insert_sql, batch_tuplas)
                connection.commit()
                pbar.update(len(batch))
        
        # Ejecutar MERGE
        print("   🔄 Ejecutando MERGE...")
        merge_sql = f"""
            MERGE INTO {TABLE_NAME} T
            USING {temp_table} S
            ON (T."uniqueid" = S."uniqueid")
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN
                INSERT ({cols_insert}) VALUES ({vals_insert})
        """
        cursor.execute(merge_sql)
        connection.commit()
        
        # Limpiar
        cursor.execute(f"DROP TABLE {temp_table}")
        connection.commit()
        
        # Verificar
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        total_insertado = cursor.fetchone()[0]
        print(f"✅ Total registros en tabla: {total_insertado:,}")
        
        return len(datos)
        
    except Exception as e:
        print(f"❌ Error en MERGE: {e}")
        try:
            cursor.execute(f"DROP TABLE {temp_table}")
            connection.commit()
        except:
            pass
        return 0
    finally:
        cursor.close()
        connection.close()

# ============================================================================
# 🎯 FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    inicio_total = time.time()
    
    print(f"\n{'='*60}")
    print("🎯 INICIANDO PROCESO - MERGE INCREMENTAL")
    print(f"{'='*60}")
    
    # 1. Obtener última fecha
    ultima_fecha = obtener_ultima_fecha_oracle()
    
    # 2. Descargar datos nuevos
    datos_nuevos = descargar_datos_nuevos(ultima_fecha)
    
    if not datos_nuevos:
        print("✅ No hay datos nuevos para procesar")
        return
    
    # 3. Mostrar muestra
    print(f"\n🔍 Muestra del primer registro nuevo:")
    for k, v in list(datos_nuevos[0].items())[:10]:
        print(f"   {k}: {v}")
    
    # 4. Hacer MERGE
    insertados = merge_en_oracle(datos_nuevos)
    
    # Tiempo total
    tiempo_total = time.time() - inicio_total
    minutos = int(tiempo_total // 60)
    segundos = int(tiempo_total % 60)
    
    print(f"\n{'='*60}")
    print(f"✅ PROCESO COMPLETADO EN {minutos} min {segundos} seg")
    print(f"   Registros nuevos procesados: {insertados}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
