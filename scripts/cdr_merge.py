# ============================================================================
# 📞 CDR LLAMADAS - VERSIÓN DEFINITIVA CON LLAVE ÚNICA
# ============================================================================

import requests
import pandas as pd
import base64
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from tqdm import tqdm
import urllib3
import time
import numpy as np
import oracledb
import os
import sys

urllib3.disable_warnings()

print("=" * 80)
print("🚀 INICIANDO PROCESO DE ACTUALIZACIÓN CDR")
print("=" * 80)

# ============================================================================
# 🛠️ CONFIGURACIÓN CON VARIABLES DE ENTORNO
# ============================================================================

# --- Credenciales de Oracle desde Secrets ---
ORACLE_USER = os.environ.get('ORACLE_USER')
ORACLE_PASSWORD = os.environ.get('ORACLE_PASSWORD')
ORACLE_DSN = os.environ.get('ORACLE_DSN')

# --- Credenciales de API desde Secrets ---
API_URL = "https://oikoscall.anw.cloud/api/integration/cdr/all"
API_USER = os.environ.get('API_USER', 'oikosadm')
API_PASSWORD = os.environ.get('API_PASSWORD')

TABLE_NAME = "CDR_LLAMADAS"

# Verificar que todas las credenciales están presentes
if not all([ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN, API_USER, API_PASSWORD]):
    print("❌ Error: Faltan credenciales en las variables de entorno")
    sys.exit(1)

# --- Basic Auth ---
basic_token = base64.b64encode(f"{API_USER}:{API_PASSWORD}".encode()).decode()
headers = {
    'Authorization': f'Basic {basic_token}',
    'Content-Type': 'application/json'
}

print(f"🔌 Configuración:")
print(f"   URL: {API_URL}")
print(f"   Usuario: {API_USER}")

# ============================================================================
# 📅 FUNCIÓN PARA OBTENER ÚLTIMA FECHA EN ORACLE
# ============================================================================

def obtener_ultima_fecha_oracle():
    """Obtiene la última fecha en Oracle"""
    try:
        connection = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=ORACLE_DSN
        )
        cursor = connection.cursor()
        
        # Verificar si la tabla existe
        cursor.execute("""
            SELECT COUNT(*) 
            FROM ALL_TABLES 
            WHERE TABLE_NAME = UPPER(:1)
        """, [TABLE_NAME])
        
        tabla_existe = cursor.fetchone()[0] > 0
        
        if not tabla_existe:
            print(f"📅 Tabla no existe - será primera carga")
            cursor.close()
            connection.close()
            return None, False
        
        # Verificar si la columna LLAVE_UNICA existe
        cursor.execute("""
            SELECT COUNT(*) 
            FROM ALL_TAB_COLUMNS 
            WHERE TABLE_NAME = UPPER(:1) 
            AND COLUMN_NAME = 'LLAVE_UNICA'
        """, [TABLE_NAME])
        
        tiene_llave = cursor.fetchone()[0] > 0
        
        # Obtener máxima fecha
        cursor.execute(f'SELECT MAX("CALLLATE") FROM "{TABLE_NAME}"')
        ultima_fecha = cursor.fetchone()[0]
        
        cursor.close()
        connection.close()
        
        if ultima_fecha:
            print(f"📅 Última fecha en BD: {ultima_fecha}")
            return ultima_fecha, tiene_llave
        else:
            print(f"📅 Tabla vacía - primera carga")
            return None, tiene_llave
            
    except Exception as e:
        print(f"⚠️ Error verificando tabla: {e}")
        return None, False

# ============================================================================
# 🧹 FUNCIÓN PARA LIMPIAR TABLAS TEMPORALES HUÉRFANAS
# ============================================================================

def limpiar_tablas_temporales():
    """Elimina todas las tablas temporales huérfanas"""
    print(f"\n🧹 Limpiando tablas temporales huérfanas...")
    
    try:
        connection = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=ORACLE_DSN
        )
        cursor = connection.cursor()
        
        # Buscar todas las tablas temporales
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM ALL_TABLES 
            WHERE TABLE_NAME LIKE 'CDR_LLAMADAS_TEMP_%'
        """)
        
        tablas_temp = cursor.fetchall()
        
        if tablas_temp:
            print(f"   🔍 Encontradas {len(tablas_temp)} tablas temporales")
            
            for (tabla,) in tablas_temp:
                try:
                    cursor.execute(f"DROP TABLE {tabla}")
                    print(f"   ✅ Eliminada: {tabla}")
                except Exception as e:
                    print(f"   ❌ Error eliminando {tabla}: {e}")
            
            connection.commit()
        else:
            print(f"   ✅ No hay tablas temporales huérfanas")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Error limpiando tablas temporales: {e}")

# ============================================================================
# 📥 FUNCIÓN PARA DESCARGAR Y FILTRAR ÚLTIMOS 3 DÍAS
# ============================================================================

def descargar_ultimos_3_dias(ultima_fecha):
    """
    Descarga todo y filtra localmente SOLO los últimos 3 días
    """
    print(f"\n🚀 Descargando datos para filtrar últimos 3 días...")
    
    session = requests.Session()
    session.verify = False
    session.headers.update(headers)
    
    # Calcular fecha límite (últimos 3 días)
    if ultima_fecha:
        # Si ya hay datos, buscar desde 3 días antes de la última fecha
        fecha_limite = ultima_fecha - timedelta(days=3)
        print(f"📅 Buscando registros desde: {fecha_limite} (última fecha - 3 días)")
    else:
        # Primera carga: últimos 30 días
        fecha_limite = datetime.now() - timedelta(days=30)
        print(f"📅 Primera carga: desde {fecha_limite}")
    
    todos_datos = []
    
    try:
        # Descargar primera página
        response = session.get(f"{API_URL}?page=1", timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            total_api = data.get('total', 0)
            total_paginas = data.get('totalPages', 1)
            print(f"✅ API tiene {total_api:,} registros totales en {total_paginas} páginas")
            
            # Descargar todas las páginas
            pbar = tqdm(total=total_paginas, desc="Descargando páginas")
            
            for pagina in range(1, total_paginas + 1):
                response = session.get(f"{API_URL}?page={pagina}", timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    todos_datos.extend(data.get('data', []))
                pbar.update(1)
            
            pbar.close()
            
            print(f"✅ Descargados {len(todos_datos):,} registros crudos")
            
            # FILTRADO LOCAL por fecha
            if todos_datos:
                df_temp = pd.DataFrame(todos_datos)
                
                # Convertir y ajustar zona horaria
                df_temp['calldate'] = pd.to_datetime(df_temp['calldate'])
                df_temp['calldate'] = df_temp['calldate'] - pd.Timedelta(hours=5)
                df_temp['calldate'] = df_temp['calldate'].dt.tz_localize(None)  # Quitar zona horaria
                
                # Filtrar por fecha límite
                df_filtrado = df_temp[df_temp['calldate'] >= fecha_limite]
                
                print(f"🔍 Después de filtrar por fecha >= {fecha_limite}: {len(df_filtrado):,} registros")
                
                # Si ya hay datos en BD, filtrar solo los más nuevos
                if ultima_fecha:
                    df_filtrado = df_filtrado[df_filtrado['calldate'] > ultima_fecha]
                    print(f"🔍 Después de filtrar > última fecha: {len(df_filtrado):,} registros realmente nuevos")
                
                return df_filtrado.to_dict('records')
            
        else:
            print(f"❌ Error API: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return []
    
    return []

# ============================================================================
# 🔑 FUNCIÓN PARA GENERAR LLAVE ÚNICA
# ============================================================================

def generar_llave_unica(row):
    """Genera llave única: YYYY-MM-DD_UNIQUEID"""
    calldate_str = row['calldate'].strftime('%Y-%m-%d')
    uniqueid = str(row['uniqueid'])
    return f"{calldate_str}_{uniqueid}"

# ============================================================================
# 🔄 PROCESAR DATOS
# ============================================================================

def procesar_datos(datos):
    """Procesa los datos y genera llave única"""
    if not datos:
        return pd.DataFrame()
    
    df = pd.DataFrame(datos)
    print(f"\n⚙️ Procesando {len(df):,} registros...")
    
    # 1. Asegurar formato de fechas
    df['calldate'] = pd.to_datetime(df['calldate'])
    df['calldate'] = df['calldate'] - pd.Timedelta(hours=5)
    df['calldate'] = df['calldate'].dt.tz_localize(None)
    
    # 2. Filtros de Power Query
    condiciones = (
        (df['dcontext'] != 'HangupCall') &
        (df['lastapp'] != 'Congestion') &
        (df['disposition'] != 'FAILED') &
        (df['dst'] != 's') & 
        (df['dst'] != '*65') & 
        (df['src'] != 'start') & 
        (df['src'] != 'anonymous')
    )
    df = df[condiciones].copy()
    
    # 3. Quitar duplicados
    df = df.drop_duplicates(subset=['uniqueid'], keep='last')
    
    # 4. Separar fecha y hora
    df['calldate_date'] = df['calldate'].dt.date
    df['callhour'] = df['calldate'].dt.strftime('%H:%M:%S')
    
    # 5. Clasificar llamadas
    def get_calltype(row):
        channel = str(row.get('channel', ''))
        dstchannel = str(row.get('dstchannel', ''))
        
        if 'Nebula_World' in channel:
            return 'ENTRANTE'
        elif 'Nebula_Loqui' in dstchannel or 'Nebula_Loqui' in channel:
            return 'SALIENTE'
        return 'INTERNO'
    
    df['calltype'] = df.apply(get_calltype, axis=1)
    
    # 6. GENERAR LLAVE ÚNICA
    df['llave_unica'] = df.apply(generar_llave_unica, axis=1)
    
    # 7. Columnas finales
    columnas = {
        'calldate_date': 'CALLLATE',
        'callhour': 'CALLHOUR',
        'clid': 'CLID',
        'src': 'SRC',
        'dst': 'DST',
        'dcontext': 'DCONTEXT',
        'channel': 'CHANNEL',
        'dstchannel': 'DSTCHANNEL',
        'lastapp': 'LASTAPP',
        'duration': 'DURATION',
        'disposition': 'DISPOSITION',
        'uniqueid': 'UNIQUEID',
        'calltype': 'CALLTYPE',
        'llave_unica': 'LLAVE_UNICA'
    }
    
    df_final = df[list(columnas.keys())].rename(columns=columnas)
    df_final['CALLLATE'] = pd.to_datetime(df_final['CALLLATE'])
    df_final['CALLHOUR'] = df_final['CALLHOUR'].astype(str)
    df_final['DURATION'] = pd.to_numeric(df_final['DURATION'], errors='coerce').fillna(0).astype(int)
    
    return df_final

# ============================================================================
# 🏗️ FUNCIÓN PARA CREAR TABLA (CON LLAVE ÚNICA)
# ============================================================================

def crear_tabla_oracle():
    """Crea la tabla en Oracle con LLAVE_UNICA incluida"""
    print(f"\n🏗️ Creando tabla {TABLE_NAME}...")
    
    connection = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN
    )
    cursor = connection.cursor()
    
    cursor.execute(f"""
        CREATE TABLE {TABLE_NAME} (
            CALLLATE DATE,
            CALLHOUR VARCHAR2(20),
            CLID VARCHAR2(255),
            SRC VARCHAR2(100),
            DST VARCHAR2(100),
            DCONTEXT VARCHAR2(100),
            CHANNEL VARCHAR2(255),
            DSTCHANNEL VARCHAR2(255),
            LASTAPP VARCHAR2(100),
            DURATION NUMBER(10),
            DISPOSITION VARCHAR2(50),
            UNIQUEID VARCHAR2(100),
            CALLTYPE VARCHAR2(50),
            LLAVE_UNICA VARCHAR2(100) PRIMARY KEY,
            FECHA_INSERCION TIMESTAMP DEFAULT SYSTIMESTAMP
        )
    """)
    connection.commit()
    
    print(f"✅ Tabla creada exitosamente")
    cursor.close()
    connection.close()

# ============================================================================
# 🔧 FUNCIÓN PARA AGREGAR LLAVE ÚNICA A TABLA EXISTENTE
# ============================================================================

def agregar_llave_unica_a_tabla_existente():
    """Agrega la columna LLAVE_UNICA a una tabla existente"""
    print(f"\n🔧 Agregando columna LLAVE_UNICA a tabla existente...")
    
    connection = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN
    )
    cursor = connection.cursor()
    
    try:
        # Agregar columna
        cursor.execute(f"""
            ALTER TABLE {TABLE_NAME} ADD (
                LLAVE_UNICA VARCHAR2(100)
            )
        """)
        
        # Actualizar registros existentes
        cursor.execute(f"""
            UPDATE {TABLE_NAME} 
            SET LLAVE_UNICA = TO_CHAR(CALLLATE, 'YYYY-MM-DD') || '_' || UNIQUEID
            WHERE LLAVE_UNICA IS NULL
        """)
        
        # Crear índice único
        cursor.execute(f"""
            CREATE UNIQUE INDEX IDX_{TABLE_NAME}_LLAVE ON {TABLE_NAME} (LLAVE_UNICA)
        """)
        
        connection.commit()
        print(f"✅ Columna LLAVE_UNICA agregada exitosamente")
        
    except Exception as e:
        print(f"⚠️ La columna ya existe o hubo un error: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

# ============================================================================
# 🚀 FUNCIÓN DE MERGE EXPRESS - CON LIMPIEZA DE TEMPS
# ============================================================================

def merge_express_oracle(df, tiene_llave):
    """Hace MERGE de los datos en Oracle - CON LIMPIEZA DE TEMPS"""
    
    if df.empty:
        print(f"⚠️ No hay datos para procesar")
        return 0
    
    print(f"\n⚡ MERGE EXPRESS ⚡")
    print(f"   Procesando {len(df):,} registros...")
    
    inicio_merge = time.time()
    temp_table = f"{TABLE_NAME}_TEMP_{int(time.time())}"
    
    connection = None
    cursor = None
    
    try:
        connection = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=ORACLE_DSN
        )
        cursor = connection.cursor()
        
        # Si la tabla no tiene la columna LLAVE_UNICA, agregarla
        if not tiene_llave:
            agregar_llave_unica_a_tabla_existente()
        
        # Crear tabla temporal (asegurar que no existe)
        print(f"   🏗️ Creando tabla temporal {temp_table}...")
        cursor.execute(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {temp_table}'; EXCEPTION WHEN OTHERS THEN NULL; END;")
        cursor.execute(f"""
            CREATE TABLE {temp_table} AS 
            SELECT * FROM {TABLE_NAME} WHERE 1=0
        """)
        
        # Preparar datos CON FECHAS EN FORMATO CORRECTO
        print(f"   📦 Preparando {len(df)} registros...")
        datos_para_insert = []
        
        for _, row in df.iterrows():
            # CONVERTIR FECHA A STRING EN FORMATO YYYY-MM-DD
            fecha_str = row['CALLLATE'].strftime('%Y-%m-%d')
            
            datos_para_insert.append((
                fecha_str,  # Fecha como string
                row['CALLHOUR'],
                row['CLID'],
                row['SRC'],
                row['DST'],
                row['DCONTEXT'],
                row['CHANNEL'],
                row['DSTCHANNEL'],
                row['LASTAPP'],
                int(row['DURATION']),
                row['DISPOSITION'],
                row['UNIQUEID'],
                row['CALLTYPE'],
                row['LLAVE_UNICA']
            ))
        
        # Insertar en temporal por lotes
        print(f"   📦 Insertando en tabla temporal...")
        batch_size = 5000
        
        for i in range(0, len(datos_para_insert), batch_size):
            batch = datos_para_insert[i:i+batch_size]
            cursor.executemany(f"""
                INSERT INTO {temp_table} (
                    CALLLATE, CALLHOUR, CLID, SRC, DST, DCONTEXT, 
                    CHANNEL, DSTCHANNEL, LASTAPP, DURATION, DISPOSITION, 
                    UNIQUEID, CALLTYPE, LLAVE_UNICA
                ) VALUES (
                    TO_DATE(:1, 'YYYY-MM-DD'), :2, :3, :4, :5, :6, 
                    :7, :8, :9, :10, :11, :12, :13, :14
                )
            """, batch)
            connection.commit()
        
        # Hacer MERGE
        print(f"   🔄 Ejecutando MERGE...")
        
        merge_sql = f"""
            MERGE INTO {TABLE_NAME} T
            USING {temp_table} S
            ON (T.LLAVE_UNICA = S.LLAVE_UNICA)
            WHEN MATCHED THEN
                UPDATE SET 
                    T.CALLLATE = S.CALLLATE,
                    T.CALLHOUR = S.CALLHOUR,
                    T.CLID = S.CLID,
                    T.SRC = S.SRC,
                    T.DST = S.DST,
                    T.DCONTEXT = S.DCONTEXT,
                    T.CHANNEL = S.CHANNEL,
                    T.DSTCHANNEL = S.DSTCHANNEL,
                    T.LASTAPP = S.LASTAPP,
                    T.DURATION = S.DURATION,
                    T.DISPOSITION = S.DISPOSITION,
                    T.CALLTYPE = S.CALLTYPE,
                    T.FECHA_INSERCION = SYSTIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT (CALLLATE, CALLHOUR, CLID, SRC, DST, DCONTEXT, 
                        CHANNEL, DSTCHANNEL, LASTAPP, DURATION, DISPOSITION, 
                        UNIQUEID, CALLTYPE, LLAVE_UNICA)
                VALUES (S.CALLLATE, S.CALLHOUR, S.CLID, S.SRC, S.DST, S.DCONTEXT,
                        S.CHANNEL, S.DSTCHANNEL, S.LASTAPP, S.DURATION, S.DISPOSITION,
                        S.UNIQUEID, S.CALLTYPE, S.LLAVE_UNICA)
        """
        
        cursor.execute(merge_sql)
        connection.commit()
        
        # LIMPIEZA: Eliminar tabla temporal (SIEMPRE)
        print(f"   🧹 Eliminando tabla temporal...")
        cursor.execute(f"DROP TABLE {temp_table}")
        connection.commit()
        
        # Verificar
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        total = cursor.fetchone()[0]
        
        tiempo_merge = time.time() - inicio_merge
        
        print(f"✅ ¡MERGE COMPLETADO!")
        print(f"   📊 Total en tabla: {total:,} registros")
        print(f"   ✨ Nuevos en esta ejecución: {len(df)}")
        print(f"   ⏱️  Tiempo: {tiempo_merge:.2f} segundos")
        
        return len(df)
        
    except Exception as e:
        print(f"❌ Error en MERGE: {e}")
        # INTENTAR LIMPIAR LA TABLA TEMPORAL AUNQUE HAYA ERROR
        if cursor and temp_table:
            try:
                cursor.execute(f"DROP TABLE {temp_table}")
                connection.commit()
                print(f"   🧹 Tabla temporal eliminada después del error")
            except:
                pass
        raise e
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# ============================================================================
# 🎯 FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    inicio_total = time.time()
    
    print(f"\n{'='*60}")
    print(f"🎯 INICIANDO PROCESO - ÚLTIMOS 3 DÍAS")
    print(f"{'='*60}")
    
    # 0. LIMPIAR TABLAS TEMPORALES HUÉRFANAS (SIEMPRE AL INICIAR)
    limpiar_tablas_temporales()
    
    # 1. Obtener última fecha en Oracle y verificar si tiene la columna llave
    ultima_fecha, tiene_llave = obtener_ultima_fecha_oracle()
    
    # 2. Si la tabla no existe, crearla
    if ultima_fecha is None and not tiene_llave:
        crear_tabla_oracle()
        tiene_llave = True
    
    # 3. Descargar y filtrar localmente últimos 3 días
    datos_filtrados = descargar_ultimos_3_dias(ultima_fecha)
    
    if not datos_filtrados:
        print(f"✅ No hay datos nuevos en los últimos 3 días")
        return
    
    # 4. Procesar datos
    df = procesar_datos(datos_filtrados)
    
    # 5. Mostrar estadísticas
    print(f"\n📊 REGISTROS A PROCESAR:")
    print(f"   • Cantidad: {len(df):,}")
    if not df.empty:
        print(f"   • Rango fechas: {df['CALLLATE'].min()} a {df['CALLLATE'].max()}")
        print(f"   • Llaves únicas: {df['LLAVE_UNICA'].nunique():,}")
    
    # 6. Hacer MERGE
    registros_procesados = merge_express_oracle(df, tiene_llave)
    
    # 7. LIMPIAR TABLAS TEMPORALES NUEVAMENTE (por si acaso)
    limpiar_tablas_temporales()
    
    # Tiempo total
    tiempo_total = time.time() - inicio_total
    minutos = int(tiempo_total // 60)
    segundos = int(tiempo_total % 60)
    
    print(f"\n{'='*60}")
    print(f"✅ ¡PROCESO COMPLETADO! 🚀")
    print(f"   Registros nuevos: {registros_procesados}")
    print(f"⏱️  Tiempo total: {minutos} minutos {segundos} segundos")
    print(f"{'='*60}")

# ============================================================================
# 🏃 EJECUTAR
# ============================================================================

if __name__ == "__main__":
    main()
