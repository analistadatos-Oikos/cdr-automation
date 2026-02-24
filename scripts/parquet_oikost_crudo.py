# ============================================================================
# SCRIPT: CDR_OIKOST_CRUDO -> PARQUET (PARA GITHUB ACTIONS)
# ============================================================================

import os
import pandas as pd
import tempfile
import time
from sqlalchemy import create_engine, text
from oci.object_storage import ObjectStorageClient
from tqdm import tqdm
import urllib3
import sys

urllib3.disable_warnings()

print("=" * 80)
print("🚀 INICIO DEL PROCESO: CDR_OIKOST_CRUDO -> PARQUET")
print("=" * 80)

# ============================================================================
# 📥 CONFIGURACIÓN CON VARIABLES DE ENTORNO (SECRETS)
# ============================================================================

# --- Credenciales Oracle (ya las tienes) ---
ORACLE_USER = os.environ.get('ORACLE_USER')
ORACLE_PASSWORD = os.environ.get('ORACLE_PASSWORD')
ORACLE_DSN = os.environ.get('ORACLE_DSN')

# --- Credenciales OCI (ya las tienes de los otros procesos) ---
OCI_USER_OCID = os.environ.get('OCI_USER_OCID')
OCI_TENANCY_OCID = os.environ.get('OCI_TENANCY_OCID')
OCI_KEY_FINGERPRINT = os.environ.get('OCI_KEY_FINGERPRINT')
OCI_PRIVATE_KEY = os.environ.get('OCI_PRIVATE_KEY')

# --- Configuración del bucket ---
BUCKET_NAME = "mis-parquets-oikos"
REGION = "us-ashburn-1"
NAMESPACE = "idazkw2d7ca6"

# --- Configuración de la tabla y archivo ---
TABLA_ORIGEN = "CDR_OIKOST_CRUDO"
ARCHIVO_PARQUET = "CDR_OIKOST_CRUDO.parquet"
NOMBRE_AMIGABLE = "CDR OIKOST Crudo"

# Verificar credenciales obligatorias
credenciales_faltantes = []
if not ORACLE_USER: credenciales_faltantes.append("ORACLE_USER")
if not ORACLE_PASSWORD: credenciales_faltantes.append("ORACLE_PASSWORD")
if not ORACLE_DSN: credenciales_faltantes.append("ORACLE_DSN")
if not OCI_USER_OCID: credenciales_faltantes.append("OCI_USER_OCID")
if not OCI_TENANCY_OCID: credenciales_faltantes.append("OCI_TENANCY_OCID")
if not OCI_KEY_FINGERPRINT: credenciales_faltantes.append("OCI_KEY_FINGERPRINT")
if not OCI_PRIVATE_KEY: credenciales_faltantes.append("OCI_PRIVATE_KEY")

if credenciales_faltantes:
    print("❌ FALTAN CREDENCIALES:")
    for cred in credenciales_faltantes:
        print(f"   - {cred}")
    sys.exit(1)

print("✅ Todas las credenciales encontradas.")

# ============================================================================
# 🔧 CONFIGURACIÓN OCI SDK
# ============================================================================

KEY_FILE_PATH = "/tmp/oci_key_oikost.pem"
OBJECT_STORAGE_CLIENT = None

try:
    # Escribir la clave privada en un archivo temporal
    with open(KEY_FILE_PATH, 'w') as f:
        f.write(OCI_PRIVATE_KEY.strip())

    OCI_CONFIG = {
        "user": OCI_USER_OCID,
        "fingerprint": OCI_KEY_FINGERPRINT,
        "key_file": KEY_FILE_PATH,
        "tenancy": OCI_TENANCY_OCID,
        "region": REGION
    }

    OBJECT_STORAGE_CLIENT = ObjectStorageClient(OCI_CONFIG)
    print("✅ Configuración OCI SDK exitosa.\n")

except Exception as e:
    print(f"❌ Error al configurar OCI SDK: {e}")
    # No salimos, pero no podremos subir a OCI

# ============================================================================
# 🧹 FUNCIONES AUXILIARES
# ============================================================================

def limpiar_versiones_antiguas(client, namespace, bucket_name, object_name):
    """Elimina TODAS las versiones anteriores de un objeto"""
    try:
        versions = client.list_object_versions(
            namespace_name=namespace,
            bucket_name=bucket_name,
            prefix=object_name
        ).data.items

        if not versions:
            return

        for version in versions:
            try:
                client.delete_object(
                    namespace_name=namespace,
                    bucket_name=bucket_name,
                    object_name=version.name,
                    version_id=version.version_id
                )
                print(f"   🗑️ Versión eliminada: {version.version_id}")
            except:
                pass
    except Exception as e:
        print(f"   ⚠️ Error limpiando versiones: {e}")

def upload_to_oci_force_overwrite(client, namespace, bucket_name, object_name, file_path, pbar):
    """Sube archivo a OCI con sobrescritura REAL"""
    if not client:
        return False

    try:
        print(f"\n   🧹 Limpiando versiones anteriores...")
        limpiar_versiones_antiguas(client, namespace, bucket_name, object_name)

        with open(file_path, 'rb') as f:
            client.put_object(
                namespace_name=namespace,
                bucket_name=bucket_name,
                object_name=object_name,
                put_object_body=f
            )

        return True

    except Exception as e:
        print(f"\n❌ Error al subir: {e}")
        return False

# ============================================================================
# 🎯 FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    inicio_total = time.time()

    # Verificar cliente OCI
    if not OBJECT_STORAGE_CLIENT:
        print("⚠️ No hay cliente OCI, solo se generará el archivo Parquet localmente.")
        subir = False
    else:
        subir = True

    print(f"\n{'='*60}")
    print(f"🎯 PROCESANDO: {NOMBRE_AMIGABLE}")
    print(f"📁 Archivo destino: {ARCHIVO_PARQUET}")
    print(f"{'='*60}\n")

    try:
        # 1. Conectar a Oracle
        print("🔌 Conectando a Oracle...")
        engine = create_engine(
            f"oracle+oracledb://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_DSN}",
            arraysize=5000,
            max_identifier_length=128
        )

        # 2. Contar registros
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM "{TABLA_ORIGEN}"'))
            total_registros = result.scalar()
            print(f"📊 Total en BD: {total_registros:,} registros")

        # 3. Leer TODOS los datos
        print(f"\n📚 Leyendo datos de {TABLA_ORIGEN}...")
        inicio_lectura = time.time()

        df = pd.read_sql(f'SELECT * FROM "{TABLA_ORIGEN}"', engine)

        tiempo_lectura = time.time() - inicio_lectura
        registros_leidos = len(df)

        print(f"✅ Leídos {registros_leidos:,} registros en {tiempo_lectura:.2f} segundos")

        if registros_leidos != total_registros:
            print(f"⚠️ ALERTA: Leídos {registros_leidos:,} vs {total_registros:,} en BD")
        else:
            print(f"✅ Integridad verificada: {registros_leidos:,} registros")

        if df.empty:
            print("❌ La tabla está vacía")
            return

        # 4. Mostrar muestra de datos
        print(f"\n🔍 Muestra de las primeras 3 filas:")
        print(df.head(3).to_string())

        # 5. Crear archivo temporal
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            ruta_temporal = tmp.name

        # 6. Guardar como Parquet
        print(f"\n💾 Guardando como Parquet...")
        inicio_parquet = time.time()

        df.to_parquet(
            ruta_temporal,
            index=False,
            engine='pyarrow',
            compression='snappy',
            row_group_size=100000
        )

        tiempo_parquet = time.time() - inicio_parquet
        tamaño_mb = os.path.getsize(ruta_temporal) / (1024 * 1024)

        print(f"✅ Archivo creado: {tamaño_mb:.2f} MB")
        print(f"⏱️  Tiempo de compresión: {tiempo_parquet:.2f} segundos")

        # 7. Subir a OCI (si hay cliente)
        if subir:
            print(f"\n☁️ Subiendo a OCI bucket '{BUCKET_NAME}'...")
            with tqdm(total=100, desc="Subiendo", unit="%", ncols=80) as pbar:
                pbar.update(10)
                resultado = upload_to_oci_force_overwrite(
                    client=OBJECT_STORAGE_CLIENT,
                    namespace=NAMESPACE,
                    bucket_name=BUCKET_NAME,
                    object_name=ARCHIVO_PARQUET,
                    file_path=ruta_temporal,
                    pbar=pbar
                )
                pbar.update(90)

            if resultado:
                print(f"\n✅ Archivo subido exitosamente!")
                print(f"   📁 {ARCHIVO_PARQUET}")
                print(f"   📦 {tamaño_mb:.2f} MB")
                print(f"   📊 {registros_leidos:,} registros")
            else:
                print(f"\n❌ Error al subir el archivo")
        else:
            print(f"\n✅ Archivo Parquet generado localmente: {ruta_temporal}")

        # 8. Limpiar archivo temporal
        if os.path.exists(ruta_temporal):
            os.remove(ruta_temporal)
            print(f"\n🧹 Archivo temporal eliminado.")

        # Tiempo total
        tiempo_total = time.time() - inicio_total
        minutos = int(tiempo_total // 60)
        segundos = int(tiempo_total % 60)

        print(f"\n{'='*60}")
        print(f"✅ PROCESO COMPLETADO EXITOSAMENTE")
        print(f"⏱️  Tiempo total: {minutos} minutos {segundos} segundos")
        print(f"📁 Archivo: {ARCHIVO_PARQUET}")
        print(f"{'='*60}")

    except Exception as e:
        print(f"\n❌ Error general: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        if os.path.exists(KEY_FILE_PATH):
            os.remove(KEY_FILE_PATH)
            print("🧹 Clave privada eliminada.")

# ============================================================================
# 🏃 EJECUTAR
# ============================================================================

if __name__ == "__main__":
    main()
