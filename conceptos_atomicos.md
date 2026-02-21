# Conceptos atómicos implementados en esta actividad

Este documento resume los conceptos base usados en el proyecto de sesionización de clickstream con Hadoop Streaming.

## 1) Docker

Docker es una plataforma para empaquetar y ejecutar software en entornos aislados llamados contenedores.

En esta tarea se usa para levantar un clúster Hadoop reproducible sin instalar Hadoop directamente en el sistema host.

Comando típico:

```bash
docker compose up -d
```

## 2) Imagen Docker

Una imagen es una plantilla inmutable con sistema base, librerías y binarios.

En el `docker-compose.yml` se usan imágenes de `bde2020/*` para cada rol de Hadoop, por ejemplo:

- `bde2020/hadoop-namenode`
- `bde2020/hadoop-datanode`
- `bde2020/hadoop-resourcemanager`

## 3) Contenedor

Un contenedor es una instancia en ejecución de una imagen.

En este proyecto, cada contenedor cumple un rol específico:

- `hadoop-namenode`
- `hadoop-datanode`
- `hadoop-resourcemanager`
- `hadoop-nodemanager`
- `hadoop-historyserver`
- `hadoop-client`

## 4) Docker Compose

Docker Compose define y orquesta múltiples contenedores como un solo sistema.

En esta tarea:

- Define servicios, puertos, variables de entorno y volúmenes.
- Arranca todo el clúster Hadoop en conjunto.

Archivo clave: `docker-compose.yml`.

## 5) Volúmenes

Un volumen preserva datos fuera del ciclo de vida del contenedor.

Aquí se usan para persistir metadatos y datos de HDFS:

- `namenode:/hadoop/dfs/name`
- `datanode:/hadoop/dfs/data`
- `historyserver:/hadoop/yarn/timeline`

## 6) HDFS

HDFS (Hadoop Distributed File System) es el sistema de archivos distribuido de Hadoop.

Roles:

- `NameNode`: administra metadatos.
- `DataNode`: almacena bloques de datos.

En la tarea:

- Se sube `data/clickstream_sample.csv` a HDFS.
- Los outputs de cada job se escriben en rutas HDFS (`/output/...`).

Comandos típicos desde scripts:

```bash
hdfs dfs -mkdir -p /input/clickstream
hdfs dfs -put -f data/clickstream_sample.csv /input/clickstream/clickstream.csv
hdfs dfs -cat /output/clickstream_user_metrics/part-*
```

## 7) YARN

YARN (Yet Another Resource Negotiator) gestiona recursos y ejecución de trabajos en Hadoop.

Roles:

- `ResourceManager`: decide dónde corre cada tarea.
- `NodeManager`: ejecuta contenedores de cómputo en los nodos.

En esta tarea, los jobs de MapReduce Streaming se envían a YARN mediante `hadoop jar ...`.

## 8) MapReduce

MapReduce es un modelo de procesamiento distribuido con dos fases:

- **Map**: transforma datos de entrada en pares clave/valor.
- **Reduce**: agrega o resume por clave.

Flujo usado en el proyecto:

1. Job 1 (sesionización):
   - Mapper: `mapreduce/mapper_clickstream.py`
   - Reducer: `mapreduce/reducer_sessionize.py`
2. Job 2 (agregación por usuario):
   - Mapper: `mapreduce/mapper_user_agg.py`
   - Reducer: `mapreduce/reducer_user_agg.py`

## 9) Hadoop Streaming

Hadoop Streaming permite escribir mappers y reducers en lenguajes como Python (no solo Java).

Se usa con el jar `hadoop-streaming-*.jar`, por ejemplo:

```bash
hadoop jar <streaming.jar> \
  -mapper "python mapper_clickstream.py" \
  -reducer "python reducer_sessionize.py"
```

## 10) Shuffle/Sort

Entre map y reduce, Hadoop:

- Agrupa por clave (shuffle).
- Ordena por clave (sort).

En esta tarea se usa clave compuesta para ordenar por usuario y timestamp, habilitando sesionización cronológica.

## 11) Scripts `.sh` (Bash)

Un archivo `.sh` es un script de shell que automatiza comandos.

Scripts principales:

- `run_hadoop_streaming.sh`: ejecuta el pipeline completo en Hadoop/HDFS/YARN.
- `run_local_smoke.sh`: simula el pipeline localmente con `sort` y genera salidas en `output/`.

Línea importante:

```bash
set -euo pipefail
```

Esto hace que el script falle rápido ante errores, variables no definidas o fallos en pipelines.

## 12) TSV vs CSV en salidas

- Entrada principal: CSV con encabezados (`data/clickstream_sample.csv`).
- Salidas de reducers: TSV sin encabezados (`output/sessions.tsv`, `output/user_metrics.tsv`).

Esto es normal en Hadoop Streaming; si se desea, se pueden agregar headers en un paso posterior.

## 13) Variables de entorno

Permiten parametrizar ejecución sin editar código.

Variables usadas:

- `SESSION_GAP_MINUTES`
- `LOCAL_INPUT`
- `HDFS_INPUT`
- `HDFS_STAGE1_OUT`
- `HDFS_STAGE2_OUT`
- `PYTHON_BIN`
- `STREAMING_JAR`

Ejemplo:

```bash
SESSION_GAP_MINUTES=20 ./run_local_smoke.sh
```

## 14) Resultado conceptual del pipeline

1. Ingesta de eventos clickstream.
2. Orden temporal por usuario.
3. Corte en sesiones por inactividad.
4. Cálculo de métricas por sesión.
5. Agregación final por usuario.

Con esto se obtiene trazabilidad completa desde eventos crudos hasta KPIs por usuario.
