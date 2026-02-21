#!/usr/bin/env bash
set -euo pipefail

LOCAL_INPUT="${LOCAL_INPUT:-data/clickstream_sample.csv}"
HDFS_INPUT="${HDFS_INPUT:-/input/clickstream/clickstream.csv}"
HDFS_STAGE1_OUT="${HDFS_STAGE1_OUT:-/output/clickstream_sessions}"
HDFS_STAGE2_OUT="${HDFS_STAGE2_OUT:-/output/clickstream_user_metrics}"
SESSION_GAP_MINUTES="${SESSION_GAP_MINUTES:-30}"

HADOOP_BIN="${HADOOP_BIN:-}"
HDFS_BIN="${HDFS_BIN:-}"
PYTHON_BIN="${PYTHON_BIN:-}"

print_python_install_hint() {
  cat >&2 <<'EOF'
Instala Python dentro del contenedor Hadoop y vuelve a ejecutar el script.
Para imagenes Debian 9 (stretch) de bde2020:
  printf '%s\n' \
    'deb http://archive.debian.org/debian stretch main' \
    'deb http://archive.debian.org/debian-security stretch/updates main' \
    > /etc/apt/sources.list
  printf '%s\n' 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check-valid
  apt-get update -qq && apt-get install -y -qq python-minimal
EOF
}

find_first_executable() {
  local bin_name="$1"
  shift
  local candidate=""
  for candidate in "$@"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

if [[ -z "$HADOOP_BIN" ]]; then
  HADOOP_BIN="$(find_first_executable "hadoop" \
    "$(command -v hadoop 2>/dev/null || true)" \
    "/opt/hadoop/bin/hadoop" \
    /opt/hadoop-*/bin/hadoop \
    "/usr/local/hadoop/bin/hadoop" \
    /usr/local/hadoop-*/bin/hadoop \
    || true)"
  if [[ -z "$HADOOP_BIN" ]]; then
    HADOOP_BIN="$(find /opt /usr/local -type f -name hadoop -path '*/bin/hadoop' 2>/dev/null | head -n 1 || true)"
  fi
fi

if [[ -z "$HDFS_BIN" ]]; then
  HDFS_BIN="$(find_first_executable "hdfs" \
    "$(command -v hdfs 2>/dev/null || true)" \
    "/opt/hadoop/bin/hdfs" \
    /opt/hadoop-*/bin/hdfs \
    "/usr/local/hadoop/bin/hdfs" \
    /usr/local/hadoop-*/bin/hdfs \
    || true)"
  if [[ -z "$HDFS_BIN" ]]; then
    HDFS_BIN="$(find /opt /usr/local -type f -name hdfs -path '*/bin/hdfs' 2>/dev/null | head -n 1 || true)"
  fi
fi

if [[ -z "$HADOOP_BIN" || -z "$HDFS_BIN" ]]; then
  echo "No se encontraron binarios de Hadoop. Define HADOOP_BIN/HDFS_BIN o ejecuta en un nodo con Hadoop instalado." >&2
  echo "Debug rápido: ls -d /opt/*hadoop* /usr/local/*hadoop* 2>/dev/null" >&2
  exit 1
fi

if [[ -n "$PYTHON_BIN" && ! -x "$PYTHON_BIN" ]]; then
  echo "PYTHON_BIN apunta a un binario inexistente o no ejecutable: $PYTHON_BIN" >&2
  echo "Se intentara autodeteccion de Python..." >&2
  PYTHON_BIN=""
fi

if [[ -z "$PYTHON_BIN" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python)"
  elif command -v python2 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python2)"
  elif command -v python2.7 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python2.7)"
  elif [[ -x /usr/bin/python2.7 ]]; then
    PYTHON_BIN="/usr/bin/python2.7"
  elif [[ -x /usr/local/bin/python2.7 ]]; then
    PYTHON_BIN="/usr/local/bin/python2.7"
  fi
fi

if [[ -z "$PYTHON_BIN" ]]; then
  echo "No se encontro interprete Python en el nodo Hadoop (python3/python/python2)." >&2
  echo "Debug rapido: ls /usr/bin/python* /usr/local/bin/python* 2>/dev/null" >&2
  print_python_install_hint
  exit 1
fi

HADOOP_HOME="${HADOOP_HOME:-$(cd "$(dirname "$HADOOP_BIN")/.." && pwd)}"

STREAMING_JAR="${STREAMING_JAR:-}"
if [[ -z "$STREAMING_JAR" ]]; then
  STREAMING_JAR="$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -n 1 || true)"
fi

if [[ -z "$STREAMING_JAR" ]]; then
  STREAMING_JAR="$(find /opt /usr /usr/local -type f -name 'hadoop-streaming-*.jar' \
    ! -path '*/sources/*' \
    ! -name '*test*' \
    2>/dev/null | head -n 1 || true)"
fi

if [[ -n "$STREAMING_JAR" && ( "$STREAMING_JAR" == *"/sources/"* || "$STREAMING_JAR" == *"test"* ) ]]; then
  STREAMING_JAR=""
fi

if [[ -z "$STREAMING_JAR" ]]; then
  echo "No se encontró hadoop-streaming jar válido. Define STREAMING_JAR manualmente." >&2
  exit 1
fi

if [[ ! -f "$LOCAL_INPUT" ]]; then
  echo "No existe input local: $LOCAL_INPUT" >&2
  exit 1
fi

echo "Usando HADOOP_BIN: $HADOOP_BIN"
echo "Usando HDFS_BIN: $HDFS_BIN"
echo "Usando PYTHON_BIN: $PYTHON_BIN"
echo "Usando STREAMING_JAR: $STREAMING_JAR"

echo "[1/5] Preparando input HDFS"
"$HDFS_BIN" dfs -mkdir -p "$(dirname "$HDFS_INPUT")"
"$HDFS_BIN" dfs -put -f "$LOCAL_INPUT" "$HDFS_INPUT"

echo "[2/5] Limpiando outputs previos"
"$HDFS_BIN" dfs -rm -r -f "$HDFS_STAGE1_OUT" "$HDFS_STAGE2_OUT" >/dev/null 2>&1 || true

echo "[3/5] Job 1 - Sesionización"
"$HADOOP_BIN" jar "$STREAMING_JAR" \
  -D mapreduce.job.name="clickstream-sessionization" \
  -D mapreduce.job.reduces=1 \
  -D stream.num.map.output.key.fields=2 \
  -D mapreduce.partition.keypartitioner.options=-k1,1 \
  -D 'mapreduce.partition.keycomparator.options=-k1,1 -k2,2n' \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
  -file mapreduce/mapper_clickstream.py \
  -file mapreduce/reducer_sessionize.py \
  -mapper "$PYTHON_BIN mapper_clickstream.py" \
  -reducer "$PYTHON_BIN reducer_sessionize.py" \
  -cmdenv SESSION_GAP_MINUTES="$SESSION_GAP_MINUTES" \
  -input "$HDFS_INPUT" \
  -output "$HDFS_STAGE1_OUT"

echo "[4/5] Job 2 - Agregado por usuario"
"$HADOOP_BIN" jar "$STREAMING_JAR" \
  -D mapreduce.job.name="clickstream-user-aggregation" \
  -D mapreduce.job.reduces=1 \
  -file mapreduce/mapper_user_agg.py \
  -file mapreduce/reducer_user_agg.py \
  -mapper "$PYTHON_BIN mapper_user_agg.py" \
  -reducer "$PYTHON_BIN reducer_user_agg.py" \
  -input "$HDFS_STAGE1_OUT" \
  -output "$HDFS_STAGE2_OUT"

echo "[5/5] Resultados"
echo "Tabla sesiones: $HDFS_STAGE1_OUT"
echo "Tabla métricas por usuario: $HDFS_STAGE2_OUT"

echo
echo "Muestra sesiones:"
"$HDFS_BIN" dfs -cat "$HDFS_STAGE1_OUT"/part-* | head -n 20

echo
echo "Muestra métricas por usuario:"
"$HDFS_BIN" dfs -cat "$HDFS_STAGE2_OUT"/part-* | head -n 20
