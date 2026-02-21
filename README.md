# Hadoop MapReduce - Sesionización de Clickstream

Implementación en Python sobre Hadoop Streaming para:
- ordenar eventos por `user_id` + `timestamp`
- detectar cortes de sesión por inactividad
- calcular métricas por sesión
- detectar patrones atípicos básicos
- construir tabla agregada por usuario

## Estructura

- `mapreduce/mapper_clickstream.py`: normaliza CSV y emite clave compuesta para orden cronológico por usuario.
- `mapreduce/reducer_sessionize.py`: sesioniza y genera tabla por sesión.
- `mapreduce/mapper_user_agg.py`: transforma sesiones en registros agregables por usuario.
- `mapreduce/reducer_user_agg.py`: calcula métricas finales por usuario.
- `data/clickstream_sample.csv`: dataset de ejemplo.
- `run_hadoop_streaming.sh`: ejecuta los 2 jobs en Hadoop (HDFS + YARN).
- `run_local_smoke.sh`: prueba local sin Hadoop (simulando shuffle/sort con `sort`).

## Formato de entrada (CSV)

Cabeceras mínimas requeridas:
- `user_id`
- `event_time`
- `page`

Cabeceras opcionales:
- `event_type`
- `device`

Ejemplo:

```csv
event_id,user_id,event_time,page,event_type,device
1,u1,2026-02-20T10:00:00Z,/home,view,mobile
```

## Salida 1: tabla por sesión

`user_id, session_id, session_start, session_end, duration_seconds, pageviews, unique_pages, entry_page, exit_page, path_sequence, anomaly_flags`

Reglas de patrones atípicos (`anomaly_flags`):
- `bounce`: 1 página y duración <= 10s
- `single_page`: 1 página con duración > 10s
- `long_session`: duración >= 7200s
- `rapid_clicking`: mínimo gap entre eventos <= 2s y al menos 5 eventos
- `high_depth`: al menos 25 páginas
- `normal`: si no aplica ninguna regla

## Salida 2: tabla agregada por usuario

`user_id, total_sessions, total_duration_seconds, total_pageviews, avg_session_duration_seconds, avg_pages_per_session, anomalous_sessions, anomalous_rate`

## Ejecución local (smoke test)

```bash
./run_local_smoke.sh
```

Archivos generados:
- `output/sessions.tsv`
- `output/user_metrics.tsv`

## Ejecución en Hadoop (docker-compose de este repo)

1. Levantar cluster:

```bash
docker compose up -d
```

2. Entrar al cliente Hadoop:

```bash
docker exec -it hadoop-client bash
```

3. Desde el contenedor, ubicarte en el proyecto montado y ejecutar:

```bash
cd /workspace
./run_hadoop_streaming.sh
```

Si el proyecto se monta en otra ruta, ajusta `cd` en consecuencia.

## Variables configurables

- `SESSION_GAP_MINUTES` (default: `30`)
- `LOCAL_INPUT` (default: `data/clickstream_sample.csv`)
- `HDFS_INPUT` (default: `/input/clickstream/clickstream.csv`)
- `HDFS_STAGE1_OUT` (default: `/output/clickstream_sessions`)
- `HDFS_STAGE2_OUT` (default: `/output/clickstream_user_metrics`)
- `STREAMING_JAR` (si no se autodetecta)

Ejemplo:

```bash
SESSION_GAP_MINUTES=20 ./run_local_smoke.sh
```
