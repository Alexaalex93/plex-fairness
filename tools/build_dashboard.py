# Builds the Grafana dashboards from one place:
#   grafana_dashboard.json                  -> for manual import (asks for the datasource)
#   grafana/dashboards/plex-fairness.json   -> baked into the Grafana image (fixed datasource uid)
# Run from the repo root: python3 tools/build_dashboard.py      (add "sql" to dump the raw queries)
import json, sys

TZ = "Europe/Madrid"
DS = {"type": "grafana-postgresql-datasource", "uid": "${DS_PLEXFAIR}"}

Q = {
"bw_user": """SELECT $__timeGroupAlias(ts, '1m'), user_name AS metric, avg(bandwidth_kbps)/1000.0 AS value
FROM session_sample WHERE $__timeFilter(ts) AND state = 'playing'
GROUP BY 1, 2 ORDER BY 1""",
"jain": """SELECT $__timeGroupAlias(ts, '1m'), avg(jain_quality) AS "Equidad de calidad"
FROM poll WHERE $__timeFilter(ts) GROUP BY 1 ORDER BY 1""",
"quality_user": """SELECT $__timeGroupAlias(ts, '1m'), user_name AS metric, avg(quality_ratio) AS value
FROM session_sample WHERE $__timeFilter(ts) AND state = 'playing'
GROUP BY 1, 2 ORDER BY 1""",
"speed_user": """SELECT $__timeGroupAlias(ts, '1m'), user_name AS metric, min(transcode_speed) AS value
FROM session_sample WHERE $__timeFilter(ts) AND state = 'playing' AND transcode_speed > 0 AND NOT coalesce(throttled, false)
GROUP BY 1, 2 ORDER BY 1""",
"gpu": """SELECT $__timeGroupAlias(ts, '1m'), avg(gpu_enc_util) AS "NVENC %", avg(gpu_dec_util) AS "NVDEC %",
  avg(gpu_util) AS "GPU %", max(transcodes) AS "Transcodes", max(playing) AS "Reproduciendo"
FROM poll WHERE $__timeFilter(ts) GROUP BY 1 ORDER BY 1""",
"disk_read": """SELECT $__timeGroupAlias(ts, '1m'), disk AS metric, avg(read_mbps) AS value
FROM disk_sample WHERE $__timeFilter(ts) GROUP BY 1, 2 HAVING max(read_mbps) > 1 ORDER BY 1""",
"disk_streams": """SELECT $__timeGroupAlias(ts, '1m'), coalesce(disk, '?') AS metric, max(n) AS value FROM (
  SELECT ts, disk, count(*) AS n FROM session_sample
  WHERE $__timeFilter(ts) AND state = 'playing' GROUP BY ts, disk) x
GROUP BY 1, 2 ORDER BY 1""",
"summary": """SELECT s.user_name AS "Usuario",
  round(count(*) * $poll / 3600.0, 1) AS "Horas",
  round(100.0 * avg(CASE WHEN s.video_decision = 'transcode' THEN 1 ELSE 0 END), 1) AS "% transcode",
  round(avg(s.quality_ratio)::numeric, 2) AS "Calidad media",
  round(100.0 * avg(CASE WHEN s.transcode_speed > 0 AND s.transcode_speed < 1 AND NOT coalesce(s.throttled, false) THEN 1 ELSE 0 END), 1) AS "% transcode lento",
  round(avg(s.bandwidth_kbps) / 1000.0, 1) AS "Mbps medio",
  (SELECT count(*) FROM buffer_event b WHERE b.user_name = s.user_name AND $__timeFilter(b.ts)) AS "Buffers"
FROM session_sample s WHERE $__timeFilter(s.ts) AND s.state = 'playing'
GROUP BY s.user_name ORDER BY 2 DESC""",
"concurrency": f"""SELECT lpad(h::text, 2, '0') || 'h' AS "Hora",
  round(avg(playing) FILTER (WHERE d = 1), 2) AS "Lun", round(avg(playing) FILTER (WHERE d = 2), 2) AS "Mar",
  round(avg(playing) FILTER (WHERE d = 3), 2) AS "Mié", round(avg(playing) FILTER (WHERE d = 4), 2) AS "Jue",
  round(avg(playing) FILTER (WHERE d = 5), 2) AS "Vie", round(avg(playing) FILTER (WHERE d = 6), 2) AS "Sáb",
  round(avg(playing) FILTER (WHERE d = 7), 2) AS "Dom", max(playing) AS "Pico"
FROM (SELECT extract(isodow FROM ts AT TIME ZONE '{TZ}')::int AS d, extract(hour FROM ts AT TIME ZONE '{TZ}')::int AS h, playing
      FROM poll WHERE $__timeFilter(ts)) x
GROUP BY h ORDER BY h""",
"unfair": """SELECT p.ts AS "Momento", p.playing AS "Reproduciendo", round(100 * p.jain_quality)::int AS "Equidad %",
  round(p.gpu_enc_util::numeric) AS "NVENC %",
  string_agg(s.user_name || ' ' || round(100 * coalesce(s.quality_ratio, 0))::text || '% '
             || coalesce(s.video_decision, '')
             || CASE WHEN s.transcode_speed > 0 AND NOT coalesce(s.throttled, false) THEN ' x' || s.transcode_speed::text
                     WHEN s.video_decision = 'transcode' THEN ' (en pausa, va adelantado)' ELSE '' END, ' | ' ORDER BY s.quality_ratio) AS "Detalle"
FROM poll p JOIN session_sample s ON s.ts = p.ts AND s.state = 'playing'
WHERE $__timeFilter(p.ts) AND p.jain_quality < $jain_threshold
GROUP BY p.ts, p.playing, p.jain_quality, p.gpu_enc_util ORDER BY p.ts DESC LIMIT 300""",
"buffers": """SELECT ts AS "Momento", user_name AS "Usuario", title AS "Título" FROM buffer_event
WHERE $__timeFilter(ts) ORDER BY ts DESC LIMIT 200""",
}

# ---------- capacity & bottlenecks ----------
Q.update({
"now_bottleneck": """SELECT ts AS time,
  CASE WHEN bottleneck_pct >= 50 THEN bottleneck ELSE 'Nada cerca del límite' END AS metric, bottleneck_pct AS value
FROM poll WHERE ts = (SELECT max(ts) FROM poll)""",
"gauge_now": """SELECT ts AS time, resource AS metric, pct AS value FROM resource_sample
WHERE ts = (SELECT max(ts) FROM resource_sample) ORDER BY pct DESC""",
"gauge_p95": """SELECT max(ts) AS time, resource AS metric, percentile_cont(0.95) WITHIN GROUP (ORDER BY pct) AS value
FROM resource_sample WHERE $__timeFilter(ts) GROUP BY resource ORDER BY 3 DESC""",
"res_ts": """SELECT $__timeGroupAlias(ts, '1m'), resource AS metric, max(pct) AS value
FROM resource_sample WHERE $__timeFilter(ts) GROUP BY 1, 2 ORDER BY 1""",
"episodes": """WITH hot AS (
  SELECT ts, bottleneck, bottleneck_pct, playing, transcodes,
    CASE WHEN lag(ts) OVER (PARTITION BY bottleneck ORDER BY ts) >= ts - make_interval(secs => 2.5 * $poll) THEN 0 ELSE 1 END AS brk
  FROM poll WHERE $__timeFilter(ts) AND bottleneck_pct >= $crit
), g AS (
  SELECT *, sum(brk) OVER (PARTITION BY bottleneck ORDER BY ts) AS grp FROM hot
)
SELECT min(g.ts) AS "Inicio",
  round(extract(epoch FROM max(g.ts) - min(g.ts)) / 60 + $poll / 60.0, 1) AS "Minutos",
  g.bottleneck AS "Cuello de botella",
  round(max(g.bottleneck_pct)::numeric) AS "Pico %",
  max(g.playing) AS "Streams", max(g.transcodes) AS "Transcodes",
  (SELECT string_agg(DISTINCT s.user_name || ' (' || coalesce(s.video_decision, '?') || ')', ', ')
     FROM session_sample s WHERE s.ts BETWEEN min(g.ts) AND max(g.ts) AND s.state = 'playing') AS "Quién estaba viendo",
  (SELECT c.name || ' ' || round(avg(c.cpu_pct)::numeric) || '% CPU'
     FROM container_sample c WHERE c.ts BETWEEN min(g.ts) AND max(g.ts)
     GROUP BY c.name HAVING avg(c.cpu_pct) >= 1 ORDER BY avg(c.cpu_pct) DESC LIMIT 1) AS "Contenedor que más CPU usaba",
  (SELECT string_agg(r.resource || ' ' || round(r.pct::numeric) || '%', ', ' ORDER BY r.pct DESC)
     FROM resource_sample r WHERE r.ts = max(g.ts) AND r.pct >= 50 AND r.resource <> g.bottleneck) AS "Otros recursos >50%"
FROM g GROUP BY g.bottleneck, g.grp ORDER BY 1 DESC LIMIT 200""",
"headroom": """WITH r AS (
  SELECT rs.resource, rs.pct, rs.capacity, rs.unit,
    CASE WHEN rs.resource LIKE 'GPU%' THEN p.transcodes ELSE p.playing END AS n
  FROM resource_sample rs JOIN poll p ON p.ts = rs.ts
  WHERE $__timeFilter(rs.ts) AND rs.resource <> 'GPU temperatura'
), s AS (
  SELECT resource, max(capacity) AS cap, max(unit) AS unit, max(pct) AS peak,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY pct) AS p95,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY pct) FILTER (WHERE n = 0) AS idle,
    avg(pct) FILTER (WHERE n > 0) AS busy,
    avg(n) FILTER (WHERE n > 0) AS avg_n
  FROM r GROUP BY resource
), k AS (
  SELECT *, (busy - coalesce(idle, 0)) / nullif(avg_n, 0) AS per_stream FROM s
)
SELECT resource AS "Recurso",
  trim_scale(round(cap::numeric, 1)) || ' ' || unit AS "Capacidad",
  round(p95::numeric, 1) AS "p95 %",
  round(peak::numeric, 1) AS "Pico %",
  round(per_stream::numeric, 1) AS "% por stream",
  CASE WHEN avg_n IS NULL THEN 'faltan datos con streams'
       WHEN per_stream > 0.5 THEN greatest(floor(($crit - p95) / per_stream), 0)::int::text
       ELSE 'sin límite a la vista' END AS "Streams extra que caben"
FROM k ORDER BY p95 DESC""",
"specs": """SELECT component AS "Componente", name AS "Nombre", detail AS "Detalle",
  trim_scale(round(capacity::numeric, 2)) || ' ' || unit AS "Capacidad"
FROM system_spec ORDER BY 1, 2""",
"cont_cpu": """WITH top AS (SELECT name FROM container_sample WHERE $__timeFilter(ts) GROUP BY name ORDER BY avg(cpu_pct) DESC LIMIT 8)
SELECT $__timeGroupAlias(ts, '1m'), name AS metric, avg(cpu_pct) AS value FROM container_sample
WHERE $__timeFilter(ts) AND name IN (SELECT name FROM top) GROUP BY 1, 2 ORDER BY 1""",
"cont_mem": """WITH top AS (SELECT name FROM container_sample WHERE $__timeFilter(ts) GROUP BY name ORDER BY avg(mem_mb) DESC LIMIT 8)
SELECT $__timeGroupAlias(ts, '1m'), name AS metric, avg(mem_mb) * 1024 * 1024 AS value FROM container_sample
WHERE $__timeFilter(ts) AND name IN (SELECT name FROM top) GROUP BY 1, 2 ORDER BY 1""",
})

# ---------- summary dashboard ("Plex - Resumen") ----------
# Same headroom logic as the detail dashboard, but split by stream type:
# GPU resources are consumed by transcodes, everything else by any stream (direct play).
HEADROOM_CTE = """WITH r AS (
  SELECT rs.resource, rs.pct, rs.used, rs.capacity, rs.unit,
    CASE WHEN rs.resource LIKE 'GPU%' OR rs.resource LIKE 'RAM transcode%' THEN p.transcodes ELSE p.playing END AS n
  FROM resource_sample rs JOIN poll p ON p.ts = rs.ts
  WHERE $__timeFilter(rs.ts) AND rs.resource NOT IN ('GPU temperatura', 'CPU núcleo más cargado', 'GPU')
), s AS (
  SELECT resource, max(capacity) AS cap, max(unit) AS unit, max(used) AS max_used,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY pct) AS p95,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY pct) FILTER (WHERE n = 0) AS idle,
    avg(pct) FILTER (WHERE n > 0) AS busy,
    avg(n) FILTER (WHERE n > 0) AS avg_n
  FROM r GROUP BY resource
), k AS (
  SELECT *,
    CASE WHEN resource LIKE 'GPU%' OR resource LIKE 'RAM transcode%' THEN 'Transcode' ELSE 'Direct play' END AS kind,
    (busy - coalesce(idle, 0)) / nullif(avg_n, 0) AS per_stream
  FROM s
), e AS (
  SELECT *,
    CASE
      -- the NVIDIA consumer driver caps simultaneous encodes, that's a hard limit, not a %
      WHEN resource = 'GPU sesiones NVENC' THEN greatest(cap - max_used, 0)::int
      -- RAM goes up and down mostly because of other containers (torrents...), so the per-stream
      -- estimate is just noise. Plex uses very little RAM per direct play, I don't count it as a limit.
      WHEN resource = 'RAM' THEN NULL
      WHEN avg_n IS NULL THEN NULL
      WHEN per_stream > 0.5 THEN greatest(floor(($crit - p95) / per_stream), 0)::int
      ELSE NULL END AS extra
  FROM k
)"""

Q.update({
"sum_now": """SELECT ts AS time, playing AS "Viendo ahora" FROM poll ORDER BY ts DESC LIMIT 1""",
"sum_peak": """SELECT ts AS time, 'el ' || to_char(ts AT TIME ZONE 'Europe/Madrid', 'DD/MM HH24:MI') AS metric, playing AS value
FROM poll WHERE $__timeFilter(ts) ORDER BY playing DESC, ts DESC LIMIT 1""",
"sum_avg": """SELECT round(avg(playing) FILTER (WHERE playing > 0), 1) AS "Media con uso" FROM poll WHERE $__timeFilter(ts)""",
"sum_hours": """SELECT round(count(*) * $poll / 3600.0) AS "Horas vistas" FROM session_sample WHERE $__timeFilter(ts) AND state = 'playing'""",
"sum_users": """SELECT count(DISTINCT user_name) AS "Usuarios activos" FROM session_sample WHERE $__timeFilter(ts) AND state = 'playing'""",
"sum_peak_trans": """SELECT ts AS time, CASE WHEN transcodes > 0 THEN 'el ' || to_char(ts AT TIME ZONE 'Europe/Madrid', 'DD/MM HH24:MI') ELSE 'ninguno' END AS metric, transcodes AS value
FROM poll WHERE $__timeFilter(ts) ORDER BY transcodes DESC, ts DESC LIMIT 1""",
"sum_resources": """WITH r AS (
  SELECT rs.resource, rs.ts, rs.pct, rs.used, rs.capacity, rs.unit, p.playing, p.transcodes
  FROM resource_sample rs JOIN poll p ON p.ts = rs.ts
  WHERE $__timeFilter(rs.ts) AND rs.resource NOT IN ('CPU núcleo más cargado', 'GPU')
), s AS (
  SELECT resource, max(capacity) AS cap, max(unit) AS unit,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY pct) AS p95,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY used) AS p95_used,
    max(pct) AS peak, max(used) AS peak_used
  FROM r GROUP BY resource
), pk AS (
  -- the moment of the peak, to explain what caused it
  SELECT DISTINCT ON (resource) resource, ts, playing, transcodes FROM r ORDER BY resource, pct DESC, ts DESC
)
SELECT s.resource AS "Recurso",
  trim_scale(round(s.cap::numeric, 1)) || ' ' || s.unit AS "Capacidad",
  CASE WHEN s.unit = '°C' THEN round(s.p95_used::numeric) || ' °C' ELSE round(s.p95::numeric, 1) || ' %' END AS "p95",
  CASE WHEN s.unit = '°C' THEN round(s.peak_used::numeric) || ' °C' ELSE round(s.peak::numeric, 1) || ' %' END AS "Pico",
  pk.ts AS "Cuándo",
  CASE WHEN pk.playing > 0
       THEN 'con ' || pk.playing || ' stream' || CASE WHEN pk.playing > 1 THEN 's' ELSE '' END
            || CASE WHEN pk.transcodes > 0 THEN ' (' || pk.transcodes || ' transcode)' ELSE '' END
       ELSE 'tarea de fondo' END
  || coalesce(' · ' || (SELECT c.name FROM container_sample c
                         WHERE c.ts BETWEEN pk.ts - interval '1 minute' AND pk.ts + interval '1 minute'
                         GROUP BY c.name HAVING avg(c.cpu_pct) >= 1 ORDER BY avg(c.cpu_pct) DESC LIMIT 1), '') AS "Causa del pico",
  CASE WHEN s.p95 >= 85 THEN 'Al límite' WHEN s.p95 >= 70 THEN 'Vigilar' WHEN s.p95 >= 50 THEN 'Holgado' ELSE 'Sobrado' END AS "Estado"
FROM s JOIN pk USING (resource)
ORDER BY CASE
  WHEN s.resource = 'CPU' THEN 1 WHEN s.resource = 'RAM' THEN 2 WHEN s.resource LIKE 'RAM transcode%' THEN 3
  WHEN s.resource LIKE 'GPU%' THEN 4 WHEN s.resource LIKE 'Red%' THEN 5 WHEN s.resource LIKE 'Internet%' THEN 6 ELSE 7 END,
  s.resource""",
"sum_extra_dp": HEADROOM_CTE + """
SELECT now() AS time, 'límite: ' || resource AS metric, extra AS value FROM e
WHERE kind = 'Direct play' AND extra IS NOT NULL ORDER BY extra LIMIT 1""",
"sum_extra_tc": HEADROOM_CTE + """
SELECT now() AS time, 'límite: ' || resource AS metric, extra AS value FROM e
WHERE kind = 'Transcode' AND extra IS NOT NULL ORDER BY extra LIMIT 1""",
"sum_headroom": HEADROOM_CTE + """
SELECT kind AS "Tipo de stream", resource AS "Recurso",
  CASE WHEN resource = 'GPU sesiones NVENC' THEN '1 sesión'
       WHEN per_stream IS NULL THEN '—' ELSE round(per_stream::numeric, 1) || ' %' END AS "Consumo por stream",
  CASE WHEN resource = 'GPU sesiones NVENC' THEN round(max_used) || ' de ' || round(cap) || ' en uso (pico)'
       ELSE round(p95::numeric, 1) || ' %' END AS "Uso p95",
  coalesce(extra::text, CASE WHEN avg_n IS NULL THEN 'faltan datos' ELSE 'sin límite a la vista' END) AS "Streams extra que caben",
  CASE WHEN resource = 'GPU sesiones NVENC' THEN 'Límite fijo del driver NVIDIA: transcodes a la vez'
       WHEN resource LIKE 'Disco%' THEN 'Por disco. Llega antes si varios 4K pesados leen del mismo disco'
       WHEN resource LIKE 'Internet%' THEN 'Tu subida; la bajada de cada usuario puede limitar antes'
       WHEN resource = 'RAM' THEN 'No se estima: la RAM la mueven sobre todo otros contenedores. Plex gasta muy poca por stream; vigila el p95'
       ELSE '' END AS "Nota"
FROM e ORDER BY kind, extra NULLS LAST, resource""",
})

if len(sys.argv) > 1 and sys.argv[1] == "sql":
    print(json.dumps(Q)); sys.exit()

_id = 0
def panel(title, key, kind, x, y, w, h, fmt="time_series", unit=None, desc="", extra=None, field_extra=None):
    global _id; _id += 1
    p = {"id": _id, "type": kind, "title": title, "description": desc, "datasource": DS,
         "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": [{"refId": "A", "datasource": DS, "editorMode": "code", "format": fmt, "rawQuery": True, "rawSql": Q[key]}],
         "fieldConfig": {"defaults": {}, "overrides": []}, "options": {}}
    if unit: p["fieldConfig"]["defaults"]["unit"] = unit
    if field_extra: p["fieldConfig"]["defaults"].update(field_extra)
    if kind == "timeseries":
        p["fieldConfig"]["defaults"]["custom"] = {"lineWidth": 1, "fillOpacity": 10, "spanNulls": 120000, "showPoints": "never"}
        p["options"] = {"legend": {"displayMode": "table", "placement": "right", "calcs": ["mean", "max"]}, "tooltip": {"mode": "multi", "sort": "desc"}}
    if extra: p["options"].update(extra)
    return p

def thresholds(*steps):
    return {"thresholds": {"mode": "absolute", "steps": [{"color": c, "value": v} for v, c in steps]}}

panels = [
  panel("Equidad entre usuarios: ¿reciben todos la misma calidad?", "jain", "timeseries", 0, 0, 24, 8, unit="percentunit",
        desc="100 % = todos los que están viendo algo a la vez reciben la misma proporción de su calidad original. Cuanto más baja, más sale perdiendo alguien frente a los demás. Solo se calcula cuando hay 2 o más personas reproduciendo.",
        field_extra={"min": 0, "max": 1, **thresholds((None, "red"), (0.7, "orange"), (0.9, "green"))}),
  panel("Ancho de banda por usuario", "bw_user", "timeseries", 0, 8, 12, 9, unit="Mbits",
        desc="Mbps que recibe cada usuario mientras reproduce."),
  panel("Calidad servida / original por usuario", "quality_user", "timeseries", 12, 8, 12, 9, unit="percentunit",
        desc="100 % = le llega la calidad original (direct play). Si transcodifica, la proporción de bitrate que le llega.",
        field_extra={"min": 0, "max": 1}),
  panel("Velocidad de transcode (<1 = cortes)", "speed_user", "timeseries", 0, 17, 12, 9, unit="x",
        desc="Mínimo por minuto. Por debajo de 1x Plex no llega a tiempo y el cliente se queda cargando. No incluye los momentos en que Plex pausa el transcode porque ya va adelantado.",
        field_extra={"min": 0, **thresholds((None, "red"), (1, "green"))}),
  panel("GPU (RTX 2060) vs transcodes", "gpu", "timeseries", 12, 17, 12, 9,
        desc="Uso del codificador (NVENC) y decodificador (NVDEC) de la GPU junto al número de transcodes."),
  panel("Lectura por disco del array", "disk_read", "timeseries", 0, 26, 12, 9, unit="MBs",
        desc="Si dos streams leen del mismo disco compiten entre sí: el array de Unraid no reparte los datos entre discos."),
  panel("Streams simultáneos por disco", "disk_streams", "timeseries", 12, 26, 12, 9,
        desc="Disco donde está el fichero de cada stream (atributo system.LOCATION de Unraid)."),
  panel("Resumen por usuario", "summary", "table", 0, 35, 24, 8, fmt="table",
        desc="Por usuario en el periodo: horas vistas, % del tiempo con transcode, calidad media recibida, % del tiempo con transcode lento (<1x), Mbps medio y avisos de buffer."),
  panel("Momentos en que alguien salió perdiendo", "unfair", "table", 0, 43, 24, 10, fmt="table",
        desc="Muestreos en los que la equidad bajó del umbral. El detalle muestra, por usuario: % de calidad recibida, si hacía direct play o transcode, y velocidad del transcode."),
  panel("Concurrencia media por día y hora", "concurrency", "table", 0, 53, 14, 14, fmt="table",
        desc="Media de personas reproduciendo a la vez en cada franja. La columna Pico es el máximo.",
        field_extra={"custom": {"cellOptions": {"type": "color-background", "mode": "gradient"}},
                     **thresholds((None, "transparent"), (1, "green"), (2, "orange"), (3, "red"))}),
  panel("Eventos de buffer", "buffers", "table", 14, 53, 10, 14, fmt="table",
        desc="Avisos de buffer que manda Tautulli por webhook (si lo tienes configurado)."),
]


PCT = {"unit": "percent", "min": 0, "max": 100,
       **thresholds((None, "green"), (70, "yellow"), (85, "red"))}

def row(title, y):
    global _id; _id += 1
    return {"id": _id, "type": "row", "title": title, "collapsed": False, "panels": [], "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}}

capacity = [
  row("Capacidad y cuellos de botella", 0),
  panel("Cuello de botella ahora", "now_bottleneck", "stat", 0, 1, 6, 8,
        desc="El recurso más cerca de su límite en el último muestreo (sin contar temperatura ni el núcleo más cargado). Si ninguno pasa del 50 % muestra 'Nada cerca del límite'.",
        extra={"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
               "textMode": "value_and_name", "colorMode": "background", "graphMode": "none"},
        field_extra=PCT),
  panel("Ocupación ahora", "gauge_now", "bargauge", 6, 1, 9, 8,
        extra={"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True, "valueMode": "color",
               "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
        field_extra=PCT),
  panel("Ocupación p95 en el periodo", "gauge_p95", "bargauge", 15, 1, 9, 8,
        desc="El 95 % del tiempo la ocupación estuvo por debajo de este valor. Es mejor indicador que el pico para planificar.",
        extra={"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True, "valueMode": "color",
               "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
        field_extra=PCT),
  panel("Ocupación por recurso", "res_ts", "timeseries", 0, 9, 24, 10,
        desc="% de su capacidad. Amarillo a partir de 70 %, rojo a partir de 85 %.",
        field_extra={**PCT, "custom": {"thresholdsStyle": {"mode": "line+area"}}}),
  panel("Episodios de cuello de botella (ocupación >= umbral)", "episodes", "table", 0, 19, 24, 9, fmt="table",
        desc="Cada fila es un tramo seguido en el que un recurso estuvo por encima del umbral: qué recurso, cuánto duró, quién estaba viendo y qué más estaba cargado."),
  panel("Margen: cuántos streams más caben", "headroom", "table", 0, 28, 14, 10, fmt="table",
        desc="Estimación: consumo medio con streams menos consumo en reposo, dividido entre streams (para la GPU, entre transcodes). 'Streams extra' = cuántos más caben antes de llegar al umbral partiendo del p95. El que tenga el número más bajo será el primer cuello de botella.",
        field_extra={"custom": {"cellOptions": {"type": "auto"}}}),
  panel("Especificaciones del sistema", "specs", "table", 14, 28, 10, 10, fmt="table",
        desc="Lo que tiene la máquina. Se actualiza al arrancar el colector y una vez al día."),
  panel("CPU por contenedor (% de toda la máquina)", "cont_cpu", "timeseries", 0, 38, 12, 9, unit="percent",
        desc="Los 8 contenedores que más CPU usan. Sirve para ver si otro contenedor le quita recursos a Plex."),
  panel("RAM por contenedor", "cont_mem", "timeseries", 12, 38, 12, 9, unit="bytes"),
  row("Calidad y equidad por usuario", 47),
]
OFFSET = 48
for p in panels:
    p["gridPos"]["y"] += OFFSET
panels = capacity + panels

dash = {
  "__inputs": [{"name": "DS_PLEXFAIR", "label": "plexfair (PostgreSQL)", "type": "datasource",
                "pluginId": "grafana-postgresql-datasource", "pluginName": "PostgreSQL"}],
  "title": "Plex - Detalle", "uid": "plex-fairness", "timezone": TZ, "links": [{"type": "dashboards", "tags": ["plex"], "asDropdown": False, "includeVars": False,
             "keepTime": False, "targetBlank": False, "title": "Dashboards", "icon": "external link"}],
  "time": {"from": "now-24h", "to": "now"}, "refresh": "1m", "schemaVersion": 39, "tags": ["plex"],
  "templating": {"list": [
    {"name": "poll", "type": "constant", "query": "30", "hide": 2, "label": "Segundos entre muestreos"},
    {"name": "jain_threshold", "type": "custom", "query": "60 : 0.6,70 : 0.7,80 : 0.8,90 : 0.9", "label": "Umbral de equidad %",
     "current": {"text": "80", "value": "0.8"}, "options": []},
    {"name": "crit", "type": "custom", "query": "70,80,85,90,95", "label": "Umbral cuello de botella %",
     "current": {"text": "85", "value": "85"}, "options": []},
  ]},
  "panels": panels,
}
json.dump(dash, open("grafana_dashboard.json", "w"), ensure_ascii=False, indent=2)

# provisioned copy: no __inputs, datasource fixed to the uid created by grafana/provisioning
prov = json.loads(json.dumps(dash).replace("${DS_PLEXFAIR}", "plexfair"))
prov.pop("__inputs")
json.dump(prov, open("grafana/dashboards/plex-fairness.json", "w"), ensure_ascii=False, indent=2)

# ================= summary dashboard =================
_id = 0
STATE_COLORS = {"custom": {"cellOptions": {"type": "color-background", "mode": "basic"}, "align": "center"},
                "mappings": [{"type": "value", "options": {
                    "Sobrado": {"color": "green", "index": 0}, "Holgado": {"color": "blue", "index": 1},
                    "Vigilar": {"color": "yellow", "index": 2}, "Al límite": {"color": "red", "index": 3}}}]}

def stat(title, key, x, y, w, h, desc="", fmt="table", name=False, color="blue"):
    return panel(title, key, "stat", x, y, w, h, fmt=fmt, desc=desc,
                 extra={"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "textMode": "value_and_name" if name else "value", "colorMode": "background", "graphMode": "none",
                        "justifyMode": "center"},
                 field_extra={"color": {"mode": "fixed", "fixedColor": color}, "decimals": 1 if key == "sum_avg" else 0})

s_panels = [
  row("Uso de Plex", 0),
  stat("Viendo ahora", "sum_now", 0, 1, 4, 5, color="green"),
  stat("Pico de simultáneos", "sum_peak", 4, 1, 4, 5, fmt="time_series", name=True, color="purple",
       desc="Máximo de personas reproduciendo a la vez en el periodo, y cuándo fue."),
  stat("Media cuando hay uso", "sum_avg", 8, 1, 4, 5, desc="Personas viendo a la vez de media, contando solo los momentos en que alguien ve algo."),
  stat("Horas vistas", "sum_hours", 12, 1, 4, 5),
  stat("Usuarios activos", "sum_users", 16, 1, 4, 5),
  stat("Pico de transcodes", "sum_peak_trans", 20, 1, 4, 5, fmt="time_series", name=True, color="orange",
       desc="Máximo de transcodes a la vez. Son los únicos streams que usan la GPU."),

  row("Ocupación de los recursos", 6),
  panel("Ocupación de los recursos", "sum_resources", "table", 0, 7, 24, 12, fmt="table",
        desc="p95: el 95 % del tiempo estuvo por debajo de este valor. Estado según el p95: Sobrado <50 %, Holgado 50-70 %, Vigilar 70-85 %, Al límite >=85 %. 'Causa del pico' dice si el pico fue con gente viendo o por una tarea de fondo, y qué contenedor usaba CPU en ese momento.",
        field_extra={"custom": {"align": "auto"}}),

  row("Margen: cuántos streams más caben", 19),
  stat("Streams direct play extra", "sum_extra_dp", 0, 20, 8, 6, fmt="time_series", name=True, color="green",
       desc="Cuántas personas más podrían ver algo en direct play a la vez antes de que algún recurso llegue al umbral. Debajo, el recurso que llegaría primero."),
  stat("Transcodes extra", "sum_extra_tc", 8, 20, 8, 6, fmt="time_series", name=True, color="orange",
       desc="Cuántos transcodes más a la vez caben antes del límite de la GPU (codificador o las 8 sesiones del driver)."),
  {"id": 999, "type": "text", "title": "Cómo leerlo", "gridPos": {"x": 16, "y": 20, "w": 8, "h": 6},
   "options": {"mode": "markdown", "content":
     "- **Direct play** no usa la GPU: lo limitan CPU, RAM, discos y red.\n"
     "- **Transcode** usa la GPU y la RAM de transcode: el límite duro son las **8 sesiones** del driver de NVIDIA.\n"
     "- La estimación mejora con más días de datos y con momentos de varios streams a la vez.\n"
     "- **Discos**: en la práctica el límite llega antes si 5-6 personas ven 4K pesado **del mismo disco**."}},
  panel("Margen por recurso", "sum_headroom", "table", 0, 26, 24, 11, fmt="table",
        desc="Consumo por stream = uso medio con gente viendo menos uso en reposo, dividido entre streams. Streams extra = cuántos más caben partiendo del p95 hasta el umbral."),
]
# colour the Estado column
for p in s_panels:
    if p.get("targets") and p["targets"][0]["rawSql"] == Q["sum_resources"]:
        p["fieldConfig"]["overrides"] = [
            {"matcher": {"id": "byName", "options": "Estado"},
             "properties": [{"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}},
                            {"id": "mappings", "value": STATE_COLORS["mappings"]}]},
            {"matcher": {"id": "byName", "options": "Cuándo"},
             "properties": [{"id": "unit", "value": "dateTimeAsLocal"}]}]

summary = {
  "__inputs": dash["__inputs"],
  "title": "Plex - Resumen", "uid": "plex-summary", "timezone": TZ, "links": dash["links"],
  "time": {"from": "now-90d", "to": "now"}, "refresh": "5m", "schemaVersion": 39, "tags": ["plex"],
  "templating": {"list": [
    {"name": "poll", "type": "constant", "query": "30", "hide": 2},
    {"name": "crit", "type": "custom", "query": "70,80,85,90,95", "label": "Umbral %",
     "current": {"text": "85", "value": "85"}, "options": []},
  ]},
  "panels": s_panels,
}
json.dump(summary, open("grafana_dashboard_summary.json", "w"), ensure_ascii=False, indent=2)
sprov = json.loads(json.dumps(summary).replace("${DS_PLEXFAIR}", "plexfair"))
sprov.pop("__inputs")
json.dump(sprov, open("grafana/dashboards/plex-summary.json", "w"), ensure_ascii=False, indent=2)
print("dashboards ok:", len(panels), "detail panels,", len(s_panels), "summary panels")
