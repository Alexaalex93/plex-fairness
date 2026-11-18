# Plex fairness collector

Además de lo de cada stream, mide:
- **Especificaciones del sistema**: CPU, RAM, GPU, red, Internet y discos.
- **Ocupación de cada recurso**, en % sobre su capacidad: CPU, RAM, `/dev/shm`, red, Internet, GPU (NVENC, NVDEC, VRAM y sesiones) y cada disco.
- **El cuello de botella de cada momento** y los episodios en que algún recurso pasa del umbral.
- **Estimación de cuántos streams más caben** antes de llegar al límite.

Las gráficas se ven en Grafana, en `http://IP_NAS:6002`: es el contenedor `plex-fairness-grafana`, que ya trae la conexión y el dashboard.

Muestrea Tautulli cada 30 s y guarda en Postgres:
- **Por stream**: ancho de banda, calidad original frente a servida, decisión de transcode, velocidad del transcode y disco donde está el fichero.
- **Del sistema**: carga de la GPU y lectura de cada disco del array.

Encima tiene un dashboard de Grafana.

## Instalación (Unraid con el plugin Compose Manager)
1. Copia esta carpeta a `/mnt/user/appdata/plex-fairness/`.
2. Edita `docker-compose.yml`:
   - `TAUTULLI_URL`, `TAUTULLI_APIKEY` y la contraseña de Postgres.
   - `PATH_MAP_FROM` / `PATH_MAP_TO`: la ruta de la multimedia tal como la ve Plex, y esa misma carpeta dentro de `/host_user`. Por ejemplo, si Plex monta `/mnt/user/media` como `/data`, pon `/data` → `/host_user/media`.
   - Comprueba que funciona: `getfattr -n system.LOCATION /mnt/user/media/<algún fichero>` debe devolver `disk3` o similar.
3. Arranca con `docker compose pull && docker compose up -d` y sigue el log con `docker logs -f plex-fairness-collector`.

## Webhook de buffer (Tautulli)
Settings → Notification Agents → Webhook:
- **URL**: `http://IP_NAS:8787`, método POST.
- **Trigger**: Buffer Warning.
- **Data → Buffer Warning (JSON)**: `{"user":"{user}","title":"{title}","session_key":"{session_key}"}`

Los umbrales del aviso se ajustan en Settings → Notifications (Buffer Warning Threshold).

## Grafana
1. Añade un datasource PostgreSQL: host `IP_NAS:5433`, base de datos `plexfair`, usuario `plexfair`, SSL disabled.
2. Dashboards → Import → `grafana_dashboard.json` y elige ese datasource.
3. Si cambias `POLL_SECONDS`, cambia también la variable `poll` del dashboard (se usa para calcular las horas).

## Cómo leerlo
- **Jain calidad**: compara la calidad servida frente a la original de cada usuario. 1 es un reparto justo; por debajo de 0,8, alguien está saliendo perdiendo.
- **Momentos injustos**: cada fila dice quién iba al X % de calidad y a qué velocidad de transcode, para que veas la causa.
- **Velocidad de transcode < 1x a la vez que NVENC alto**: el límite es la GPU.
- **Varios streams en el mismo disco y ese disco cerca del 100 %**: el límite es el disco.

## Regenerar el dashboard
Las consultas y los paneles están en `tools/build_dashboard.py`. Para regenerar los dos JSON, ejecuta desde la raíz del repo `python3 tools/build_dashboard.py`.
