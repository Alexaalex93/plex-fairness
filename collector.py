"""
Plex fairness collector.

Every POLL_SECONDS I ask Tautulli what everyone is watching right now and save a snapshot:
bandwidth, source vs delivered quality, transcode speed, which array disk the file lives on...
On top of that I grab GPU load (nvidia-smi) and per-disk read load (/proc/diskstats).
With all that in Postgres, Grafana can show who gets what when several people watch at once.
On top of that, system_stats.py measures how busy every resource is (CPU, RAM, network, GPU, disks...)
and each poll stores which one is closest to its limit, so I can see what the bottleneck was.

It also runs a tiny HTTP server so Tautulli can send a webhook on "Buffer Warning",
that way buffering events end up in the same database.
"""
import json
import logging
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import psycopg
import requests

import system_stats as ss

# ---------- config (all from env vars) ----------
TAUTULLI_URL = os.environ["TAUTULLI_URL"].rstrip("/")
TAUTULLI_APIKEY = os.environ["TAUTULLI_APIKEY"]
PG_DSN = os.environ["PG_DSN"]
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8787"))
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "180"))

# Plex sees files with its own container path (e.g. /data/movies/x.mkv).
# I translate that to the same file under /mnt/user mounted in this container,
# so I can ask Unraid which physical disk it's on.
PATH_MAP_FROM = os.getenv("PATH_MAP_FROM", "")            # e.g. /data
PATH_MAP_TO = os.getenv("PATH_MAP_TO", "/host_user")      # e.g. /host_user/media
DISKS_INI = os.getenv("DISKS_INI", "/host_emhttp/disks.ini")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("plex-fairness")

SCHEMA = """
CREATE TABLE IF NOT EXISTS poll (
    ts               timestamptz PRIMARY KEY,
    streams          int,
    playing          int,
    transcodes       int,
    total_bw_kbps    int,
    wan_bw_kbps      int,
    lan_bw_kbps      int,
    jain_bandwidth   real,
    jain_quality     real,
    gpu_util         real,
    gpu_enc_util     real,
    gpu_dec_util     real,
    gpu_enc_sessions int,
    gpu_mem_mb       real
);
CREATE TABLE IF NOT EXISTS session_sample (
    ts                  timestamptz,
    session_key         text,
    user_name           text,
    player              text,
    platform            text,
    product             text,
    location            text,
    state               text,
    media_type          text,
    title               text,
    transcode_decision  text,
    video_decision      text,
    audio_decision      text,
    src_height          int,
    stream_height       int,
    src_bitrate_kbps    int,
    stream_bitrate_kbps int,
    bandwidth_kbps      int,
    quality_ratio       real,
    transcode_speed     real,
    hw_decode           boolean,
    hw_encode           boolean,
    throttled           boolean,
    disk                text,
    PRIMARY KEY (ts, session_key)
);
CREATE INDEX IF NOT EXISTS session_sample_user_ts ON session_sample (user_name, ts);
CREATE TABLE IF NOT EXISTS disk_sample (
    ts        timestamptz,
    disk      text,
    read_mbps real,
    util_pct  real,
    PRIMARY KEY (ts, disk)
);
CREATE TABLE IF NOT EXISTS buffer_event (
    id          bigserial PRIMARY KEY,
    ts          timestamptz DEFAULT now(),
    user_name   text,
    title       text,
    session_key text,
    raw         jsonb
);
ALTER TABLE poll ADD COLUMN IF NOT EXISTS bottleneck text;
ALTER TABLE poll ADD COLUMN IF NOT EXISTS bottleneck_pct real;
CREATE TABLE IF NOT EXISTS resource_sample (
    ts       timestamptz,
    resource text,
    used     real,
    capacity real,
    unit     text,
    pct      real,
    PRIMARY KEY (ts, resource)
);
CREATE TABLE IF NOT EXISTS container_sample (
    ts      timestamptz,
    name    text,
    cpu_pct real,
    mem_mb  real,
    PRIMARY KEY (ts, name)
);
CREATE TABLE IF NOT EXISTS system_spec (
    component  text,
    name       text,
    capacity   real,
    unit       text,
    detail     text,
    updated_at timestamptz DEFAULT now(),
    PRIMARY KEY (component, name)
);
"""


# ---------- small helpers ----------
def to_int(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def res_height(res):
    """Turn Tautulli resolution strings ('4k', '1080p', '720', 'sd') into a height in pixels."""
    if not res:
        return None
    r = str(res).lower()
    if "4k" in r:
        return 2160
    if r.startswith("sd"):
        return 480
    m = re.match(r"(\d+)", r)
    return int(m.group(1)) if m else None


def jain(values):
    """
    Jain's fairness index: 1.0 = everyone gets exactly the same, 1/n = one guy gets everything.
    Only makes sense with 2+ people watching, otherwise I return None.
    """
    vals = [v for v in values if v is not None]
    if len(vals) < 2:
        return None
    s = sum(vals)
    sq = sum(v * v for v in vals)
    return (s * s) / (len(vals) * sq) if sq > 0 else None


def quality_ratio(s):
    """
    How much of the original quality the user is actually getting (0..1).
    Direct play / copy = 1. If transcoding, delivered bitrate / source bitrate.
    I use this instead of raw bandwidth for fairness, because a 4K remux and a 720p
    episode need very different bandwidth and that's not "unfair" by itself.
    """
    vd = (s.get("video_decision") or "").lower()
    if vd in ("direct play", "copy"):
        return 1.0
    src = to_int(s.get("bitrate"))
    out = to_int(s.get("stream_bitrate"))
    if src and out:
        return min(out / src, 1.0)
    return None


# ---------- file -> physical disk (Unraid) ----------
_disk_cache = {}


def file_disk(plex_path):
    """
    Unraid's user share (shfs) exposes an xattr 'system.LOCATION' that says which disk holds the file.
    Reading it doesn't spin up anything, unlike checking /mnt/diskX one by one.
    """
    if not plex_path or not PATH_MAP_FROM:
        return None
    if plex_path in _disk_cache:
        return _disk_cache[plex_path]
    disk = None
    if plex_path.startswith(PATH_MAP_FROM):
        local = PATH_MAP_TO + plex_path[len(PATH_MAP_FROM):]
        try:
            disk = os.getxattr(local, "system.LOCATION").decode().strip("\x00 ")
        except OSError:
            disk = None
    _disk_cache[plex_path] = disk
    return disk


# ---------- array disk load from /proc/diskstats ----------
def load_disk_names():
    """Read Unraid's disks.ini to map sdX -> disk1/parity/cache..."""
    mapping = {}
    try:
        with open(DISKS_INI) as f:
            content = f.read()
    except OSError:
        log.warning("Can't read %s, disk stats will use raw device names", DISKS_INI)
        return mapping
    for block in re.split(r'^\["[^"]+"\]\s*$', content, flags=re.M):
        name = re.search(r'^name="([^"]*)"', block, re.M)
        dev = re.search(r'^device="([^"]*)"', block, re.M)
        if name and dev and dev.group(1):
            mapping[dev.group(1)] = name.group(1)
    return mapping


class DiskStats:
    def __init__(self):
        self.names = load_disk_names()
        self.prev = None
        self.prev_t = None

    def _read(self):
        stats = {}
        with open("/proc/diskstats") as f:
            for line in f:
                p = line.split()
                dev = p[2]
                # only whole devices that Unraid knows about (skip partitions, loop, md...)
                if self.names and dev not in self.names:
                    continue
                if not self.names and not re.fullmatch(r"(sd[a-z]+|nvme\d+n\d+)", dev):
                    continue
                stats[dev] = (int(p[5]), int(p[12]))  # sectors read, ms spent doing IO
        return stats

    def sample(self):
        """Returns [(disk, read_MB/s, util%)] since the previous call."""
        now = time.monotonic()
        cur = self._read()
        out = []
        if self.prev is not None:
            dt = now - self.prev_t
            for dev, (sectors, io_ms) in cur.items():
                if dev not in self.prev:
                    continue
                ps, pio = self.prev[dev]
                read_mbps = (sectors - ps) * 512 / dt / 1_000_000
                util = min((io_ms - pio) / (dt * 1000) * 100, 100.0)
                out.append((self.names.get(dev, dev), read_mbps, util))
        self.prev, self.prev_t = cur, now
        return out


# ---------- Tautulli ----------
def get_activity():
    r = requests.get(
        f"{TAUTULLI_URL}/api/v2",
        params={"apikey": TAUTULLI_APIKEY, "cmd": "get_activity"},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["response"]["data"]


def session_row(ts, s):
    return (
        ts,
        str(s.get("session_key") or s.get("session_id")),
        s.get("friendly_name") or s.get("user"),
        s.get("player"),
        s.get("platform"),
        s.get("product"),
        s.get("location"),
        s.get("state"),
        s.get("media_type"),
        s.get("full_title") or s.get("title"),
        s.get("transcode_decision"),
        s.get("video_decision"),
        s.get("audio_decision"),
        res_height(s.get("video_full_resolution") or s.get("video_resolution")),
        res_height(s.get("stream_video_full_resolution") or s.get("stream_video_resolution")),
        to_int(s.get("bitrate")),
        to_int(s.get("stream_bitrate")),
        to_int(s.get("bandwidth")),
        quality_ratio(s),
        to_float(s.get("transcode_speed")),
        bool(to_int(s.get("transcode_hw_decoding"))),
        bool(to_int(s.get("transcode_hw_encoding"))),
        bool(to_int(s.get("transcode_throttled"))),
        file_disk(s.get("file")),
    )


# ---------- webhook for buffer warnings ----------
class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            data = json.loads(body or b"{}")
            with psycopg.connect(PG_DSN, autocommit=True) as conn:
                conn.execute(
                    "INSERT INTO buffer_event (user_name, title, session_key, raw) VALUES (%s,%s,%s,%s)",
                    (data.get("user"), data.get("title"), str(data.get("session_key", "")), json.dumps(data)),
                )
            log.info("Buffer event: %s - %s", data.get("user"), data.get("title"))
            self.send_response(200)
        except Exception as e:
            log.error("Webhook error: %s", e)
            self.send_response(400)
        self.end_headers()

    def log_message(self, *args):  # keep the log clean
        pass


# ---------- main loop ----------
class Probes:
    """All the stateful samplers together (they need the previous reading to compute rates)."""

    def __init__(self):
        self.disks = DiskStats()
        self.cpu = ss.CpuStats()
        self.net = ss.NetStats()
        self.containers = ss.ContainerStats()
        self.disks.sample()       # first reads are just the baseline for the deltas
        _safe_call(self.containers.sample)
        log.info("Network: %s at %s Mbps | docker stats: %s", self.net.iface, self.net.speed, self.containers.enabled)


def _safe_call(fn, *a, default=None):
    try:
        return fn(*a)
    except Exception as e:
        log.warning("%s failed: %s", getattr(fn, "__qualname__", fn), e)
        return default


def save_specs(conn, probes):
    rows = ss.specs(probes.disks.names, probes.net)
    with conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO system_spec (component, name, capacity, unit, detail, updated_at)
               VALUES (%s,%s,%s,%s,%s, now())
               ON CONFLICT (component, name) DO UPDATE
               SET capacity = EXCLUDED.capacity, unit = EXCLUDED.unit, detail = EXCLUDED.detail, updated_at = now()""",
            rows,
        )
    log.info("Specs saved: %s", ", ".join(f"{r[0]} {r[2]} {r[3]}" for r in rows))


def collect_once(conn, probes):
    ts = datetime.now(timezone.utc).replace(microsecond=0)
    act = get_activity()
    sessions = act.get("sessions", [])
    rows = [session_row(ts, s) for s in sessions]
    playing = [r for r in rows if r[7] == "playing"]

    g = ss.gpu()
    disk_rows = probes.disks.sample()
    res = ss.resources(probes.cpu, probes.net, g, disk_rows, to_int(act.get("wan_bandwidth")))
    bottleneck, bottleneck_pct = ss.bottleneck(res)
    containers = _safe_call(probes.containers.sample, default=[]) or []

    poll = (
        ts,
        to_int(act.get("stream_count")),
        len(playing),
        sum(1 for r in rows if (r[10] or "").lower() == "transcode"),
        to_int(act.get("total_bandwidth")),
        to_int(act.get("wan_bandwidth")),
        to_int(act.get("lan_bandwidth")),
        jain([r[17] for r in playing]),   # bandwidth
        jain([r[18] for r in playing]),   # quality ratio
        g.get("utilization.gpu"),
        g.get("utilization.encoder"),
        g.get("utilization.decoder"),
        to_int(g.get("encoder.stats.sessionCount")),
        g.get("memory.used"),
        bottleneck,
        bottleneck_pct,
    )

    with conn.transaction():
        conn.execute(
            """INSERT INTO poll (ts, streams, playing, transcodes, total_bw_kbps, wan_bw_kbps, lan_bw_kbps,
                                 jain_bandwidth, jain_quality, gpu_util, gpu_enc_util, gpu_dec_util,
                                 gpu_enc_sessions, gpu_mem_mb, bottleneck, bottleneck_pct)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            poll,
        )
        with conn.cursor() as cur:
            if rows:
                cur.executemany(
                    "INSERT INTO session_sample VALUES (" + ",".join(["%s"] * 24) + ") ON CONFLICT DO NOTHING",
                    rows,
                )
            if disk_rows:
                cur.executemany("INSERT INTO disk_sample VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                                [(ts, d, r, u) for d, r, u in disk_rows])
            if res:
                cur.executemany("INSERT INTO resource_sample VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                                [(ts, *r) for r in res])
            if containers:
                cur.executemany("INSERT INTO container_sample VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                                [(ts, *c) for c in containers])

    if playing or (bottleneck_pct or 0) >= 85:
        log.info("%d playing, %d transcoding, jain quality=%s, busiest: %s %s%%",
                 len(playing), poll[3], poll[8], bottleneck, bottleneck_pct)


def cleanup(conn):
    for table in ("poll", "session_sample", "disk_sample", "buffer_event", "resource_sample", "container_sample"):
        conn.execute(f"DELETE FROM {table} WHERE ts < now() - make_interval(days => %s)", (RETENTION_DAYS,))


def main():
    threading.Thread(
        target=HTTPServer(("0.0.0.0", WEBHOOK_PORT), WebhookHandler).serve_forever, daemon=True
    ).start()
    log.info("Webhook listening on :%d", WEBHOOK_PORT)

    probes = Probes()
    conn = None
    last_cleanup = 0.0
    last_specs = 0.0

    while True:
        started = time.monotonic()
        try:
            if conn is None or conn.closed:
                conn = psycopg.connect(PG_DSN, autocommit=True)
                conn.execute(SCHEMA)
                log.info("Connected to Postgres")
            # specs once a day; if it fails it shouldn't stop the stats collection
            if not last_specs or started - last_specs > 86400:
                _safe_call(save_specs, conn, probes)
                last_specs = started
            collect_once(conn, probes)
            if started - last_cleanup > 3600:
                cleanup(conn)
                last_cleanup = started
        except psycopg.OperationalError as e:
            log.error("Postgres error, will reconnect: %s", e)
            conn = None
        except Exception as e:
            # Tautulli down, Plex restarting... just keep going, next poll will probably work
            log.error("Poll failed: %s", e)
        time.sleep(max(0, POLL_SECONDS - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
