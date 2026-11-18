"""
System-wide stats: what the machine has (specs) and how busy each part of it is.

Every resource is returned as (resource, used, capacity, unit, pct) so the dashboard can treat
CPU, RAM, network, GPU and disks the same way and say which one is closest to its limit,
that's the bottleneck.

Needs (see docker-compose):
  - network_mode: host            -> real NIC traffic and link speed
  - /dev/shm mounted at HOST_SHM  -> Plex transcodes to RAM there
  - docker.sock (read only)       -> CPU / RAM per container
  - nvidia runtime                -> nvidia-smi
"""
import http.client
import json
import os
import re
import socket
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

HOST_SHM = os.getenv("HOST_SHM", "/host_shm")
DOCKER_SOCK = os.getenv("DOCKER_SOCK", "/var/run/docker.sock")
NET_IFACE = os.getenv("NET_IFACE", "")                # empty = interface with the default route
NIC_SPEED_MBPS = os.getenv("NIC_SPEED_MBPS", "")      # only if autodetect gets it wrong
WAN_UP_MBPS = float(os.getenv("WAN_UP_MBPS", "0") or 0)
NVENC_SESSION_LIMIT = int(os.getenv("NVENC_SESSION_LIMIT", "8"))
GPU_TEMP_LIMIT = float(os.getenv("GPU_TEMP_LIMIT", "85"))

# Resource names, these are what you see in Grafana
R_CPU = "CPU"
R_CPU_CORE = "CPU núcleo más cargado"
R_RAM = "RAM"
R_SHM = "RAM transcode (/dev/shm)"
R_NIC = "Red (tarjeta)"
R_WAN = "Internet (subida Plex)"
R_GPU = "GPU"
R_NVENC = "GPU NVENC (codificar)"
R_NVDEC = "GPU NVDEC (decodificar)"
R_VRAM = "GPU VRAM"
R_SESSIONS = "GPU sesiones NVENC"
R_GPU_TEMP = "GPU temperatura"

# These are shown but never picked as "the bottleneck": a hot GPU or one busy core
# for a second doesn't mean the system is out of capacity.
NOT_BOTTLENECK = {R_CPU_CORE, R_GPU_TEMP}


def pct(used, cap):
    return round(100.0 * used / cap, 1) if cap else None


def _read(path, default=None):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return default


# ---------- CPU ----------
class CpuStats:
    def __init__(self):
        self.prev = self._read()

    @staticmethod
    def _read():
        out = {}
        with open("/proc/stat") as f:
            for line in f:
                if not line.startswith("cpu"):
                    break
                p = line.split()
                vals = list(map(int, p[1:9]))      # guest time is already inside user, skip it
                idle = vals[3] + vals[4]           # idle + iowait (disk waits show up in the disk stats)
                out[p[0]] = (sum(vals), idle)
        return out

    def sample(self):
        """Returns (total %, busiest core %) since the last call."""
        cur = self._read()
        res = {}
        for k, (tot, idle) in cur.items():
            if k in self.prev:
                dt = tot - self.prev[k][0]
                di = idle - self.prev[k][1]
                res[k] = 100.0 * (dt - di) / dt if dt > 0 else 0.0
        self.prev = cur
        cores = [v for k, v in res.items() if k != "cpu"]
        return res.get("cpu", 0.0), (max(cores) if cores else 0.0)


def cpu_spec():
    model, cores = None, set()
    phys = core = None
    with open("/proc/cpuinfo") as f:
        for line in f:
            if ":" not in line:
                continue
            k, v = [x.strip() for x in line.split(":", 1)]
            if k == "model name" and not model:
                model = v
            elif k == "physical id":
                phys = v
            elif k == "core id":
                core = v
                cores.add((phys, core))
    return model or "?", len(cores) or os.cpu_count(), os.cpu_count()


# ---------- RAM ----------
def mem_gb():
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":", 1)
            info[k] = int(v.split()[0])  # kB
    total = info["MemTotal"] / 1024 / 1024
    avail = info.get("MemAvailable", info["MemFree"]) / 1024 / 1024
    return total - avail, total


def shm_gb():
    try:
        st = os.statvfs(HOST_SHM)
    except OSError:
        return None, None
    total = st.f_blocks * st.f_frsize / 1024 ** 3
    free = st.f_bavail * st.f_frsize / 1024 ** 3
    return total - free, total


# ---------- network ----------
VIRTUAL_IFACES = re.compile(r"^(veth|vnet|docker|virbr|tap|tun|wg|lo|shim|vhost)")


def default_iface():
    if NET_IFACE:
        return NET_IFACE
    try:
        with open("/proc/net/route") as f:
            for line in list(f)[1:]:
                p = line.split()
                if p[1] == "00000000":
                    return p[0]
    except OSError:
        pass
    return "eth0"


def link_speed(iface, depth=0):
    """
    Bridges (br0) and bonds don't report a speed, so I look at the physical ports behind them.
    Virtual stuff (veth of containers...) is skipped, it reports fake speeds.
    """
    s = _read(f"/sys/class/net/{iface}/speed")
    try:
        if s and int(s) > 0:
            return int(s)
    except ValueError:
        pass
    if depth > 3:
        return None
    kids = []
    if os.path.isdir(f"/sys/class/net/{iface}/brif"):
        kids += os.listdir(f"/sys/class/net/{iface}/brif")
    kids += (_read(f"/sys/class/net/{iface}/bonding/slaves", "") or "").split()
    speeds = [link_speed(k, depth + 1) for k in kids if not VIRTUAL_IFACES.match(k)]
    speeds = [x for x in speeds if x]
    return max(speeds) if speeds else None


class NetStats:
    def __init__(self):
        self.iface = default_iface()
        self.speed = int(NIC_SPEED_MBPS) if NIC_SPEED_MBPS else link_speed(self.iface)
        self.prev = self._read()
        self.prev_t = time.monotonic()

    def _read(self):
        with open("/proc/net/dev") as f:
            for line in f:
                if ":" not in line:
                    continue
                name, data = line.split(":", 1)
                if name.strip() == self.iface:
                    p = data.split()
                    return int(p[0]), int(p[8])  # rx bytes, tx bytes
        return None

    def sample(self):
        """Returns (rx Mbps, tx Mbps) since the last call."""
        cur, now = self._read(), time.monotonic()
        out = (None, None)
        if cur and self.prev:
            dt = now - self.prev_t
            out = ((cur[0] - self.prev[0]) * 8 / dt / 1e6, (cur[1] - self.prev[1]) * 8 / dt / 1e6)
        self.prev, self.prev_t = cur, now
        return out


# ---------- GPU ----------
GPU_FIELDS = [
    "name", "driver_version", "utilization.gpu", "utilization.encoder", "utilization.decoder",
    "encoder.stats.sessionCount", "memory.used", "memory.total", "temperature.gpu",
]


def gpu():
    """Dict with nvidia-smi values, or {} if there's no GPU / no nvidia runtime."""
    try:
        line = subprocess.run(
            ["nvidia-smi", f"--query-gpu={','.join(GPU_FIELDS)}", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10, check=True,
        ).stdout.strip().splitlines()[0]
    except (OSError, subprocess.SubprocessError, IndexError):
        return {}
    out = {}
    for k, v in zip(GPU_FIELDS, [x.strip() for x in line.split(",")]):
        try:
            out[k] = float(v)
        except ValueError:
            out[k] = v if v not in ("[N/A]", "N/A") else None
    return out


# ---------- disks ----------
def disk_specs(names):
    """names: {sdX: disk1}. Returns [(disk name, size TB, model, is_hdd)]"""
    out = []
    for dev, name in names.items():
        size = _read(f"/sys/block/{dev}/size")
        if not size:
            continue
        rot = _read(f"/sys/block/{dev}/queue/rotational", "1") == "1"
        model = _read(f"/sys/block/{dev}/device/model", "?")
        out.append((name, int(size) * 512 / 1e12, model, rot))
    return out


# ---------- containers (docker API over the unix socket) ----------
class _UnixConn(http.client.HTTPConnection):
    def __init__(self, sock_path):
        super().__init__("localhost", timeout=15)
        self.sock_path = sock_path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(15)
        self.sock.connect(self.sock_path)


def _docker_get(url):
    c = _UnixConn(DOCKER_SOCK)
    try:
        c.request("GET", url)
        r = c.getresponse()
        data = r.read()
        if r.status != 200:
            raise RuntimeError(f"docker API {r.status} on {url}")
        return json.loads(data)
    finally:
        c.close()


class ContainerStats:
    """
    CPU and RAM per container. one-shot=true makes docker answer right away (no 1 s wait per container),
    but then it doesn't give the previous CPU reading, so I keep my own and do the delta myself.
    CPU % here is % of the WHOLE machine (all threads), so it's comparable with the CPU gauge.
    """

    def __init__(self):
        self.enabled = os.path.exists(DOCKER_SOCK)
        self.prev = {}

    def _one(self, c):
        s = _docker_get(f"/containers/{c['Id']}/stats?stream=false&one-shot=true")
        name = c["Names"][0].lstrip("/")
        cpu = s.get("cpu_stats", {})
        mem = s.get("memory_stats", {})
        stats = mem.get("stats", {})
        cache = stats.get("inactive_file", stats.get("total_inactive_file", 0))
        mem_mb = max(mem.get("usage", 0) - cache, 0) / 1024 / 1024
        return c["Id"], name, cpu.get("cpu_usage", {}).get("total_usage", 0), cpu.get("system_cpu_usage", 0), mem_mb

    def sample(self):
        """Returns [(name, cpu % of machine, mem MB)]. First call only fills the baseline."""
        if not self.enabled:
            return []
        containers = _docker_get("/containers/json")
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda c: _safe(self._one, c), containers))
        out, new_prev = [], {}
        for r in results:
            if not r:
                continue
            cid, name, cpu_tot, sys_tot, mem_mb = r
            new_prev[cid] = (cpu_tot, sys_tot)
            if cid in self.prev:
                dc = cpu_tot - self.prev[cid][0]
                ds = sys_tot - self.prev[cid][1]
                out.append((name, round(100.0 * dc / ds, 2) if ds > 0 else 0.0, round(mem_mb, 1)))
        self.prev = new_prev
        return out


def _safe(fn, *a):
    try:
        return fn(*a)
    except Exception:
        return None


# ---------- specs ----------
def specs(disk_names, net):
    """Rows for the system_spec table: (component, name, capacity, unit, detail)."""
    rows = []
    model, cores, threads = cpu_spec()
    rows.append(("CPU", "cpu", threads, "hilos", f"{model} · {cores} núcleos"))
    _, ram = mem_gb()
    rows.append(("RAM", "ram", round(ram, 1), "GB", ""))
    _, shm = shm_gb()
    if shm:
        rows.append(("RAM transcode", "shm", round(shm, 1), "GB", "/dev/shm, donde transcodifica Plex"))
    g = gpu()
    if g:
        rows.append(("GPU", str(g.get("name")), round((g.get("memory.total") or 0) / 1024, 1), "GB VRAM",
                     f"driver {g.get('driver_version')} · límite NVENC {NVENC_SESSION_LIMIT} sesiones"))
    rows.append(("Red", net.iface, net.speed, "Mbps", f"interfaz {net.iface}"))
    if WAN_UP_MBPS:
        rows.append(("Internet", "wan", WAN_UP_MBPS, "Mbps subida", "configurado en WAN_UP_MBPS"))
    for name, tb, dmodel, hdd in disk_specs(disk_names):
        rows.append(("Disco", name, round(tb, 2), "TB", f"{'HDD' if hdd else 'SSD'} · {dmodel}"))
    return rows


# ---------- everything for one poll ----------
def resources(cpu, net, g, disk_rows, wan_kbps):
    """
    Builds the list of (resource, used, capacity, unit, pct) for this poll.
    disk_rows: [(disk, read MB/s, util %)] from DiskStats.
    """
    out = []
    total, core = cpu.sample()
    out.append((R_CPU, round(total, 1), 100, "%", round(total, 1)))
    out.append((R_CPU_CORE, round(core, 1), 100, "%", round(core, 1)))

    used, cap = mem_gb()
    out.append((R_RAM, round(used, 2), round(cap, 1), "GB", pct(used, cap)))
    used, cap = shm_gb()
    if cap:
        out.append((R_SHM, round(used, 2), round(cap, 1), "GB", pct(used, cap)))

    rx, tx = net.sample()
    if tx is not None and net.speed:
        busiest = max(rx, tx)  # full duplex, so whichever direction is busier
        out.append((R_NIC, round(busiest, 1), net.speed, "Mbps", pct(busiest, net.speed)))
    if WAN_UP_MBPS and wan_kbps is not None:
        out.append((R_WAN, round(wan_kbps / 1000, 1), WAN_UP_MBPS, "Mbps", pct(wan_kbps / 1000, WAN_UP_MBPS)))

    if g:
        for name, key in ((R_GPU, "utilization.gpu"), (R_NVENC, "utilization.encoder"), (R_NVDEC, "utilization.decoder")):
            if g.get(key) is not None:
                out.append((name, g[key], 100, "%", g[key]))
        if g.get("memory.total"):
            out.append((R_VRAM, round(g["memory.used"] / 1024, 2), round(g["memory.total"] / 1024, 1), "GB",
                        pct(g["memory.used"], g["memory.total"])))
        if g.get("encoder.stats.sessionCount") is not None:
            n = g["encoder.stats.sessionCount"]
            out.append((R_SESSIONS, n, NVENC_SESSION_LIMIT, "sesiones", pct(n, NVENC_SESSION_LIMIT)))
        if g.get("temperature.gpu") is not None:
            t = g["temperature.gpu"]
            out.append((R_GPU_TEMP, t, GPU_TEMP_LIMIT, "°C", pct(t, GPU_TEMP_LIMIT)))

    for disk, _read_mbps, util in disk_rows:
        if disk == "flash":  # USB boot stick, not relevant
            continue
        out.append((f"Disco {disk}", round(util, 1), 100, "%", round(util, 1)))
    return out


def bottleneck(res):
    """The resource closest to its limit right now (ignoring temperature and the single busiest core)."""
    cands = [r for r in res if r[0] not in NOT_BOTTLENECK and r[4] is not None]
    if not cands:
        return None, None
    r = max(cands, key=lambda x: x[4])
    return r[0], r[4]
