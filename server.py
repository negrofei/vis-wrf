#!/usr/bin/env python3
"""Visor local de productos WRF.

Para sumar un campo, agregar un objeto en fields.json y reiniciar.
El ciclo vigente es el que tiene la última imagen con fecha más nueva.
Al abrir un campo se descargan todas sus horas para el ciclo elegido
y quedan en cache/.
"""

import csv
import json
import os
import re
import shutil
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "fields.json"
STATIONS_PATH = ROOT / "PInteres.dat"
CACHE_DIR = ROOT / "cache"
METEOGRAM_PRODUCT = "WRF_ARG4K.FCST_METEOGRAM"
CIPI_RE = re.compile(r"^[A-Z]{2}\d{2}$")
SAVIPA_FIR_RE = re.compile(r"^[A-Z]{3}$")
SAVIPA_FIELD_RE = re.compile(r"^(TURB|ICE)$")
SAVIPA_FILE_RE = re.compile(
    r"(?P<fir>[A-Za-z]{3})_(?P<field>TURB|ICE)(?P<level>\d{3})_(?P<time>\d{2})\.png",
    re.IGNORECASE,
)
SAVIPA_LEVEL_RE = re.compile(r"(?:TURB|ICE)(\d{3})", re.IGNORECASE)
USER_AGENT = "vis-wrf"
STATUS_TTL_SECONDS = 20
SAVIPA_LIST_TTL_SECONDS = 60

_status_cache = {}
_status_lock = threading.Lock()
_jobs = {}
_jobs_lock = threading.Lock()
_file_locks = {}
_file_locks_guard = threading.Lock()


def load_config():
    with CONFIG_PATH.open(encoding="utf-8") as handle:
        config = json.load(handle)
    for field in config.get("fields", []):
        if "hours" in field:
            field["hours"] = [int(hour) for hour in field["hours"]]
        else:
            field["hours"] = list(range(int(field["hour_from"]), int(field["hour_to"]) + 1))
        if not field["hours"]:
            raise ValueError(f"El campo {field['id']} no tiene horas")
    for field in config.get("region_fields", []):
        if "hours" in field:
            field["hours"] = [int(hour) for hour in field["hours"]]
        else:
            field["hours"] = list(range(int(field["hour_from"]), int(field["hour_to"]) + 1))
        if not field["hours"]:
            raise ValueError(f"El campo regional {field['id']} no tiene horas")
    return config


def field_by_id(config, field_id):
    for field in config.get("fields", []):
        if field["id"] == field_id:
            return field
    return None


def region_by_id(config, region_id):
    for region in config.get("regions", []):
        if region["id"] == region_id:
            return region
    return None


def region_field_by_id(config, field_id):
    for field in config.get("region_fields", []):
        if field["id"] == field_id:
            return field
    return None


def field_domains(config):
    domains = config.get("domains") or [
        {"id": "ARG", "name": "Todo el territorio"},
        {"id": "NOR", "name": "Norte"},
        {"id": "SUR", "name": "Sur"},
    ]
    return domains


def default_domain(config):
    return config.get("default_domain") or "ARG"


def normalize_domain(config, domain):
    value = (domain or default_domain(config)).strip().upper()
    allowed = {item["id"].upper() for item in field_domains(config)}
    if value not in allowed:
        return None
    return value


def field_product(field, domain):
    """Build the repo2 product stem for a field + coverage domain.

    ``field["product"]`` is the base stem without ARG/NOR/SUR.
    Templates with ``{domain}`` are still accepted for compatibility.
    """
    product = (field.get("product") or "").strip()
    domain = (domain or "ARG").strip().upper()
    if "{domain}" in product:
        resolved = product.replace("{domain}", domain)
    elif product.endswith(("_ARG", "_NOR", "_SUR")):
        resolved = product[:-4] + "_" + domain
    else:
        resolved = product + "_" + domain
    if "{domain}" in resolved or "{" in resolved or "}" in resolved:
        raise ValueError(f"Producto sin resolver: {resolved!r}")
    return resolved


def remote_url(config, field, cycle, hour, domain):
    product = field_product(field, domain)
    # Avoid f-strings here so a leftover "{domain}" can never be interpolated.
    return (
        config["base_url"]
        + product
        + "."
        + str(cycle)
        + "UTC.F"
        + format(int(hour), "03d")
        + ".png"
    )


def region_remote_url(config, region, field, cycle, hour):
    product = field["product_template"].replace("{region}", region["id"])
    return f"{config['base_url']}{product}.{cycle}UTC.F{int(hour):03d}.png"


def region_cache_dir(region_id, field_id, cycle):
    return CACHE_DIR / "region" / region_id / field_id / cycle


def region_image_path(region_id, field_id, cycle, hour):
    return region_cache_dir(region_id, field_id, cycle) / f"F{int(hour):03d}.png"


def region_meta_path(region_id, field_id, cycle):
    return region_cache_dir(region_id, field_id, cycle) / "meta.json"


def read_region_meta(region_id, field_id, cycle):
    path = region_meta_path(region_id, field_id, cycle)
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_region_meta(region_id, field_id, cycle, payload):
    path = region_meta_path(region_id, field_id, cycle)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(temporary, path)


def cache_dir(field_id, domain, cycle):
    return CACHE_DIR / field_id / domain / cycle


def image_path(field_id, domain, cycle, hour):
    return cache_dir(field_id, domain, cycle) / f"F{int(hour):03d}.png"


def meta_path(field_id, domain, cycle):
    return cache_dir(field_id, domain, cycle) / "meta.json"


def read_meta(field_id, domain, cycle):
    path = meta_path(field_id, domain, cycle)
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_meta(field_id, domain, cycle, payload):
    path = meta_path(field_id, domain, cycle)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(temporary, path)


def field_job_key(field_id, domain, cycle):
    return (field_id, domain, cycle)


def file_lock(path):
    key = str(path)
    with _file_locks_guard:
        lock = _file_locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _file_locks[key] = lock
        return lock


def _parse_mtime(raw):
    if not raw:
        return None
    moment = parsedate_to_datetime(raw)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def head_mtime(url):
    """Return Last-Modified as ISO UTC, or None if the image is missing.

    Tries HEAD first; if the server rejects HEAD or omits Last-Modified,
    falls back to a ranged GET.
    """
    last_error = None
    for attempt in range(3):
        request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                mtime = _parse_mtime(response.headers.get("Last-Modified"))
                if mtime:
                    return mtime
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            # Some servers answer 405/501 for HEAD; try GET below.
            if exc.code not in (405, 501):
                last_error = exc
        except urllib.error.URLError as exc:
            last_error = exc

        get_request = urllib.request.Request(
            url,
            headers={"User-Agent": USER_AGENT, "Range": "bytes=0-0"},
        )
        try:
            with urllib.request.urlopen(get_request, timeout=20) as response:
                mtime = _parse_mtime(response.headers.get("Last-Modified"))
                if mtime:
                    return mtime
                # Object exists but has no Last-Modified; treat as available now.
                if response.status in (200, 206):
                    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            last_error = exc
        except urllib.error.URLError as exc:
            last_error = exc

        if attempt < 2:
            time.sleep(0.4)
    if last_error:
        raise last_error
    return None


def sentinel_hours(field):
    hours = list(field["hours"])
    preferred = []
    for value in (hours[-1], 48, 24, 12, 6, 1, 0, hours[0]):
        if value in hours and value not in preferred:
            preferred.append(value)
    for value in reversed(hours):
        if value not in preferred:
            preferred.append(value)
    return preferred


def probe_field_cycle(config, field, cycle, domain):
    last_error = None
    last_url = None
    for hour in sentinel_hours(field):
        url = remote_url(config, field, cycle, hour, domain)
        last_url = url
        try:
            mtime = head_mtime(url)
        except Exception as exc:
            last_error = str(exc)
            continue
        if mtime:
            return {
                "cycle": cycle,
                "mtime": mtime,
                "available": True,
                "probe_url": url,
                "probe_hour": hour,
            }
    payload = {
        "cycle": cycle,
        "mtime": None,
        "available": False,
        "probe_url": last_url,
    }
    if last_error:
        payload["error"] = last_error
    return payload


def download(url, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".part")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=60) as response, temporary.open("wb") as output:
            shutil.copyfileobj(response, output)
        os.replace(temporary, destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def ensure_image(config, field, cycle, hour, domain):
    path = image_path(field["id"], domain, cycle, hour)
    lock = file_lock(path)
    with lock:
        if path.exists() and path.stat().st_size > 0:
            return path
        download(remote_url(config, field, cycle, hour, domain), path)
        return path


def cache_is_fresh(field, domain, cycle, mtime):
    meta = read_meta(field["id"], domain, cycle)
    if not meta or meta.get("mtime") != mtime or not meta.get("complete"):
        return False
    return all(image_path(field["id"], domain, cycle, hour).exists() for hour in field["hours"])


def clear_images(field_id, domain, cycle):
    directory = cache_dir(field_id, domain, cycle)
    if not directory.exists():
        return
    for path in directory.glob("F*.png"):
        path.unlink(missing_ok=True)


def job_snapshot(job):
    return {
        "running": job["running"],
        "complete": job["complete"],
        "done": job["done"],
        "total": job["total"],
        "mtime": job["mtime"],
        "error": job["error"],
    }


def start_cache(config, field, cycle, domain):
    if cycle not in config["cycles"]:
        raise ValueError("Ciclo desconocido")
    if normalize_domain(config, domain) is None:
        raise ValueError("Dominio desconocido")
    key = field_job_key(field["id"], domain, cycle)
    with _jobs_lock:
        current = _jobs.get(key)
        if current and current["running"]:
            return job_snapshot(current)
        job = {
            "running": True,
            "complete": False,
            "done": 0,
            "total": len(field["hours"]),
            "mtime": None,
            "error": None,
            "ready": threading.Event(),
        }
        _jobs[key] = job
    threading.Thread(
        target=_fill_cache,
        args=(config, field, cycle, domain, job),
        daemon=True,
    ).start()
    return job_snapshot(job)


def cache_status(field_id, domain, cycle):
    with _jobs_lock:
        job = _jobs.get(field_job_key(field_id, domain, cycle))
        if job:
            return job_snapshot(job)
    return {
        "running": False,
        "complete": False,
        "done": 0,
        "total": 0,
        "mtime": None,
        "error": None,
    }


def wait_until_ready(field_id, domain, cycle, timeout=90):
    with _jobs_lock:
        job = _jobs.get(field_job_key(field_id, domain, cycle))
    if job is None:
        return None
    job["ready"].wait(timeout)
    return job


def _fill_cache(config, field, cycle, domain, job):
    fresh = False
    try:
        sentinel = field["hours"][-1]
        mtime = head_mtime(remote_url(config, field, cycle, sentinel, domain))
        if mtime is None:
            raise FileNotFoundError("El ciclo no tiene la última imagen publicada")
        job["mtime"] = mtime
        fresh = cache_is_fresh(field, domain, cycle, mtime)
        if not fresh:
            meta = read_meta(field["id"], domain, cycle)
            if not meta or meta.get("mtime") != mtime:
                clear_images(field["id"], domain, cycle)
            write_meta(field["id"], domain, cycle, {"mtime": mtime, "complete": False})
    except Exception as exc:
        job["error"] = str(exc)
        job["running"] = False
        job["ready"].set()
        return

    job["ready"].set()
    if fresh:
        job["done"] = job["total"]
        job["complete"] = True
        job["running"] = False
        return

    def fetch(hour):
        try:
            ensure_image(config, field, cycle, hour, domain)
            return True
        except Exception as exc:
            print(f"cache {field['id']} {domain} {cycle} F{hour:03d}: {exc}", flush=True)
            return False

    try:
        failures = 0
        with ThreadPoolExecutor(max_workers=6) as pool:
            for ok in pool.map(fetch, field["hours"]):
                with _jobs_lock:
                    job["done"] += 1
                if not ok:
                    failures += 1
        if failures:
            job["error"] = f"No se pudieron guardar {failures} imágenes"
            return
        write_meta(field["id"], domain, cycle, {"mtime": job["mtime"], "complete": True})
        job["complete"] = True
        print(f"cache lista {field['id']} {domain} {cycle}UTC ({job['total']} imágenes)", flush=True)
    finally:
        job["running"] = False


def fold(text):
    normalized = unicodedata.normalize("NFD", text or "")
    without = "".join(char for char in normalized if unicodedata.category(char) != "Mn")
    return re.sub(r"[^a-z0-9]+", " ", without.lower()).strip()


def load_stations():
    groups = {}
    with STATIONS_PATH.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            cipi = (row.get("CIPI") or "").strip().upper()
            if not CIPI_RE.match(cipi):
                continue
            place = groups.setdefault(cipi, {"cipi": cipi, "oaci": "", "names": []})
            oaci = (row.get("oaci") or "").strip().upper()
            if re.fullmatch(r"[A-Z]{4}", oaci):
                place["oaci"] = oaci
            place["names"].append({
                "tipo": (row.get("tipo") or "").strip(),
                "name": (row.get("desc") or "").strip() or (row.get("nombre") or "").strip(),
            })
    return list(groups.values())


def station_public(place, name):
    return {"cipi": place["cipi"], "oaci": place["oaci"], "name": name}


def search_stations(query):
    folded = fold(query)
    if len(folded) < 2:
        return []
    upper = query.strip().upper()
    tipo_rank = {"ESTACION": 0, "WEB": 1, "PIMET": 2, "TURISTICA": 3, "INTRAMET": 4}
    results = []
    for place in load_stations():
        best = None
        for entry in place["names"]:
            name = entry["name"]
            folded_name = fold(name)
            score = 0
            if place["oaci"] and place["oaci"] == upper:
                score = 100
            elif place["cipi"] == upper:
                score = 90
            elif folded_name == folded:
                score = 80
            elif folded_name.startswith(folded):
                score = 70
            elif folded and folded in folded_name:
                score = 60
            if score and (best is None or score > best[0] or (score == best[0] and tipo_rank.get(entry["tipo"], 9) < best[2])):
                best = (score, name, tipo_rank.get(entry["tipo"], 9))
        if best:
            results.append((best[0], best[2], station_public(place, best[1])))
    results.sort(key=lambda item: (-item[0], item[1], item[2]["name"]))
    return [item[2] for item in results[:15]]


def resolve_station(code):
    matches = search_stations(code)
    upper = code.strip().upper()
    folded = fold(code)
    for station in matches:
        if station["oaci"] == upper or station["cipi"] == upper or fold(station["name"]) == folded:
            return station
    return matches[0] if len(matches) == 1 else None


def meteogram_url(config, cipi, cycle):
    return f"{config['base_url']}{METEOGRAM_PRODUCT}_{cipi}.{cycle}UTC.png"


def meteogram_file(cipi, cycle):
    return CACHE_DIR / "meteogram" / cipi / f"{cycle}.png"


def meteogram_meta_file(cipi, cycle):
    return CACHE_DIR / "meteogram" / cipi / f"{cycle}.json"


def read_json(path):
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(temporary, path)


def meteogram_key(cipi, cycle):
    return ("meteogram", cipi, cycle)


def job_status(key):
    with _jobs_lock:
        job = _jobs.get(key)
        if job:
            return job_snapshot(job)
    return {
        "running": False,
        "complete": False,
        "done": 0,
        "total": 0,
        "mtime": None,
        "error": None,
    }


def wait_job(key, timeout=90):
    with _jobs_lock:
        job = _jobs.get(key)
    if job is None:
        return None
    job["ready"].wait(timeout)
    return job


def start_meteogram_cache(config, cipi, cycle):
    if cycle not in config["cycles"] or not CIPI_RE.match(cipi):
        raise ValueError("Estación o ciclo desconocido")
    key = meteogram_key(cipi, cycle)
    with _jobs_lock:
        current = _jobs.get(key)
        if current and current["running"]:
            return job_snapshot(current)
        job = {
            "running": True,
            "complete": False,
            "done": 0,
            "total": 1,
            "mtime": None,
            "error": None,
            "ready": threading.Event(),
        }
        _jobs[key] = job
    threading.Thread(
        target=_fill_meteogram,
        args=(config, cipi, cycle, job),
        daemon=True,
    ).start()
    return job_snapshot(job)


def _fill_meteogram(config, cipi, cycle, job):
    path = meteogram_file(cipi, cycle)
    meta_path = meteogram_meta_file(cipi, cycle)
    fresh = False
    try:
        mtime = head_mtime(meteogram_url(config, cipi, cycle))
        if mtime is None:
            raise FileNotFoundError("Ese ciclo no tiene meteograma publicado")
        job["mtime"] = mtime
        meta = read_json(meta_path)
        fresh = bool(meta and meta.get("mtime") == mtime and meta.get("complete") and path.exists())
        if not fresh:
            if not meta or meta.get("mtime") != mtime:
                path.unlink(missing_ok=True)
            write_json(meta_path, {"mtime": mtime, "complete": False})
    except Exception as exc:
        job["error"] = str(exc)
        job["running"] = False
        job["ready"].set()
        return

    job["ready"].set()
    if fresh:
        job["done"] = 1
        job["complete"] = True
        job["running"] = False
        return
    try:
        lock = file_lock(path)
        with lock:
            if not path.exists() or path.stat().st_size == 0:
                download(meteogram_url(config, cipi, cycle), path)
        write_json(meta_path, {"mtime": job["mtime"], "complete": True})
        job["done"] = 1
        job["complete"] = True
    except Exception as exc:
        job["error"] = str(exc)
    finally:
        job["running"] = False


def meteogram_cycles(config, cipi):
    now = time.monotonic()
    cache_key = f"met:{cipi}"
    with _status_lock:
        cached = _status_cache.get(cache_key)
        if cached and now - cached[0] < STATUS_TTL_SECONDS:
            return cached[1]

    def one(cycle):
        try:
            mtime = head_mtime(meteogram_url(config, cipi, cycle))
        except Exception as exc:
            return {"cycle": cycle, "mtime": None, "available": False, "error": str(exc)}
        return {"cycle": cycle, "mtime": mtime, "available": mtime is not None}

    with ThreadPoolExecutor(max_workers=4) as pool:
        cycles = list(pool.map(one, config["cycles"]))
    available = [cycle for cycle in cycles if cycle["available"]]
    vigente = max(available, key=lambda cycle: cycle["mtime"])["cycle"] if available else None
    payload = {"vigente": vigente, "cycles": cycles}
    with _status_lock:
        _status_cache[cache_key] = (now, payload)
    return payload


def cycle_report(config, field, domain):
    cache_key = f"{field['id']}:{domain}"
    now = time.monotonic()
    with _status_lock:
        cached = _status_cache.get(cache_key)
        if cached and now - cached[0] < STATUS_TTL_SECONDS:
            return cached[1]

    def one(cycle):
        return probe_field_cycle(config, field, cycle, domain)

    with ThreadPoolExecutor(max_workers=4) as pool:
        cycles = list(pool.map(one, config["cycles"]))

    available = [cycle for cycle in cycles if cycle["available"]]
    vigente = max(available, key=lambda cycle: cycle["mtime"])["cycle"] if available else None
    sample_hour = sentinel_hours(field)[0]
    product = field_product(field, domain)
    payload = {
        "vigente": vigente,
        "cycles": cycles,
        "domain": domain,
        "product": product,
        "sample_url": remote_url(config, field, "00", sample_hour, domain),
    }
    with _status_lock:
        _status_cache[cache_key] = (now, payload)
    return payload


def probe_region_cycle(config, region, field, cycle):
    last_error = None
    last_url = None
    for hour in sentinel_hours(field):
        url = region_remote_url(config, region, field, cycle, hour)
        last_url = url
        try:
            mtime = head_mtime(url)
        except Exception as exc:
            last_error = str(exc)
            continue
        if mtime:
            return {
                "cycle": cycle,
                "mtime": mtime,
                "available": True,
                "probe_url": url,
                "probe_hour": hour,
            }
    payload = {
        "cycle": cycle,
        "mtime": None,
        "available": False,
        "probe_url": last_url,
    }
    if last_error:
        payload["error"] = last_error
    return payload


def region_cycle_report(config, region, field):
    cache_key = f"region:{region['id']}:{field['id']}"
    now = time.monotonic()
    with _status_lock:
        cached = _status_cache.get(cache_key)
        if cached and now - cached[0] < STATUS_TTL_SECONDS:
            return cached[1]

    def one(cycle):
        return probe_region_cycle(config, region, field, cycle)

    with ThreadPoolExecutor(max_workers=4) as pool:
        cycles = list(pool.map(one, config["cycles"]))

    available = [cycle for cycle in cycles if cycle["available"]]
    vigente = max(available, key=lambda cycle: cycle["mtime"])["cycle"] if available else None
    sample_hour = sentinel_hours(field)[0]
    payload = {
        "vigente": vigente,
        "cycles": cycles,
        "sample_url": region_remote_url(config, region, field, "00", sample_hour),
    }
    with _status_lock:
        _status_cache[cache_key] = (now, payload)
    return payload


def region_cache_is_fresh(region, field, cycle, mtime):
    meta = read_region_meta(region["id"], field["id"], cycle)
    if not meta or meta.get("mtime") != mtime or not meta.get("complete"):
        return False
    return all(region_image_path(region["id"], field["id"], cycle, hour).exists() for hour in field["hours"])


def clear_region_images(region_id, field_id, cycle):
    directory = region_cache_dir(region_id, field_id, cycle)
    if not directory.exists():
        return
    for path in directory.glob("F*.png"):
        path.unlink(missing_ok=True)


def region_job_key(region_id, field_id, cycle):
    return ("region", region_id, field_id, cycle)


def start_region_cache(config, region, field, cycle):
    if cycle not in config["cycles"]:
        raise ValueError("Ciclo desconocido")
    key = region_job_key(region["id"], field["id"], cycle)
    with _jobs_lock:
        current = _jobs.get(key)
        if current and current["running"]:
            return job_snapshot(current)
        job = {
            "running": True,
            "complete": False,
            "done": 0,
            "total": len(field["hours"]),
            "mtime": None,
            "error": None,
            "ready": threading.Event(),
        }
        _jobs[key] = job
    threading.Thread(
        target=_fill_region_cache,
        args=(config, region, field, cycle, job),
        daemon=True,
    ).start()
    return job_snapshot(job)


def region_cache_status(region_id, field_id, cycle):
    with _jobs_lock:
        job = _jobs.get(region_job_key(region_id, field_id, cycle))
        if job:
            return job_snapshot(job)
    return {
        "running": False,
        "complete": False,
        "done": 0,
        "total": 0,
        "mtime": None,
        "error": None,
    }


def wait_region_ready(region_id, field_id, cycle, timeout=90):
    with _jobs_lock:
        job = _jobs.get(region_job_key(region_id, field_id, cycle))
    if job is None:
        return None
    job["ready"].wait(timeout)
    return job


def _fill_region_cache(config, region, field, cycle, job):
    fresh = False
    try:
        sentinel = field["hours"][-1]
        mtime = head_mtime(region_remote_url(config, region, field, cycle, sentinel))
        if mtime is None:
            raise FileNotFoundError("El ciclo no tiene la última imagen publicada")
        job["mtime"] = mtime
        fresh = region_cache_is_fresh(region, field, cycle, mtime)
        if not fresh:
            meta = read_region_meta(region["id"], field["id"], cycle)
            if not meta or meta.get("mtime") != mtime:
                clear_region_images(region["id"], field["id"], cycle)
            write_region_meta(region["id"], field["id"], cycle, {"mtime": mtime, "complete": False})
    except Exception as exc:
        job["error"] = str(exc)
        job["running"] = False
        job["ready"].set()
        return

    job["ready"].set()
    if fresh:
        job["done"] = job["total"]
        job["complete"] = True
        job["running"] = False
        return

    def fetch(hour):
        try:
            ensure_region_image(config, region, field, cycle, hour)
            return True
        except Exception as exc:
            print(f"cache region {region['id']} {field['id']} {cycle} F{hour:03d}: {exc}", flush=True)
            return False

    try:
        failures = 0
        with ThreadPoolExecutor(max_workers=6) as pool:
            for ok in pool.map(fetch, field["hours"]):
                with _jobs_lock:
                    job["done"] += 1
                if not ok:
                    failures += 1
        if failures:
            job["error"] = f"No se pudieron guardar {failures} imágenes"
            return
        write_region_meta(region["id"], field["id"], cycle, {"mtime": job["mtime"], "complete": True})
        job["complete"] = True
        print(f"cache lista region {region['id']} {field['id']} {cycle}UTC ({job['total']} imágenes)", flush=True)
    finally:
        job["running"] = False


def ensure_region_image(config, region, field, cycle, hour):
    path = region_image_path(region["id"], field["id"], cycle, hour)
    lock = file_lock(path)
    with lock:
        if path.exists() and path.stat().st_size > 0:
            return path
        download(region_remote_url(config, region, field, cycle, hour), path)
        return path


def savipa_config(config):
    return config.get("savipa") or {
        "base_url": "http://dma.smn.gob.ar:8007/aeronautica/KWBC/",
        "firs": [{"id": code, "name": code} for code in ("CBA", "CRV", "DOZ", "EZE", "SIS")],
        "fields": [
            {"id": "TURB", "name": "Turbulencia"},
            {"id": "ICE", "name": "Hielo"},
        ],
    }


def savipa_normalize_fir(config, fir):
    value = (fir or "").strip().upper()
    allowed = {item["id"].upper() for item in savipa_config(config).get("firs", [])}
    if not SAVIPA_FIR_RE.match(value) or (allowed and value not in allowed):
        return None
    return value


def savipa_normalize_field(config, field):
    value = (field or "").strip().upper()
    allowed = {item["id"].upper() for item in savipa_config(config).get("fields", [])}
    if not SAVIPA_FIELD_RE.match(value) or (allowed and value not in allowed):
        return None
    return value


def savipa_dir_url(config, fir, field):
    base = savipa_config(config)["base_url"]
    if not base.endswith("/"):
        base += "/"
    return f"{base}{fir}/{field}/"


def savipa_remote_url(config, fir, field, level, forecast_time):
    return (
        savipa_dir_url(config, fir, field)
        + f"{fir}_{field}{int(level):03d}_{int(forecast_time):02d}.png"
    )


def savipa_cache_path(fir, field, level, forecast_time):
    return (
        CACHE_DIR
        / "savipa"
        / fir
        / field
        / f"{int(level):03d}"
        / f"{int(forecast_time):02d}.png"
    )


def fetch_text(url, timeout=30):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def parse_savipa_listing(html, fir, field):
    levels = set()
    times = set()
    files_by_key = {}
    text = html or ""
    for match in SAVIPA_FILE_RE.finditer(text):
        item_fir = match.group("fir").upper()
        item_field = match.group("field").upper()
        if item_fir != fir or item_field != field:
            continue
        level = int(match.group("level"))
        forecast_time = int(match.group("time"))
        levels.add(level)
        times.add(forecast_time)
        key = (level, forecast_time)
        files_by_key[key] = {
            "fir": item_fir,
            "field": item_field,
            "level": level,
            "time": forecast_time,
            "name": match.group(0),
        }
    if not levels:
        for match in SAVIPA_LEVEL_RE.finditer(text):
            levels.add(int(match.group(1)))
    if not times:
        for match in re.finditer(rf"{re.escape(field)}\d{{3}}_(\d{{2}})\.png", text, re.IGNORECASE):
            times.add(int(match.group(1)))
        if not times:
            for match in re.finditer(r"_(\d{2})\.png", text, re.IGNORECASE):
                times.add(int(match.group(1)))
    files = sorted(files_by_key.values(), key=lambda item: (item["level"], item["time"]))
    return {
        "levels": sorted(levels),
        "times": sorted(times),
        "files": files,
    }


def discover_savipa(config, fir, field, force=False):
    cache_key = f"savipa:{fir}:{field}"
    now = time.monotonic()
    if not force:
        with _status_lock:
            cached = _status_cache.get(cache_key)
            if cached and now - cached[0] < SAVIPA_LIST_TTL_SECONDS:
                return cached[1]

    url = savipa_dir_url(config, fir, field)
    try:
        html = fetch_text(url)
        parsed = parse_savipa_listing(html, fir, field)
        files = [
            {"level": item["level"], "time": item["time"]}
            for item in parsed["files"]
        ]
        if not files and parsed["levels"] and parsed["times"]:
            files = [
                {"level": level, "time": forecast_time}
                for level in parsed["levels"]
                for forecast_time in parsed["times"]
            ]
        payload = {
            "fir": fir,
            "field": field,
            "source_url": url,
            "levels": parsed["levels"],
            "times": parsed["times"],
            "files": files,
            "count": len(files),
            "error": None,
        }
        if not payload["levels"] or not payload["times"]:
            payload["error"] = "No encontré niveles o tiempos en el listado SAVIPA"
    except Exception as exc:
        payload = {
            "fir": fir,
            "field": field,
            "source_url": url,
            "levels": [],
            "times": [],
            "files": [],
            "count": 0,
            "error": str(exc),
        }

    with _status_lock:
        _status_cache[cache_key] = (now, payload)
    return payload


def savipa_meta_path(fir, field):
    return CACHE_DIR / "savipa" / fir / field / "meta.json"


def clear_savipa_images(fir, field):
    directory = CACHE_DIR / "savipa" / fir / field
    if not directory.exists():
        return
    for path in directory.rglob("*.png"):
        path.unlink(missing_ok=True)
    meta = savipa_meta_path(fir, field)
    meta.unlink(missing_ok=True)


def savipa_job_key(fir, field):
    return ("savipa", fir, field)


def savipa_files_cached(fir, field, files):
    if not files:
        return False
    return all(
        savipa_cache_path(fir, field, item["level"], item["time"]).exists()
        for item in files
    )


def savipa_cache_is_fresh(fir, field, files):
    meta = read_json(savipa_meta_path(fir, field))
    if not meta or not meta.get("complete"):
        return False
    if meta.get("total") not in (None, len(files)):
        return False
    return savipa_files_cached(fir, field, files)


def ensure_savipa_image(config, fir, field, level, forecast_time, force=False):
    path = savipa_cache_path(fir, field, level, forecast_time)
    lock = file_lock(path)
    with lock:
        if not force and path.exists() and path.stat().st_size > 0:
            return path
        if force and path.exists():
            path.unlink(missing_ok=True)
        download(savipa_remote_url(config, fir, field, level, forecast_time), path)
        return path


def start_savipa_cache(config, fir, field, force=False):
    key = savipa_job_key(fir, field)
    with _jobs_lock:
        current = _jobs.get(key)
        if current and current["running"]:
            return job_snapshot(current)
        job = {
            "running": True,
            "complete": False,
            "done": 0,
            "total": 0,
            "mtime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "error": None,
            "force": bool(force),
            "ready": threading.Event(),
        }
        _jobs[key] = job
    threading.Thread(
        target=_fill_savipa_cache,
        args=(config, fir, field, force, job),
        daemon=True,
    ).start()
    return job_snapshot(job)


def savipa_cache_status(fir, field):
    with _jobs_lock:
        job = _jobs.get(savipa_job_key(fir, field))
        if job:
            return job_snapshot(job)
    meta = read_json(savipa_meta_path(fir, field))
    if meta and meta.get("complete"):
        return {
            "running": False,
            "complete": True,
            "done": meta.get("total", 0),
            "total": meta.get("total", 0),
            "mtime": meta.get("mtime"),
            "error": None,
        }
    return {
        "running": False,
        "complete": False,
        "done": 0,
        "total": 0,
        "mtime": None,
        "error": None,
    }


def _fill_savipa_cache(config, fir, field, force, job):
    try:
        if force:
            with _status_lock:
                _status_cache.pop(f"savipa:{fir}:{field}", None)
            clear_savipa_images(fir, field)

        discovery = discover_savipa(config, fir, field, force=force)
        if discovery.get("error") and not discovery.get("files"):
            raise FileNotFoundError(discovery["error"])

        files = discovery.get("files") or []
        if not files:
            files = [
                {"level": level, "time": forecast_time}
                for level in discovery.get("levels", [])
                for forecast_time in discovery.get("times", [])
            ]
        job["total"] = len(files)
        if not force and savipa_cache_is_fresh(fir, field, files):
            meta = read_json(savipa_meta_path(fir, field)) or {}
            job["mtime"] = meta.get("mtime") or job["mtime"]
            job["done"] = job["total"]
            job["complete"] = True
            job["running"] = False
            job["ready"].set()
            return

        job["mtime"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        write_json(
            savipa_meta_path(fir, field),
            {
                "mtime": job["mtime"],
                "complete": False,
                "total": job["total"],
                "force": bool(force),
            },
        )
    except Exception as exc:
        job["error"] = str(exc)
        job["running"] = False
        job["ready"].set()
        return

    job["ready"].set()

    def fetch(item):
        try:
            ensure_savipa_image(
                config,
                fir,
                field,
                item["level"],
                item["time"],
                force=force,
            )
            return True
        except Exception as exc:
            print(
                f"cache savipa {fir} {field} {item['level']:03d}_{item['time']:02d}: {exc}",
                flush=True,
            )
            return False

    try:
        failures = 0
        with ThreadPoolExecutor(max_workers=8) as pool:
            for ok in pool.map(fetch, files):
                with _jobs_lock:
                    job["done"] += 1
                if not ok:
                    failures += 1
        if failures:
            job["error"] = f"No se pudieron guardar {failures} imágenes SAVIPA"
            return
        write_json(
            savipa_meta_path(fir, field),
            {
                "mtime": job["mtime"],
                "complete": True,
                "total": job["total"],
                "force": bool(force),
            },
        )
        job["complete"] = True
        print(
            f"cache lista savipa {fir} {field} ({job['total']} imágenes)",
            flush=True,
        )
    finally:
        job["running"] = False


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)
        query = urllib.parse.parse_qs(parsed.query)
        if (
            path in ("/", "/index.html", "/campos")
            or path == "/meteogramas" or path.startswith("/meteogramas/")
            or path == "/regiones" or path.startswith("/regiones/")
            or path == "/savipa" or path.startswith("/savipa/")
        ):
            self._send_file(ROOT / "index.html", "text/html; charset=utf-8")
            return
        if path == "/api/fields":
            config = load_config()
            self._send_json({
                "fields": [
                    {"id": field["id"], "name": field["name"]}
                    for field in config.get("fields", [])
                ],
                "domains": field_domains(config),
                "default_domain": default_domain(config),
            })
            return
        if path == "/api/status":
            self._handle_status(query)
            return
        if path == "/api/cache":
            config = load_config()
            field_id = _one(query, "field")
            cycle = _one(query, "cycle")
            domain = normalize_domain(config, _one(query, "domain")) or default_domain(config)
            self._send_json(cache_status(field_id, domain, cycle))
            return
        if path == "/api/stations":
            self._send_json({"stations": search_stations(_one(query, "q"))})
            return
        if path == "/api/stations/resolve":
            station = resolve_station(_one(query, "code"))
            if station is None:
                self._send_json({"error": "No encontré esa estación"}, 404)
                return
            self._send_json(station)
            return
        if path == "/api/meteogram/status":
            self._handle_meteogram_status(query)
            return
        if path == "/api/meteogram/cache":
            cipi = _one(query, "cipi").upper()
            cycle = _one(query, "cycle")
            self._send_json(job_status(meteogram_key(cipi, cycle)))
            return
        if path == "/api/region/config":
            self._handle_region_config()
            return
        if path == "/api/region/status":
            self._handle_region_status(query)
            return
        if path == "/api/region/cache":
            region_id = _one(query, "region").upper()
            field_id = _one(query, "field")
            cycle = _one(query, "cycle")
            self._send_json(region_cache_status(region_id, field_id, cycle))
            return
        if path == "/api/debug/probe":
            self._handle_debug_probe(query)
            return
        if path == "/api/savipa/config":
            self._handle_savipa_config()
            return
        if path == "/api/savipa/status":
            self._handle_savipa_status(query)
            return
        if path == "/api/savipa/cache":
            config = load_config()
            fir = savipa_normalize_fir(config, _one(query, "fir"))
            field = savipa_normalize_field(config, _one(query, "field"))
            if fir is None or field is None:
                self.send_error(400, "FIR o campo SAVIPA desconocido")
                return
            self._send_json(savipa_cache_status(fir, field))
            return
        if path.startswith("/img/"):
            self._handle_image(path)
            return
        self.send_error(404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/api/cache":
            self._handle_cache(query)
            return
        if parsed.path == "/api/meteogram/cache":
            self._handle_meteogram_cache(query)
            return
        if parsed.path == "/api/region/cache":
            self._handle_region_cache(query)
            return
        if parsed.path == "/api/savipa/cache":
            self._handle_savipa_cache(query)
            return
        self.send_error(404)

    def _handle_status(self, query):
        config = load_config()
        field = field_by_id(config, _one(query, "field"))
        domain = normalize_domain(config, _one(query, "domain"))
        if field is None:
            self.send_error(404, "Campo desconocido")
            return
        if domain is None:
            self.send_error(400, "Dominio desconocido")
            return
        report = cycle_report(config, field, domain)
        product = field_product(field, domain)
        self._send_json({
            "base_url": config["base_url"],
            "domain": domain,
            "domains": field_domains(config),
            "field": {
                "id": field["id"],
                "name": field["name"],
                "product": product,
                "product_template": field["product"],
                "hours": field["hours"],
                "step": field["step"],
            },
            "vigente": report["vigente"],
            "cycles": report["cycles"],
            "sample_url": report.get("sample_url"),
            "product": product,
        })

    def _handle_cache(self, query):
        config = load_config()
        field = field_by_id(config, _one(query, "field"))
        cycle = _one(query, "cycle")
        domain = normalize_domain(config, _one(query, "domain"))
        if field is None or cycle not in config["cycles"] or domain is None:
            self.send_error(400, "Campo, dominio o ciclo desconocido")
            return
        self._send_json(start_cache(config, field, cycle, domain))

    def _handle_meteogram_status(self, query):
        config = load_config()
        cipi = _one(query, "cipi").upper()
        if not CIPI_RE.match(cipi):
            self.send_error(400, "Estación desconocida")
            return
        self._send_json(meteogram_cycles(config, cipi))

    def _handle_meteogram_cache(self, query):
        config = load_config()
        cipi = _one(query, "cipi").upper()
        cycle = _one(query, "cycle")
        if not CIPI_RE.match(cipi) or cycle not in config["cycles"]:
            self.send_error(400, "Estación o ciclo desconocido")
            return
        self._send_json(start_meteogram_cache(config, cipi, cycle))

    def _handle_region_config(self):
        config = load_config()
        self._send_json({
            "regions": [
                {"id": r["id"], "name": r["name"], "stations": r["stations"]}
                for r in config.get("regions", [])
            ],
            "fields": [
                {"id": f["id"], "name": f["name"]}
                for f in config.get("region_fields", [])
            ]
        })

    def _handle_region_status(self, query):
        config = load_config()
        region_id = _one(query, "region").upper()
        field_id = _one(query, "field")
        region = region_by_id(config, region_id)
        field = region_field_by_id(config, field_id)
        if region is None:
            self.send_error(404, "Región desconocida")
            return
        if field is None:
            self.send_error(404, "Campo desconocido")
            return
        report = region_cycle_report(config, region, field)
        self._send_json({
            "base_url": config["base_url"],
            "region": {
                "id": region["id"],
                "name": region["name"],
                "stations": region["stations"],
            },
            "field": {
                "id": field["id"],
                "name": field["name"],
                "product_template": field["product_template"],
                "hours": field["hours"],
                "step": field["step"],
            },
            "vigente": report["vigente"],
            "cycles": report["cycles"],
        })

    def _handle_region_cache(self, query):
        config = load_config()
        region_id = _one(query, "region").upper()
        field_id = _one(query, "field")
        cycle = _one(query, "cycle")
        region = region_by_id(config, region_id)
        field = region_field_by_id(config, field_id)
        if region is None or field is None or cycle not in config["cycles"]:
            self.send_error(400, "Región, campo o ciclo desconocido")
            return
        self._send_json(start_region_cache(config, region, field, cycle))

    def _handle_debug_probe(self, query):
        """Probe likely product URL variants against repo2 (needs VPN)."""
        config = load_config()
        field_id = _one(query, "field") or "pp01h"
        cycle = _one(query, "cycle") or "00"
        hour = int(_one(query, "hour") or "24")
        domain = normalize_domain(config, _one(query, "domain")) or default_domain(config)
        field = field_by_id(config, field_id)
        raw = field_product(field, domain) if field else f"WRF_ARG4K.FCST_FIELD_PP01H_SLP_U10_V10_{domain}"
        key = raw
        if key.startswith("WRF_ARG4K.FCST_FIELD_") and key.endswith(f"_{domain}"):
            key = key[len("WRF_ARG4K.FCST_FIELD_"):-(len(domain) + 1)]
        elif key.startswith("WRF_ARG4K.FCST_") and key.endswith(f"_{domain}"):
            key = key[len("WRF_ARG4K.FCST_"):-(len(domain) + 1)]
        stems = [
            raw,
            f"WRF_ARG4K.FCST_FIELD_{key}_{domain}",
            f"WRF_ARG4K.FCST_{key}_{domain}",
            f"WRF_ARG4K.FCST_CRTM_{key}_{domain}" if "CRTM" not in key else raw,
            "WRF_ARG4K.FCST_REGION_PP01H_U10_V10_R01",
        ]
        seen = set()
        unique = []
        for stem in stems:
            if stem not in seen:
                seen.add(stem)
                unique.append(stem)

        results = []
        for stem in unique:
            url = f"{config['base_url']}{stem}.{cycle}UTC.F{hour:03d}.png"
            entry = {"stem": stem, "url": url}
            try:
                mtime = head_mtime(url)
                entry["ok"] = mtime is not None
                entry["mtime"] = mtime
            except Exception as exc:
                entry["ok"] = False
                entry["error"] = str(exc)
            results.append(entry)
            print(f"probe {stem}: ok={entry.get('ok')} err={entry.get('error')} mtime={entry.get('mtime')}", flush=True)

        self._send_json({
            "field": field_id,
            "cycle": cycle,
            "hour": hour,
            "results": results,
            "first_ok": next((item for item in results if item.get("ok")), None),
        })

    def _handle_savipa_config(self):
        config = load_config()
        savipa = savipa_config(config)
        self._send_json({
            "base_url": savipa.get("base_url"),
            "firs": savipa.get("firs", []),
            "fields": savipa.get("fields", []),
        })

    def _handle_savipa_status(self, query):
        config = load_config()
        fir = savipa_normalize_fir(config, _one(query, "fir"))
        field = savipa_normalize_field(config, _one(query, "field"))
        force = _one(query, "force") in ("1", "true", "yes")
        if fir is None or field is None:
            self.send_error(400, "FIR o campo SAVIPA desconocido")
            return
        self._send_json(discover_savipa(config, fir, field, force=force))

    def _handle_savipa_cache(self, query):
        config = load_config()
        fir = savipa_normalize_fir(config, _one(query, "fir"))
        field = savipa_normalize_field(config, _one(query, "field"))
        force = _one(query, "force") in ("1", "true", "yes")
        if fir is None or field is None:
            self.send_error(400, "FIR o campo SAVIPA desconocido")
            return
        self._send_json(start_savipa_cache(config, fir, field, force=force))

    def _handle_image(self, path):
        parts = path.strip("/").split("/")
        if len(parts) == 4 and parts[1] == "meteogram":
            self._handle_meteogram_image(parts[2].upper(), parts[3])
            return
        if len(parts) == 6 and parts[1] == "region":
            self._handle_region_image(parts[2].upper(), parts[3], parts[4], parts[5])
            return
        if len(parts) == 6 and parts[1] == "savipa":
            self._handle_savipa_image(parts[2].upper(), parts[3].upper(), parts[4], parts[5])
            return
        config = load_config()
        # New: /img/{field}/{domain}/{cycle}/{hour}
        # Legacy fallback: /img/{field}/{cycle}/{hour} -> domain ARG
        if len(parts) == 5:
            _, field_id, domain_text, cycle, hour_text = parts
            domain = normalize_domain(config, domain_text)
        elif len(parts) == 4:
            _, field_id, cycle, hour_text = parts
            domain = default_domain(config)
        else:
            self.send_error(404)
            return
        field = field_by_id(config, field_id)
        try:
            hour = int(hour_text)
        except ValueError:
            self.send_error(404)
            return
        if field is None or domain is None or cycle not in config["cycles"] or hour not in field["hours"]:
            self.send_error(404)
            return
        fpath = image_path(field_id, domain, cycle, hour)
        key = field_job_key(field_id, domain, cycle)
        with _jobs_lock:
            job = _jobs.get(key)
            running = bool(job and job["running"])
        if not running:
            meta = read_meta(field_id, domain, cycle)
            if meta and meta.get("complete") and fpath.exists():
                self._send_file(fpath, "image/png")
                return
            start_cache(config, field, cycle, domain)
        job = wait_until_ready(field_id, domain, cycle)
        if job and job["error"] and not fpath.exists():
            self.send_error(404, job["error"])
            return
        try:
            image = ensure_image(config, field, cycle, hour, domain)
        except Exception:
            self.send_error(404, "Imagen no disponible")
            return
        self._send_file(image, "image/png")

    def _handle_region_image(self, region_id, field_id, cycle, hour_text):
        config = load_config()
        region = region_by_id(config, region_id)
        field = region_field_by_id(config, field_id)
        try:
            hour = int(hour_text)
        except ValueError:
            self.send_error(404)
            return
        if region is None or field is None or cycle not in config["cycles"] or hour not in field["hours"]:
            self.send_error(404)
            return
        fpath = region_image_path(region_id, field_id, cycle, hour)
        key = region_job_key(region_id, field_id, cycle)
        with _jobs_lock:
            job = _jobs.get(key)
            running = bool(job and job["running"])
        if not running:
            meta = read_region_meta(region_id, field_id, cycle)
            if meta and meta.get("complete") and fpath.exists():
                self._send_file(fpath, "image/png")
                return
            start_region_cache(config, region, field, cycle)
        job = wait_region_ready(region_id, field_id, cycle)
        if job and job["error"] and not fpath.exists():
            self.send_error(404, job["error"])
            return
        try:
            image = ensure_region_image(config, region, field, cycle, hour)
        except Exception:
            self.send_error(404, "Imagen no disponible")
            return
        self._send_file(image, "image/png")

    def _handle_savipa_image(self, fir, field, level_text, time_text):
        config = load_config()
        fir = savipa_normalize_fir(config, fir)
        field = savipa_normalize_field(config, field)
        try:
            level = int(level_text)
            forecast_time = int(time_text)
        except ValueError:
            self.send_error(404)
            return
        if fir is None or field is None or level < 0 or forecast_time < 0:
            self.send_error(404)
            return
        path = savipa_cache_path(fir, field, level, forecast_time)
        if path.exists() and path.stat().st_size > 0:
            self._send_file(path, "image/png")
            return
        try:
            image = ensure_savipa_image(config, fir, field, level, forecast_time)
        except Exception:
            self.send_error(404, "Imagen SAVIPA no disponible")
            return
        self._send_file(image, "image/png")

    def _handle_meteogram_image(self, cipi, cycle):
        config = load_config()
        if not CIPI_RE.match(cipi) or cycle not in config["cycles"]:
            self.send_error(404)
            return
        path = meteogram_file(cipi, cycle)
        key = meteogram_key(cipi, cycle)
        with _jobs_lock:
            job = _jobs.get(key)
            running = bool(job and job["running"])
        if not running:
            meta = read_json(meteogram_meta_file(cipi, cycle))
            if meta and meta.get("complete") and path.exists():
                self._send_file(path, "image/png")
                return
            start_meteogram_cache(config, cipi, cycle)
        job = wait_job(key)
        if job and job["error"] and not path.exists():
            self.send_error(404, job["error"])
            return
        try:
            lock = file_lock(path)
            with lock:
                if not path.exists() or path.stat().st_size == 0:
                    download(meteogram_url(config, cipi, cycle), path)
        except Exception:
            self.send_error(404, "Meteograma no disponible")
            return
        self._send_file(path, "image/png")

    def _send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path, content_type):
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        if content_type == "image/png":
            self.send_header("Cache-Control", "public, max-age=86400")
        else:
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, fmt, *args):
        if args and str(args[0]).startswith("GET /img/"):
            return
        super().log_message(fmt, *args)


def _one(query, key):
    values = query.get(key) or [""]
    return values[0]


def main():
    import sys

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8765
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"Visor en http://127.0.0.1:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
