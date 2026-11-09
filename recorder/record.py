# AI generated
# Saves Madison Metro's realtime feeds as raw gzipped protobuf under data/raw/<date>/<feed>/.

import csv
import gzip
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Chicago")
RAW_DIR = Path("data/raw")

FEEDS = {
    "trips": ("https://metromap.cityofmadison.com/gtfsrt/trips", 30),
    "vehicles": ("https://metromap.cityofmadison.com/gtfsrt/vehicles", 30),
    "alerts": ("https://metromap.cityofmadison.com/gtfsrt/alerts", 300),
}


def log(msg):
    print(datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"), msg)


def read_env():
    env = {}
    for line in open(".env"):
        line = line.strip()
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            env[key] = value
    return env


def fetch(url, user_agent):
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return resp.read()


def save_poll(day_dir, row):
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / "polls.csv"
    new_file = not path.exists()
    with open(path, "a", newline="") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(["fetched_at", "local_time", "feed", "result", "bytes", "elapsed_ms", "detail"])
        writer.writerow(row)


def poll(name, url, interval, state, user_agent):
    started = time.time()
    local = datetime.fromtimestamp(started, TZ)
    day_dir = RAW_DIR / local.strftime("%Y-%m-%d")
    row = [f"{started:.1f}", local.isoformat(timespec="seconds"), name]

    try:
        body = fetch(url, user_agent)
    except Exception as e:
        # wait twice as long after each failure, up to 10 min
        state["fails"] += 1
        delay = min(interval * 2 ** state["fails"], 600)
        state["next"] = started + delay
        save_poll(day_dir, row + ["error", 0, int((time.time() - started) * 1000), repr(e)])
        log(f"{name} ERROR {e!r}, retry in {delay}s")
        return

    elapsed_ms = int((time.time() - started) * 1000)
    state["fails"] = 0
    state["next"] = started + interval

    if body == state["last_body"]:
        result = "unchanged"
    else:
        path = day_dir / name / f"{int(started)}.pb.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wb") as f:
            f.write(body)
        state["last_body"] = body
        result = "saved"

    save_poll(day_dir, row + [result, len(body), elapsed_ms, ""])
    log(f"{name} {result} {len(body)} bytes {elapsed_ms} ms")


def main():
    contact = read_env()["USER_AGENT_CONTACT"]
    user_agent = f"BusBuffer/0.1 (UW-Madison student project; contact: {contact})"
    state = {name: {"next": 0, "fails": 0, "last_body": None} for name in FEEDS}

    log("recording to data/raw")
    while True:
        for name, (url, interval) in FEEDS.items():
            if time.time() >= state[name]["next"]:
                poll(name, url, interval, state[name], user_agent)
        time.sleep(1)


if __name__ == "__main__":
    main()
