#!/usr/bin/env python3
"""
fleet-healer — multi-host infrastructure monitor and auto-healer.

Runs every 5 minutes (via cron/launchd/systemd timer) on more than one host
in the fleet, each checking the whole fleet from its own vantage point, and
posts to Slack. Designed for a small self-hosted multi-machine setup where
there's no budget or need for a full observability stack, but silent
failures are unacceptable.

This is a sanitized, standalone reimplementation of a real tool that's been
running in production for months — the MACHINES/CONTAINERS/URLS config
below is illustrative example data, not a real inventory, but every check,
every auto-fix, and every comment describing a real lesson learned is
accurate to what was actually found and fixed building the original.
"""

import subprocess, json, time, urllib.request, urllib.error
from datetime import datetime
import sys, os, traceback, hashlib, socket

def _load_env_file(path):
    """Minimal .env parser — no external deps. Returns {} if file missing."""
    out = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return out

# Secrets deliberately loaded from a chmod-600 file local to each host, not
# hardcoded and not behind any passphrase-gated vault — this process runs
# unattended every 5 minutes with nobody present to unlock anything. One
# file per host (no cross-host sync), but a single shared source within each
# host that any local automation script can read, rather than every script
# keeping its own copy.
_secrets = _load_env_file(os.path.expanduser("~/fleet-healer/.env.secrets"))

SLACK_TOKEN   = _secrets.get("SLACK_TOKEN", "")
SLACK_CHANNEL = "fleet-healer"
DOCKER        = "/usr/local/bin/docker"
LOG_FILE      = "/tmp/fleet-healer.log"
STATE_FILE    = "/tmp/fleet-healer-state.json"   # tracks known issues to avoid spam
FLEET_ROOT    = os.path.expanduser("~/fleet")    # NOT hardcoded to one path — this
                                                  # script is deployed unmodified to
                                                  # every host and must resolve relative
                                                  # to whichever host it's running on.

# ── Alert tiers ──────────────────────────────────────────────────────────────
# HIGH → fires an immediate Slack post.
# LOW  → never interrupts in real time; accumulates and only surfaces in a
#        daily digest, batched with everything since the last digest.
#        Nothing is ever silently dropped — it's just not an interrupt.
#        Added after a real noise problem: a 5-minute network blip on a
#        secondary host that resolved on its own before anyone could act on
#        it was paging the same as a real primary-host outage. Splitting by
#        tier fixed the signal-to-noise problem without losing any data —
#        the low-tier event is still fully logged, just not an interrupt.

CONTAINER_TIER = {"CRITICAL": "HIGH", "HIGH": "HIGH", "MEDIUM": "LOW", "LOW": "LOW"}
MACHINE_TIER   = {"primary": "HIGH", "inference": "HIGH", "standby": "LOW", "storage": "LOW"}

# ── Machines ─────────────────────────────────────────────────────────────────
# silent=True → offline is expected/normal for this host, never triggers an alert.
# "ssh" is the alias used to reach that host WHEN IT ISN'T THIS_MACHINE — every
# host (including the one currently running) needs a real alias here, since any
# one of them might be "local" depending on which copy of this script is live.

MACHINES = [
    {"name": "primary",   "ssh": "host-a", "role": "primary-docker", "silent": False},
    {"name": "inference",  "ssh": "host-b", "role": "inference",      "silent": False},
    {"name": "standby",    "ssh": "host-c", "role": "warm-standby",   "silent": False},
    {"name": "storage",    "ssh": "host-d", "role": "storage",        "silent": False},
    {"name": "backup-laptop", "ssh": "host-e", "role": "backup",      "silent": True},
]

# ── Containers: (name, compose_subdir_or_None, priority) ─────────────────────
# compose_subdir is relative to FLEET_ROOT on that host.
CONTAINERS = {
    "primary": [
        ("reverse-proxy", "caddy",       "CRITICAL"),
        ("tunnel",        "cloudflared", "CRITICAL"),
        ("database",      "postgres",    "CRITICAL"),
        ("workflow-engine","n8n",        "CRITICAL"),
        ("cache",          None,         "HIGH"),
        ("mail-list-manager","listmonk", "HIGH"),
        ("internal-tool-a", "tool-a",    "MEDIUM"),
        ("internal-tool-b", "tool-b",    "LOW"),
    ],
    "standby": [
        ("reverse-proxy", "caddy",    "HIGH"),
        ("database",      "postgres", "HIGH"),
        # workflow-engine deliberately excluded here: this host's copy stays
        # fully stopped between real failovers by design — a real incident
        # found that "marked inactive" in the application's own database was
        # NOT sufficient to stop its internal scheduler (it kept firing a
        # cron trigger for hours after a clean restart with the flag already
        # set to inactive). Only the process actually not running is a
        # reliable guarantee, so this host's copy is fully stopped, not just
        # flagged off, and only started deliberately during a real promotion.
        ("cache",          None, "MEDIUM"),
    ],
}

# ── Public URLs ───────────────────────────────────────────────────────────────
# 4th element = alert tier.
URLS = [
    ("Main app",        "https://app.example.com/healthz", [200],      "HIGH"),
    ("Public site",     "https://example.com",              [200],      "HIGH"),
    ("Internal tool",   "https://internal.example.com",     [200, 302], "LOW"),
]

# File-server-backed routes: root "/" has no index, so it 404s by design even
# when healthy. check_file_server_route() below tells apart the reverse
# proxy's own expected root-404 (has the Cache-Control header the route
# explicitly sets) from an edge/CDN-layer catch-all 404 (missing ingress
# entry — request never actually reached the reverse proxy, no such header)
# — these look identical from a bare status-code check, but mean completely
# different things: one is "working as designed," the other is "broken
# routing config." Found this distinction the hard way after a real outage
# where a route return a plausible-looking 404 for a day before anyone
# noticed it was actually the wrong kind of 404.
FILE_SERVER_URLS = [
    ("Media host", "https://media.example.com/", "LOW"),
]

DISK_WARN_PCT = 85
OLLAMA_EXPECTED_MODELS = 2  # example of a service-specific expectation, not just "is it up"

# ── Helpers ───────────────────────────────────────────────────────────────────

def log(msg):
    ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def run_cmd(cmd, timeout=20):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return False, "", "timeout"
    except Exception as e:
        return False, "", str(e)

def ssh(host, cmd, timeout=18):
    escaped = cmd.replace('"', '\\"')
    return run_cmd(f'ssh -o ConnectTimeout=8 -o BatchMode=yes -o StrictHostKeyChecking=no {host} "{escaped}"', timeout)

def docker_local(cmd):
    return run_cmd(f"{DOCKER} {cmd}")

def docker_remote(host, cmd):
    return ssh(host, f"/usr/local/bin/docker {cmd}")

def check_url(url, expected_codes):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "fleet-healer/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            return r.status in expected_codes, r.status
    except urllib.error.HTTPError as e:
        return e.code in expected_codes, e.code
    except Exception as e:
        return False, str(e)

def check_self_connectivity():
    """Fast check that THIS host (not a remote one) has real outbound internet.

    Real bug this fixes: when the checking host's OWN network blips, every
    remote check fails at once (SSH to other hosts, all URLs, even the Slack
    post itself) — indistinguishable, from the old code's point of view, from
    every other host in the fleet genuinely being down simultaneously. That
    produced a false "multiple hosts offline" HIGH alert, followed by a false
    "recovered" alert five minutes later once the checking host's own network
    came back. This was the single biggest source of noisy, non-actionable
    alerts before being fixed. This gate runs first and skips the entire
    cycle if it fails, so a self-network blip can never get misattributed to
    any other host.
    """
    try:
        with urllib.request.urlopen("https://www.google.com/generate_204", timeout=6) as r:
            return r.status in (200, 204)
    except Exception:
        return False

def check_file_server_route(url):
    """See FILE_SERVER_URLS comment above for why this isn't just check_url."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "fleet-healer/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            return True, r.status
    except urllib.error.HTTPError as e:
        cc = e.headers.get("Cache-Control", "") if e.headers else ""
        if e.code == 404 and "no-store" in cc:
            return True, "404 (expected — reverse proxy reached, file-server root)"
        return False, e.code
    except Exception as e:
        return False, str(e)

def check_disk(ssh_host):
    """Returns (pct_used_int_or_None, error_or_None). ssh_host=None for local host."""
    if ssh_host is None:
        ok, out, err = run_cmd("df / | awk 'NR==2{gsub(/%/,\"\",$5); print $5}'")
    else:
        # Remote via ssh(): the command string passes through a local shell
        # (shell=True) before ssh even sees it, so an unescaped $5 gets
        # expanded to empty locally first. Escape it so it survives to the
        # remote awk instead.
        ok, out, err = ssh(ssh_host, "df / | awk 'NR==2{gsub(/%/,\"\",\\$5); print \\$5}'", timeout=12)
    if not ok or not out.strip():
        return None, err or "no output"
    try:
        return int(out.strip()), None
    except ValueError:
        return None, f"unparseable: {out!r}"

def load_state():
    try:
        with open(STATE_FILE) as f:
            state = json.load(f)
    except Exception:
        state = {}
    state.setdefault("known_issues", {})
    state.setdefault("low_tier_log", [])
    for k, v in list(state["known_issues"].items()):
        if isinstance(v, str):
            state["known_issues"][k] = {"first_seen": v, "tier": "HIGH"}
    return state

def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        log(f"Could not save state: {e}")

def issue_key(issue_text):
    return hashlib.md5(issue_text.encode()).hexdigest()[:12]

def slack_post(text):
    payload = json.dumps({"channel": SLACK_CHANNEL, "text": text, "unfurl_links": False}).encode()
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage", data=payload,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {SLACK_TOKEN}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            result = json.load(r)
            if not result.get("ok"):
                log(f"Slack error: {result.get('error')}")
            return result.get("ok", False)
    except Exception as e:
        log(f"Slack post failed: {e}")
        return False

# ── Container checks + fixes ──────────────────────────────────────────────────

def container_status(this_machine, machine, name):
    cmd = f'inspect {name} --format "{{{{.State.Status}}}}"'
    if machine == this_machine:
        ok, out, _ = docker_local(cmd)
    else:
        ssh_host = next(m["ssh"] for m in MACHINES if m["name"] == machine)
        ok, out, _ = docker_remote(ssh_host, cmd)
    if not ok or not out:
        return False, "not found"
    status = out.strip().strip('"')
    return status == "running", status

def fix_container(this_machine, machine, name, compose_subdir):
    """Try `docker start` first; if the container doesn't exist at all, fall
    back to `docker compose up`. Returns (fixed, method)."""
    if machine == this_machine:
        ok, _, _ = docker_local(f"start {name}")
        if not ok and compose_subdir:
            cdir = f"{FLEET_ROOT}/{compose_subdir}"
            ok, _, _ = docker_local(f"compose -f {cdir}/docker-compose.yml up -d {name}")
        method = "local"
    else:
        ssh_host = next(m["ssh"] for m in MACHINES if m["name"] == machine)
        ok, _, _ = docker_remote(ssh_host, f"start {name}")
        if not ok and compose_subdir:
            ok, _, _ = ssh(ssh_host, f"/usr/local/bin/docker compose -f ~/fleet/{compose_subdir}/docker-compose.yml up -d")
        method = f"ssh:{ssh_host}"

    time.sleep(6)
    running, _ = container_status(this_machine, machine, name)
    return running, method

# ── Main ──────────────────────────────────────────────────────────────────────

def run_healer():
    this_machine = {"host-a": "primary", "host-b": "inference", "host-c": "standby"}.get(
        socket.gethostname().split(".")[0].lower(), "primary"
    )
    now = datetime.now()
    is_digest_hour = (now.hour == 7 and now.minute < 5)

    # ── 0. Self-connectivity gate — see check_self_connectivity() docstring ──
    if not check_self_connectivity():
        log(f"SELF-NETWORK-DOWN on {this_machine} — skipping this cycle (local connectivity issue, not a remote outage)")
        state = load_state()
        state.setdefault("low_tier_log", []).append(
            f"{now.strftime('%-I:%M%p')} — ⚠️ {this_machine} lost its own outbound network briefly, cycle skipped"
        )
        save_state(state)
        return

    issues, fixes, skipped = [], [], []

    # ── 1. Machine reachability ───────────────────────────────────────────────
    machine_up = {}
    for m in MACHINES:
        if m["name"] == this_machine:
            machine_up[m["name"]] = True
            continue
        ok, _, _ = ssh(m["ssh"], "echo ok", timeout=12)
        machine_up[m["name"]] = ok
        if not ok:
            log(f"OFFLINE: {m['name']}")

    offline_noisy = [n for n, up in machine_up.items() if not up
                     and not next(m["silent"] for m in MACHINES if m["name"] == n)]

    if len(offline_noisy) >= 2:
        # HIGH only if a HIGH-tier host is among the offline set — a
        # simultaneous blip on two LOW-tier hosts (the common shared-ISP
        # case) stays LOW rather than paging.
        tier = "HIGH" if any(MACHINE_TIER.get(n) == "HIGH" for n in offline_noisy) else "LOW"
        issues.append((tier, f"⚠️ *{len(offline_noisy)} hosts offline simultaneously* — probable shared network outage\n  Offline: {', '.join(offline_noisy)}"))
    elif offline_noisy:
        for n in offline_noisy:
            issues.append((MACHINE_TIER.get(n, "HIGH"), f"⚠️ *{n}* unreachable via SSH — check host/power"))

    # ── 2. Container checks + auto-fix ────────────────────────────────────────
    for machine, containers in CONTAINERS.items():
        if not machine_up.get(machine, False):
            skipped.append(f"{machine} (host offline — skipped container checks)")
            continue
        for (name, compose_sub, priority) in containers:
            running, status = container_status(this_machine, machine, name)
            if running:
                continue
            log(f"DOWN: {machine}/{name} (status={status}, priority={priority})")
            fixed, method = fix_container(this_machine, machine, name, compose_sub)
            tier = CONTAINER_TIER[priority]
            if fixed:
                fixes.append((tier, f"• `{name}` on {machine} → restarted ✓"))
                log(f"FIXED: {machine}/{name} via {method}")
            else:
                issues.append((tier, f"🔴 *{name}* [{priority}] on {machine} — down, auto-fix failed"))

    # ── 3. Public URLs ─────────────────────────────────────────────────────────
    url_failures_high, url_failures_low = [], []
    for label, url, expected, tier in URLS:
        ok, code = check_url(url, expected)
        if not ok:
            (url_failures_high if tier == "HIGH" else url_failures_low).append(f"{label} ({code})")
    for label, url, tier in FILE_SERVER_URLS:
        ok, code = check_file_server_route(url)
        if not ok:
            (url_failures_high if tier == "HIGH" else url_failures_low).append(f"{label} ({code})")
    if url_failures_high:
        issues.append(("HIGH", f"🔴 *URLs returning errors:* {', '.join(url_failures_high)}"))
    if url_failures_low:
        issues.append(("LOW", f"🔴 *URLs returning errors:* {', '.join(url_failures_low)}"))

    # ── 4. Disk usage ─────────────────────────────────────────────────────────
    disk_warnings = []
    for m in MACHINES:
        if not machine_up.get(m["name"], False):
            continue
        pct, err = check_disk(None if m["name"] == this_machine else m["ssh"])
        if pct is not None and pct >= DISK_WARN_PCT:
            disk_warnings.append(f"{m['name']} ({pct}%)")
    if disk_warnings:
        issues.append(("LOW", f"⚠️ *Disk usage high:* {', '.join(disk_warnings)}"))

    # ── 5. Classify: new vs standing vs recovered, split by tier ──────────────
    # Every known issue is stamped with its first-seen time + tier, so
    # standing issues can be reported with an age instead of going
    # permanently silent, and recovered issues attribute to the right tier
    # even though only their hash (not original text) survives to recovery.
    state = load_state()
    known = state["known_issues"]
    low_tier_log = state["low_tier_log"]
    now_ts = now.isoformat()
    ts_short = now.strftime("%-I:%M%p")

    new_high, new_low = [], []
    standing_high, standing_low = [], []
    for tier, issue in issues:
        k = issue_key(issue)
        if k not in known:
            known[k] = {"first_seen": now_ts, "tier": tier}
            (new_high if tier == "HIGH" else new_low).append(issue)
            if tier == "LOW":
                low_tier_log.append(f"{ts_short} — 🆕 {issue}")
        else:
            entry_tier = known[k].get("tier", tier)
            (standing_high if entry_tier == "HIGH" else standing_low).append((issue, known[k]["first_seen"]))

    current_keys = {issue_key(i) for _, i in issues}
    recovered_high_count = recovered_low_count = 0
    for k in [k for k in list(known.keys()) if k not in current_keys]:
        entry_tier = known[k].get("tier", "HIGH")
        if entry_tier == "HIGH":
            recovered_high_count += 1
        else:
            recovered_low_count += 1
            low_tier_log.append(f"{ts_short} — ✅ recovered (was open since {known[k].get('first_seen','?')})")
        del known[k]

    fixes_high = [text for tier, text in fixes if tier == "HIGH"]
    fixes_low  = [text for tier, text in fixes if tier == "LOW"]
    for text in fixes_low:
        low_tier_log.append(f"{ts_short} — 🔧 {text}")

    if len(low_tier_log) > 300:
        low_tier_log = low_tier_log[-300:]

    state["known_issues"] = known
    state["low_tier_log"] = low_tier_log
    save_state(state)

    def fmt_age(first_seen_iso):
        try:
            delta = now - datetime.fromisoformat(first_seen_iso)
        except Exception:
            return "unknown"
        days, hours = delta.days, delta.seconds // 3600
        if days > 0:
            return f"{days}d {hours}h"
        minutes = (delta.seconds % 3600) // 60
        return f"{hours}h {minutes}m" if hours else f"{minutes}m"

    # ── 6. Decide whether to post right now ───────────────────────────────────
    changed_high_this_run = bool(new_high or fixes_high or recovered_high_count)
    if not changed_high_this_run and not is_digest_hour:
        log("All systems nominal — silent pass" if not (new_low or fixes_low or recovered_low_count)
            else f"LOW-tier only ({len(new_low)} new, {len(fixes_low)} fixed, {recovered_low_count} recovered) — logged, no interrupt")
        return

    # ── 7. Build and send the Slack message ───────────────────────────────────
    ts = now.strftime("%a %b %-d, %-I:%M %p")
    machine_line = "  ".join(
        f"✅ {m['name']}" if machine_up.get(m['name']) else f"⚠️ {m['name']} (offline)"
        for m in MACHINES
    )

    if is_digest_hour and not new_high and not fixes_high and not standing_high and not recovered_high_count and not low_tier_log:
        slack_post(f"✅ *Fleet — all systems nominal* | {ts}\n{machine_line}\n{len(URLS)} URLs ✓ | All containers running")
        log("Daily digest sent — all clear")
        return

    header = "✅ *fleet-healer — daily digest*" if (is_digest_hour and not new_high and not fixes_high) else "🏥 *fleet-healer alert*"
    lines = [f"{header} | {ts}", "", f"*Hosts:* {machine_line}"]
    if fixes_high:
        lines += ["", "*Auto-fixed:*"] + [f"• {t}" for t in fixes_high]
    if recovered_high_count:
        lines += ["", f"*Recovered:* {recovered_high_count} issue(s) cleared since last run ✓"]
    if new_high:
        lines += ["", "*🆕 New — needs attention:*"] + [f"  {i}" for i in new_high]
    if standing_high:
        lines += ["", "*⏳ Still open:*"] + [f"  {i}  _(open {fmt_age(ts_seen)})_" for i, ts_seen in standing_high]

    if is_digest_hour:
        if standing_low:
            lines += ["", "*⏳ Still open (lower priority):*"] + [f"  {i}  _(open {fmt_age(ts_seen)})_" for i, ts_seen in standing_low]
        if low_tier_log:
            lines += ["", f"*Lower-priority activity since last digest ({len(low_tier_log)} event(s)):*"]
            lines += [f"  {entry}" for entry in low_tier_log[-40:]]
            if len(low_tier_log) > 40:
                lines += [f"  _...and {len(low_tier_log) - 40} more (see {LOG_FILE})_"]
        state["low_tier_log"] = []
        save_state(state)

    if skipped:
        lines += ["", f"_Skipped: {', '.join(skipped)}_"]

    slack_post("\n".join(lines))
    log(f"Alert sent — {len(new_high)} new(high), {len(standing_high)} standing(high), {len(fixes_high)} fixes(high), {recovered_high_count} recovered(high)")


if __name__ == "__main__":
    log("=== fleet-healer run starting ===")
    try:
        run_healer()
    except Exception as e:
        log(f"FATAL: {e}\n{traceback.format_exc()}")
        try:
            slack_post(f"🔴 *fleet-healer crashed* — {e}\nCheck log: {LOG_FILE}")
        except Exception:
            pass
