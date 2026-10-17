# fleet-healer

**What it is:** A self-healing multi-host infrastructure monitor.
**Why it existed:** A real multi-machine self-hosted fleet needed monitoring with no budget for a third-party observability stack, and silent failures weren't acceptable.
**What I owned:** Full design and build, including three real false-positive/false-negative bugs found and fixed in production.
**Stack:** Python, Docker, Slack API, cron/launchd.
**Production status:** This repo is a sanitized reimplementation of a tool that's been running in production for months — the config here is illustrative, the logic and the bugs are real.

---

A self-healing, multi-vantage-point infrastructure monitor for a small
self-hosted server fleet. Checks containers, public URLs, disk usage, and
host reachability every 5 minutes; auto-remediates what it safely can;
alerts on what it can't; and separates "needs attention right now" from
"logged, batched into tomorrow's digest" so a noisy fleet doesn't train
everyone to ignore it.

This is a sanitized, standalone version of a tool that's been running in
production for months on a real multi-host self-hosted setup. The
`MACHINES`/`CONTAINERS`/`URLS` config in `fleet_healer.py` is illustrative
example data, not a real inventory — but every check, every auto-fix, and
every comment describing a real bug is accurate to what was actually found
running this in production.

## What it actually does

- **Checks containers across every host**, from whichever host is running
  the check — not just "is this process up on the machine I'm sitting on."
- **Auto-remediates the common case**: tries `docker start` first, falls
  back to `docker compose up` if the container doesn't exist at all.
- **Distinguishes "root cause reached, working as designed" from "request
  never got there"** for file-server-backed routes with no index page —
  both return a 404, but a reverse proxy's own expected root-404 carries a
  specific cache header a missing-ingress 404 from the edge/CDN layer
  doesn't. A plain status-code check can't tell these apart; this one does.
- **Splits every finding into two alert tiers.** HIGH interrupts
  immediately. LOW gets fully logged and batched into a once-daily digest.
  Nothing is ever silently dropped — it's just not an interrupt. This
  exists because of a real, measured problem: before the split, a 5-minute
  network blip on a non-critical host was paging exactly as loudly as a
  real primary-host outage, which is how you train a human to stop reading
  alerts.
- **Tracks issue age, not just issue existence.** A standing problem is
  reported with "open 2h 15m," not re-announced from scratch or silently
  dropped after the first mention.

## Real lessons baked into this design

**A self-network blip looks identical to "everything else is down" from
the checking host's point of view — unless you check for it first.** If
the host running the check loses its own outbound connectivity, every
remote check (SSH to other hosts, every URL, even the Slack API call
itself) fails simultaneously. Without a dedicated gate for this, that
produces a false "multiple hosts offline" alert, followed by a false
"recovered" alert five minutes later once the checking host's own network
comes back — a pattern that was, for a real period of time, the single
biggest source of noisy, non-actionable alerts this tool produced.
`check_self_connectivity()` runs first and skips the entire cycle if it
fails, rather than ever attributing a local problem to a remote host.

**An "inactive" flag in an application's own database is not the same
guarantee as the process not running.** A real incident (documented in a
companion repo, [incident-postmortem-duplicate-send](https://github.com/melissa-r-chaudhry/incident-postmortem-duplicate-send))
found that a workflow engine's own "active=false" flag did not reliably
stop its internal scheduler — a workflow kept firing for hours after a
clean restart with the flag already correctly set to inactive. The
practical lesson that shaped this tool's design: verifying something is
truly off has to mean confirming no new activity occurs, not trusting a
database flag or a config value.

**Identical tooling deployed to multiple hosts for redundancy will drift
out of sync, and the un-updated copy is often invisible until it causes
real damage.** This exact class of tool — deployed unmodified to more than
one host, by design, so monitoring itself has no single point of failure —
was once the mechanism that kept silently undoing a fix: one copy got
patched, a second deployed copy didn't, and kept independently
auto-remediating the exact thing the first patch was meant to stop. The
practical takeaway, not specific to this tool: when something is
intentionally deployed as multiple identical copies, a fix to the logic
has to be verified against *every* copy, not just the one you happened to
touch first.

## Configuration

Everything that's specific to a real deployment — hostnames, SSH aliases,
container names, public URLs — lives in the `MACHINES` / `CONTAINERS` /
`URLS` / `FILE_SERVER_URLS` constants near the top of `fleet_healer.py`.
Secrets (a Slack bot token, in this example) load from a local,
`chmod 600` `.env.secrets` file rather than being hardcoded or kept behind
any passphrase gate — this process needs to run fully unattended on a
5-minute schedule, which rules out anything requiring a human to unlock it.

```
# ~/fleet-healer/.env.secrets
SLACK_TOKEN=xoxb-...
```

Run it on a cron/launchd/systemd-timer schedule, deployed identically to
every host that should be checking the fleet from its own vantage point.
