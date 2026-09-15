#!/usr/bin/env python3
"""Overnight Ralph — Continuous ablation loop with periodic supervisor barge-in.

Ralph runs ablation experiments in a loop. Every 5 minutes of wall time,
a supervisor node reads the accumulated results, adjusts strategy, and
writes updated guidance. Ralph reads the guidance on its next iteration.

Thermal-aware: monitors system temperature and pauses if throttling.

Usage:
    python3 scripts/overnight-ralph.py
    SUPERVISOR=codex python3 scripts/overnight-ralph.py
"""

import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

# ── Config ───────────────────────────────────────────────────────────────────

OLLAMA = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
KERNEL = os.environ.get("KERNEL_HOST", "http://localhost:6931")
WORKSPACE = os.environ.get("WORKSPACE", os.path.expanduser("~/workspaces/cog"))
SUPERVISOR_MODE = os.environ.get("SUPERVISOR", "local")  # "local" or "codex"
MODEL = os.environ.get("MODEL", "gemma4:26b")
SUPERVISOR_MODEL = os.environ.get("SUPERVISOR_MODEL", "gemma4:26b")
BARGE_IN_INTERVAL = int(os.environ.get("BARGE_IN_INTERVAL", "300"))  # 5 min
THERMAL_LIMIT = int(os.environ.get("THERMAL_LIMIT", "95"))  # celsius
COOL_DOWN_PAUSE = int(os.environ.get("COOL_DOWN_PAUSE", "60"))  # seconds
INTER_QUESTION_PAUSE = int(os.environ.get("INTER_QUESTION_PAUSE", "5"))  # seconds

OUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/tmp/ralph-overnight"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

CHAIN_FILE = OUT_DIR / "chain.jsonl"
SUPERVISOR_FILE = OUT_DIR / "supervisor-guidance.md"
SUMMARY_FILE = OUT_DIR / "summary.md"
LOG_FILE = OUT_DIR / "ralph.log"

# ── Question Bank ────────────────────────────────────────────────────────────

QUESTIONS = [
    {"id": "ws-01", "q": "What are the four process states in the CogOS kernel?",
     "expect": ["active", "receptive", "consolidating", "dormant"]},
    {"id": "ws-02", "q": "What is the default port for the CogOS kernel?",
     "expect": ["6931"]},
    {"id": "ws-03", "q": "What is the TRM's D_STATE value?",
     "expect": ["4"]},
    {"id": "ws-04", "q": "What four zones does the foveated context engine use?",
     "expect": ["nucleus", "knowledge", "history", "current"]},
    {"id": "ws-05", "q": "What hash algorithm does the CogOS ledger use?",
     "expect": ["sha-256", "sha256"]},
    {"id": "ws-06", "q": "What is the default local model in CogOS?",
     "expect": ["gemma4", "e4b"]},
    {"id": "ws-07", "q": "How many attention heads does the TRM's best config use?",
     "expect": ["16"]},
    {"id": "ws-08", "q": "What does the sovereignty gradient determine?",
     "expect": ["local", "provider", "route", "fallback"]},
    {"id": "ws-09", "q": "What is the ConstellationBridge used for?",
     "expect": ["heartbeat", "trust", "constellation"]},
    {"id": "ws-10", "q": "What three mechanisms does LoRO unify?",
     "expect": ["ple", "lora", "trm"]},
    {"id": "ws-11", "q": "What paper inspired the CogOS TRM?",
     "expect": ["jolicoeur", "samsung", "tiny recursive"]},
    {"id": "ws-12", "q": "What two signals drive foveated rendering?",
     "expect": ["iris", "foveal", "where", "how much"]},
    {"id": "ws-13", "q": "What is the EA/EFM thesis in one sentence?",
     "expect": ["externalized", "attention", "executive", "substrate"]},
    {"id": "ws-14", "q": "How does the tool-call hallucination gate work?",
     "expect": ["validate", "tool", "reject", "unknown"]},
    {"id": "ws-15", "q": "What is the modality bus in mod3?",
     "expect": ["modality", "bus", "voice", "translate"]},
]

# ── Helpers ──────────────────────────────────────────────────────────────────

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


def get_cpu_temp() -> float:
    """Get CPU temperature on macOS. Returns 0 if unavailable."""
    try:
        r = subprocess.run(
            ["sudo", "-n", "powermetrics", "--samplers", "smc", "-i", "1", "-n", "1"],
            capture_output=True, text=True, timeout=5)
        for line in r.stdout.split("\n"):
            if "CPU die temperature" in line:
                return float(line.split(":")[1].strip().replace(" C", ""))
    except Exception:
        pass
    # Fallback: check if thermal throttling via sysctl
    try:
        r = subprocess.run(["sysctl", "machdep.xcpm.cpu_thermal_level"],
                          capture_output=True, text=True, timeout=2)
        level = int(r.stdout.strip().split(":")[-1].strip())
        if level > 0:
            return THERMAL_LIMIT  # Signal throttling
    except Exception:
        pass
    return 0.0


def check_thermal() -> bool:
    """Returns True if OK to proceed, False if need to cool down."""
    temp = get_cpu_temp()
    if temp >= THERMAL_LIMIT:
        log(f"⚠ Thermal throttle: {temp}°C >= {THERMAL_LIMIT}°C. Cooling down {COOL_DOWN_PAUSE}s...")
        time.sleep(COOL_DOWN_PAUSE)
        return False
    return True


def ollama_chat(model: str, messages: list, temp: float = 0.1, timeout: int = 120) -> str:
    data = json.dumps({
        "model": model, "messages": messages,
        "stream": False, "options": {"temperature": temp},
    }).encode()
    req = Request(f"{OLLAMA}/api/chat", data=data,
                  headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())["message"]["content"]
    except Exception as e:
        return f"ERROR: {e}"


def kernel_foveated(prompt: str) -> str:
    data = json.dumps({
        "prompt": prompt, "iris": {"size": 128000, "used": 5000},
        "profile": "default",
    }).encode()
    req = Request(f"{KERNEL}/v1/context/foveated", data=data,
                  headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read())
            return body.get("data", body).get("context", "")
    except Exception:
        return ""


def rag_search(query: str) -> str:
    keywords = [w for w in query.lower().split() if len(w) > 3][:5]
    pattern = "|".join(keywords)
    try:
        r = subprocess.run(
            ["grep", "-rli", "-E", pattern,
             f"{WORKSPACE}/.cog/mem/", f"{WORKSPACE}/apps/cogos-v3/"],
            capture_output=True, text=True, timeout=5)
        files = [f for f in r.stdout.strip().split("\n") if f.strip()][:5]
        parts = []
        for f in files:
            try:
                parts.append(f"--- {Path(f).name} ---\n{Path(f).read_text()[:2000]}")
            except Exception:
                pass
        return "\n\n".join(parts)
    except Exception:
        return ""


def score(response: str, expected: list[str]) -> float:
    resp = response.lower()
    found = sum(1 for t in expected if t.lower() in resp)
    return found / len(expected) if expected else 0.0


def append_chain(entry: dict):
    with open(CHAIN_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


def read_chain() -> list[dict]:
    if not CHAIN_FILE.exists():
        return []
    entries = []
    for line in CHAIN_FILE.read_text().strip().split("\n"):
        if line.strip():
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return entries


# ── Conditions ───────────────────────────────────────────────────────────────

def run_stock(model: str, question: str) -> str:
    return ollama_chat(model, [{"role": "user", "content": question}])

def run_rag(model: str, question: str) -> str:
    ctx = rag_search(question)
    if not ctx:
        return run_stock(model, question)
    return ollama_chat(model, [
        {"role": "system", "content": f"Context:\n\n{ctx}"},
        {"role": "user", "content": question},
    ])

def run_foveated(model: str, question: str) -> str:
    ctx = kernel_foveated(question)
    if not ctx:
        return run_stock(model, question)
    return ollama_chat(model, [
        {"role": "system", "content": f"Workspace context (CogOS foveated):\n\n{ctx}"},
        {"role": "user", "content": question},
    ])

CONDITIONS = {
    "A": ("stock", run_stock),
    "B": ("rag", run_rag),
    "C": ("foveated", run_foveated),
}

# ── Supervisor ───────────────────────────────────────────────────────────────

def supervisor_barge_in(chain: list[dict], cycle: int):
    """Supervisor reads the chain and produces updated guidance."""
    log("═══ SUPERVISOR BARGE-IN ═══")

    # Compute running stats
    stats = {}
    for cond in ["A", "B", "C"]:
        cond_entries = [e for e in chain if e.get("condition") == cond]
        if cond_entries:
            avg = sum(e["score"] for e in cond_entries) / len(cond_entries)
            stats[cond] = {"avg": round(avg, 3), "n": len(cond_entries)}

    stats_text = "\n".join(f"  {k} ({CONDITIONS[k][0]}): avg={v['avg']:.3f} n={v['n']}"
                           for k, v in stats.items())

    # Recent entries
    recent = chain[-15:] if len(chain) > 15 else chain
    recent_text = "\n".join(
        f"  {e['question_id']} cond={e['condition']} score={e['score']:.2f} ({e['elapsed_sec']}s)"
        for e in recent
    )

    # Per-question breakdown
    q_scores = {}
    for e in chain:
        qid = e["question_id"]
        cond = e["condition"]
        if qid not in q_scores:
            q_scores[qid] = {}
        if cond not in q_scores[qid]:
            q_scores[qid][cond] = []
        q_scores[qid][cond].append(e["score"])

    interesting = []
    for qid, conds in q_scores.items():
        avgs = {c: sum(s)/len(s) for c, s in conds.items()}
        if "C" in avgs and "A" in avgs:
            diff = avgs["C"] - avgs["A"]
            if abs(diff) > 0.2:
                interesting.append(f"  {qid}: foveated {'beats' if diff > 0 else 'loses to'} stock by {diff:+.2f}")

    interesting_text = "\n".join(interesting) if interesting else "  (no strong differentials yet)"

    prompt = f"""You are the supervisor of an overnight ablation study comparing CogOS context assembly methods.

Cycle: {cycle}
Total observations: {len(chain)}

Running averages:
{stats_text}

Recent results:
{recent_text}

Interesting differentials (foveated vs stock, |diff| > 0.2):
{interesting_text}

Based on these results:
1. Which questions show the biggest difference between conditions? Why?
2. Are there patterns in what foveated assembly gets right vs wrong?
3. Any recommendations for the next cycle? (e.g., focus on specific questions, adjust temperature)
4. Is the data sufficient to draw preliminary conclusions?

Be concise — 5-10 sentences max."""

    if SUPERVISOR_MODE == "codex":
        try:
            result = subprocess.run(
                ["codex", "exec", "-m", "gpt-5.6-terra",
                 "--config", "model_reasoning_effort=low",
                 "--sandbox", "read-only", "--skip-git-repo-check",
                 prompt],
                capture_output=True, text=True, timeout=120)
            analysis = result.stdout.strip()
        except Exception as e:
            analysis = f"Codex supervisor failed: {e}"
    else:
        analysis = ollama_chat(SUPERVISOR_MODEL, [
            {"role": "user", "content": prompt}
        ], temp=0.3, timeout=60)

    # Save supervisor guidance
    guidance = f"""# Supervisor Guidance — Cycle {cycle}
Updated: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
Observations: {len(chain)}

## Running Scores
{stats_text}

## Analysis
{analysis}

## Interesting Questions
{interesting_text}
"""
    SUPERVISOR_FILE.write_text(guidance)

    # Append supervisor entry to chain
    append_chain({
        "type": "supervisor",
        "cycle": cycle,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "stats": stats,
        "analysis": analysis[:500],
        "total_observations": len(chain),
    })

    log(f"Supervisor: {analysis[:200]}...")
    log(f"Stats: {stats}")
    log("═══ END BARGE-IN ═══")


# ── Main Loop ────────────────────────────────────────────────────────────────

def main():
    # Check kernel
    kernel_ok = False
    try:
        with urlopen(f"{KERNEL}/health", timeout=2):
            kernel_ok = True
    except Exception:
        pass

    if not kernel_ok:
        log("WARNING: CogOS kernel not running. Condition C (foveated) will fall back to stock.")
        log(f"Start kernel: ./cogos serve --workspace {WORKSPACE} --port 6931")

    log("╔══════════════════════════════════════════════════╗")
    log("║  Overnight Ralph — Continuous Ablation Loop     ║")
    log(f"║  Model:      {MODEL}")
    log(f"║  Supervisor: {SUPERVISOR_MODE} ({SUPERVISOR_MODEL})")
    log(f"║  Barge-in:   every {BARGE_IN_INTERVAL}s")
    log(f"║  Kernel:     {'yes' if kernel_ok else 'no'}")
    log(f"║  Thermal:    limit {THERMAL_LIMIT}°C, cooldown {COOL_DOWN_PAUSE}s")
    log(f"║  Output:     {OUT_DIR}")
    log("╚══════════════════════════════════════════════════╝")
    log("")

    cycle = 0
    last_barge_in = time.time()
    shuffled_questions = list(QUESTIONS)

    try:
        while True:
            cycle += 1

            # Shuffle questions each cycle for variety
            random.shuffle(shuffled_questions)

            for qi, q in enumerate(shuffled_questions):
                # Run ALL conditions on each question (paired comparison)
                for cond_id in ["A", "B", "C"]:
                    # Thermal check before each call
                    while not check_thermal():
                        pass

                    cond_name, cond_fn = CONDITIONS[cond_id]

                    log(f"C{cycle} Q{qi+1}/{len(shuffled_questions)}: {q['id']} cond={cond_id}({cond_name})")

                    t0 = time.time()
                    try:
                        response = cond_fn(MODEL, q["q"])
                    except Exception as e:
                        response = f"ERROR: {e}"
                    elapsed = round(time.time() - t0, 1)
                    sc = score(response, q["expect"])

                    entry = {
                        "type": "observation",
                        "cycle": cycle,
                        "question_id": q["id"],
                        "condition": cond_id,
                        "condition_name": cond_name,
                        "score": round(sc, 3),
                        "elapsed_sec": elapsed,
                        "model": MODEL,
                        "response_preview": response[:150],
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    }
                    append_chain(entry)

                    marker = "✓" if sc >= 0.5 else "✗"
                    log(f"  → score={sc:.2f} {marker} ({elapsed}s)")

                    # Brief pause between conditions
                    time.sleep(INTER_QUESTION_PAUSE)

                # Check if supervisor barge-in is due (between questions)
                if time.time() - last_barge_in >= BARGE_IN_INTERVAL:
                    try:
                        chain = read_chain()
                        supervisor_barge_in(chain, cycle)
                    except Exception as e:
                        log(f"Supervisor barge-in failed: {e}")
                    last_barge_in = time.time()

            log(f"Cycle {cycle} complete. Total observations: {len(read_chain())}")
            log("")

    except KeyboardInterrupt:
        log("")
        log("═══ INTERRUPTED ═══")
        chain = read_chain()
        obs = [e for e in chain if e.get("type") == "observation"]

        # Final summary
        log(f"Total observations: {len(obs)}")
        log(f"Total cycles: {cycle}")
        for cond in ["A", "B", "C"]:
            cond_entries = [e for e in obs if e.get("condition") == cond]
            if cond_entries:
                avg = sum(e["score"] for e in cond_entries) / len(cond_entries)
                log(f"  {cond} ({CONDITIONS[cond][0]}): avg={avg:.3f} n={len(cond_entries)}")

        # Write final summary
        summary = f"""# Overnight Ralph — Final Summary
Run ended: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
Model: {MODEL}
Cycles: {cycle}
Total observations: {len(obs)}
Supervisor barge-ins: {len([e for e in chain if e.get('type') == 'supervisor'])}

## Results
"""
        for cond in ["A", "B", "C"]:
            cond_entries = [e for e in obs if e.get("condition") == cond]
            if cond_entries:
                avg = sum(e["score"] for e in cond_entries) / len(cond_entries)
                summary += f"- {cond} ({CONDITIONS[cond][0]}): avg={avg:.3f} (n={len(cond_entries)})\n"

        a_avg = sum(e["score"] for e in obs if e["condition"] == "A") / max(1, len([e for e in obs if e["condition"] == "A"]))
        for cond in ["B", "C"]:
            cond_entries = [e for e in obs if e.get("condition") == cond]
            if cond_entries:
                avg = sum(e["score"] for e in cond_entries) / len(cond_entries)
                summary += f"\n{cond}-A differential: {avg - a_avg:+.3f}\n"

        SUMMARY_FILE.write_text(summary)
        log(f"Summary written to {SUMMARY_FILE}")
        log(f"Chain file: {CHAIN_FILE}")
        log(f"Supervisor guidance: {SUPERVISOR_FILE}")


if __name__ == "__main__":
    main()
