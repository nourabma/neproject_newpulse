from __future__ import annotations

import atexit
import os
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)
os.makedirs("data/incoming", exist_ok=True)

processes: list[subprocess.Popen] = []


def _shutdown() -> None:
    for p in processes:
        if p.poll() is None:
            try:
                p.terminate()
            except Exception:
                pass
    deadline = time.time() + 5
    for p in processes:
        try:
            p.wait(timeout=max(0.1, deadline - time.time()))
        except Exception:
            try:
                p.kill()
            except Exception:
                pass


atexit.register(_shutdown)

for sig in (signal.SIGINT, signal.SIGTERM):
    try:
        signal.signal(sig, lambda *_: (_shutdown(), sys.exit(0)))
    except (OSError, ValueError):
        pass


def main() -> None:
    print("[run] spawning RSS ingester")
    processes.append(subprocess.Popen([sys.executable, "ingester.py"]))

    time.sleep(6)

    print("[run] launching Streamlit dashboard at http://localhost:8501")
    streamlit = subprocess.Popen(
        [
            sys.executable, "-m", "streamlit", "run", "app.py",
            "--browser.gatherUsageStats=false",
        ]
    )
    processes.append(streamlit)

    try:
        streamlit.wait()
    except KeyboardInterrupt:
        print("[run] interrupted, shutting down")


if __name__ == "__main__":
    main()
