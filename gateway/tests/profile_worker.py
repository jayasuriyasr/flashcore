"""Profile one uvicorn worker of this gateway replica with py-spy while load is running.

    docker compose exec --privileged gateway pip install py-spy
    docker compose exec --privileged gateway python -m tests.profile_worker 15

Prints the functions where the worker spends the most CPU (own time and inclusive time).
"""
import collections
import os
import subprocess
import sys


def worker_pids():
    pids = []
    for p in os.listdir("/proc"):
        if not p.isdigit():
            continue
        try:
            cmd = open(f"/proc/{p}/cmdline", "rb").read().replace(b"\0", b" ").decode()
        except OSError:
            continue
        if "multiprocessing" in cmd and "spawn_main" in cmd:  # uvicorn worker processes
            pids.append(int(p))
    return pids


def main(seconds: int):
    pids = worker_pids()
    if not pids:
        sys.exit("no uvicorn worker processes found")
    pid = pids[0]
    out = "/tmp/profile.txt"
    subprocess.run(
        ["py-spy", "record", "--pid", str(pid), "--duration", str(seconds), "--rate", "200",
         "--format", "raw", "--output", out, "--nonblocking"],
        check=True, capture_output=True,
    )
    own, inclusive, total = collections.Counter(), collections.Counter(), 0
    for line in open(out):
        stack, _, count = line.rstrip().rpartition(" ")
        n = int(count)
        frames = stack.split(";")
        total += n
        own[frames[-1]] += n
        for frame in set(frames):
            inclusive[frame] += n
    print(f"worker pid {pid}, {total} samples\n\nTOP OWN TIME")
    for frame, n in own.most_common(15):
        print(f"{100 * n / total:5.1f}%  {frame[:110]}")
    print("\nTOP INCLUSIVE TIME")
    for frame, n in inclusive.most_common(25):
        print(f"{100 * n / total:5.1f}%  {frame[:110]}")


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 15)
