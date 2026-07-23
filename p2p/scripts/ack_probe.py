#!/usr/bin/env python3
"""Run clicksync-p2p as a bounded probe parent and acknowledge its events."""

import json
import subprocess
import sys
import threading


def copy_stderr(pipe) -> None:
    for line in pipe:
        sys.stderr.write(line)


def main() -> int:
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} COMMAND [ARG ...]", file=sys.stderr)
        return 2
    child = subprocess.Popen(
        sys.argv[1:],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    stderr_thread = threading.Thread(
        target=copy_stderr,
        args=(child.stderr,),
        daemon=True,
    )
    stderr_thread.start()

    session_id = None
    last_seq = 0
    try:
        for line in child.stdout:
            event = json.loads(line)
            if event.get("schema_version") != 1:
                raise RuntimeError("unexpected schema version")
            if event.get("source_seq") != last_seq + 1:
                raise RuntimeError("source sequence gap")
            if session_id is None:
                session_id = event.get("session_id")
            elif event.get("session_id") != session_id:
                raise RuntimeError("session ID changed")
            last_seq = event["source_seq"]
            sys.stdout.write(json.dumps(event, separators=(",", ":")) + "\n")
            sys.stdout.flush()
            if event.get("kind") in {"roll_forward", "roll_backward", "heartbeat"}:
                ack = {
                    "schema_version": 1,
                    "kind": "ack",
                    "session_id": session_id,
                    "source_seq": last_seq,
                }
                child.stdin.write(json.dumps(ack, separators=(",", ":")) + "\n")
                child.stdin.flush()
    except Exception as exc:
        child.kill()
        print(f"probe parent rejected child output: {exc}", file=sys.stderr)
        return 1
    finally:
        if child.stdin:
            child.stdin.close()

    rc = child.wait()
    stderr_thread.join(timeout=1)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
