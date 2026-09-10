#!/usr/bin/env python3
"""Raw RTSP DESCRIBE/SETUP/PLAY hammer (stdlib only).

Used by prove_upstream_gst.sh to drive the fix14 dead-camera crash vector:
a never-connected camera is served the dummy factory's "Stream not Ready"
splash (videotestsrc, 500 buffers = 20 s, then EOS).  After that EOS the
cached shared media's streams are "complete senders" that never pass data
again, and every subsequent client PLAY reaches gst_rtsp_media_get_rates ->
g_assert(FALSE) -> SIGABRT of the whole neolink process (Debian bookworm
gst-rtsp-server 1.22).  This script is the client side of that hammer.

Usage: deadcam_hammer.py <host> <port> <mount> <seconds>
Prints a one-line summary: attaches / play_ok / play_err / conn_err.
"""
import socket
import sys
import time


def rtsp(sock, method, url, cseq, extra=None):
    hdrs = [f"{method} {url} RTSP/1.0", f"CSeq: {cseq}"]
    if extra:
        hdrs += extra
    sock.sendall(("\r\n".join(hdrs) + "\r\n\r\n").encode())
    sock.settimeout(5)
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
    return buf.decode("latin-1", "replace")


def attach(host, port, url):
    """One full DESCRIBE -> SETUP -> PLAY -> TEARDOWN cycle.

    Returns 'play_ok', 'play_err' (server refused somewhere -- the desired
    graceful outcome) or 'conn_err'.
    """
    s = socket.create_connection((host, port), timeout=5)
    try:
        rtsp(s, "OPTIONS", url, 1)
        d = rtsp(s, "DESCRIBE", url, 2, ["Accept: application/sdp"])
        if not d.startswith("RTSP/1.0 200"):
            return "play_err"
        setup_url = url + "/stream=0"
        r = rtsp(s, "SETUP", setup_url, 3,
                 ["Transport: RTP/AVP/TCP;unicast;interleaved=0-1"])
        if not r.startswith("RTSP/1.0 200"):
            return "play_err"
        sess = ""
        for line in r.split("\r\n"):
            if line.lower().startswith("session:"):
                sess = line.split(":", 1)[1].strip().split(";")[0]
        p = rtsp(s, "PLAY", url, 4, [f"Session: {sess}", "Range: npt=0.000-"])
        ok = p.startswith("RTSP/1.0 200")
        try:
            rtsp(s, "TEARDOWN", url, 5, [f"Session: {sess}"])
        except Exception:
            pass
        return "play_ok" if ok else "play_err"
    finally:
        try:
            s.close()
        except Exception:
            pass


def hold(host, port, url, secs):
    """Open a session, PLAY, and keep it alive without TEARDOWN.

    Keeps the shared media PREPARED across the splash EOS so that later
    attaches reuse the cached post-EOS media (the fix14 corpse class)
    instead of triggering a fresh create_element.
    """
    s = socket.create_connection((host, port), timeout=5)
    rtsp(s, "OPTIONS", url, 1)
    d = rtsp(s, "DESCRIBE", url, 2, ["Accept: application/sdp"])
    if not d.startswith("RTSP/1.0 200"):
        print("hold: DESCRIBE failed")
        return
    r = rtsp(s, "SETUP", url + "/stream=0", 3,
             ["Transport: RTP/AVP/TCP;unicast;interleaved=0-1"])
    sess = ""
    for line in r.split("\r\n"):
        if line.lower().startswith("session:"):
            sess = line.split(":", 1)[1].strip().split(";")[0]
    p = rtsp(s, "PLAY", url, 4, [f"Session: {sess}", "Range: npt=0.000-"])
    print(f"hold: session={sess} play={p.splitlines()[0] if p else 'none'}")
    end = time.time() + secs
    cseq = 5
    s.settimeout(1)
    while time.time() < end:
        try:
            s.sendall((f"OPTIONS {url} RTSP/1.0\r\nCSeq: {cseq}\r\n"
                       f"Session: {sess}\r\n\r\n").encode())
            cseq += 1
            try:
                s.recv(65536)          # drain interleaved RTP + the reply
            except socket.timeout:
                pass
        except Exception as exc:
            print(f"hold: died after {cseq} keepalives: {exc}")
            return
        time.sleep(4)
    print(f"hold: survived {cseq} keepalives")


def main():
    host, port, mount, secs = sys.argv[1], int(sys.argv[2]), sys.argv[3], float(sys.argv[4])
    url = f"rtsp://{host}:{port}{mount}"
    if len(sys.argv) > 5 and sys.argv[5] == "hold":
        hold(host, port, url, secs)
        return
    counts = {"play_ok": 0, "play_err": 0, "conn_err": 0}
    end = time.time() + secs
    while time.time() < end:
        try:
            counts[attach(host, port, url)] += 1
        except Exception:
            counts["conn_err"] += 1
            time.sleep(0.05)
    total = sum(counts.values())
    print(f"attaches={total} play_ok={counts['play_ok']} "
          f"play_err={counts['play_err']} conn_err={counts['conn_err']}")


if __name__ == "__main__":
    main()
