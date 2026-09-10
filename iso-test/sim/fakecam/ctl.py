#!/usr/bin/env python3
"""Send commands to a running fakecam's control port.

Usage: ctl.py [host:port] <command> [<command> ...]

Commands are normal, freeze, hang, die and status. Several may be given, in
which case they are sent in order; prefix one with a number and a comma to
delay it, e.g. `10,freeze` waits ten seconds first. That is enough to script
a whole scenario without a supervisor:

    ctl.py 127.0.0.1:9010 5,freeze 20,normal
"""
import socket
import sys
import time


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print(__doc__, file=sys.stderr)
        return 2
    addr = "127.0.0.1:9010"
    if ":" in args[0] and not args[0].split(",")[-1].isalpha():
        addr = args.pop(0)
    if not args:
        print(__doc__, file=sys.stderr)
        return 2
    host, _, port = addr.partition(":")

    with socket.create_connection((host, int(port)), timeout=5) as sock:
        sock.settimeout(5)
        banner = sock.recv(4096).decode(errors="replace").strip()
        print(banner, file=sys.stderr)
        for arg in args:
            delay, _, cmd = arg.rpartition(",")
            if delay:
                time.sleep(float(delay))
            sock.sendall((cmd + "\n").encode())
            print(sock.recv(4096).decode(errors="replace").strip())
    return 0


if __name__ == "__main__":
    sys.exit(main())
