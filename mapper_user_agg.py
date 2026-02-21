#!/usr/bin/env python
import sys


def main():
    for line in sys.stdin:
        raw = line.rstrip("\n")
        if not raw:
            continue

        parts = raw.split("\t")
        if len(parts) < 11:
            sys.stderr.write("Skipping malformed session row: {0}\n".format(raw))
            continue

        user_id = parts[0]
        try:
            duration = int(parts[4])
            pageviews = int(parts[5])
        except ValueError:
            sys.stderr.write("Skipping row with non-numeric metrics: {0}\n".format(raw))
            continue

        anomaly = parts[10]
        anomalous_session = 0 if anomaly == "normal" else 1

        sys.stdout.write(
            "{0}\t1\t{1}\t{2}\t{3}\n".format(
                user_id, duration, pageviews, anomalous_session
            )
        )


if __name__ == "__main__":
    main()
