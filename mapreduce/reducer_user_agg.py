#!/usr/bin/env python
import sys


def flush(user_id, sessions, total_duration, total_pageviews, anomalous_sessions):
    if sessions == 0:
        return

    sessions_f = float(sessions)
    avg_duration = total_duration / sessions_f
    avg_pages = total_pageviews / sessions_f
    anomalous_rate = anomalous_sessions / sessions_f

    out = [
        user_id,
        str(sessions),
        str(total_duration),
        str(total_pageviews),
        "{0:.2f}".format(avg_duration),
        "{0:.2f}".format(avg_pages),
        str(anomalous_sessions),
        "{0:.4f}".format(anomalous_rate),
    ]
    sys.stdout.write("\t".join(out) + "\n")


def main():
    current_user = None
    sessions = 0
    total_duration = 0
    total_pageviews = 0
    anomalous_sessions = 0

    for line in sys.stdin:
        raw = line.rstrip("\n")
        if not raw:
            continue

        parts = raw.split("\t")
        if len(parts) < 5:
            sys.stderr.write("Skipping malformed aggregate row: {0}\n".format(raw))
            continue

        user_id = parts[0]
        try:
            session_count = int(parts[1])
            duration = int(parts[2])
            pageviews = int(parts[3])
            anomalous = int(parts[4])
        except ValueError:
            sys.stderr.write("Skipping row with invalid metrics: {0}\n".format(raw))
            continue

        if current_user is None:
            current_user = user_id

        if user_id != current_user:
            flush(current_user, sessions, total_duration, total_pageviews, anomalous_sessions)
            current_user = user_id
            sessions = 0
            total_duration = 0
            total_pageviews = 0
            anomalous_sessions = 0

        sessions += session_count
        total_duration += duration
        total_pageviews += pageviews
        anomalous_sessions += anomalous

    if current_user is not None:
        flush(current_user, sessions, total_duration, total_pageviews, anomalous_sessions)


if __name__ == "__main__":
    main()
