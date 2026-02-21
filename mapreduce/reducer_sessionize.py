#!/usr/bin/env python
import datetime
import os
import sys


def sanitize(value):
    return (value or "").replace("\t", " ").replace("\n", " ").strip()


def event_iso(epoch):
    return datetime.datetime.utcfromtimestamp(int(epoch)).strftime("%Y-%m-%dT%H:%M:%SZ")


def anomaly_flags(duration, pageviews, min_gap):
    flags = []
    if pageviews == 1 and duration <= 10:
        flags.append("bounce")
    elif pageviews == 1:
        flags.append("single_page")

    if duration >= 7200:
        flags.append("long_session")
    if min_gap is not None and min_gap <= 2 and pageviews >= 5:
        flags.append("rapid_clicking")
    if pageviews >= 25:
        flags.append("high_depth")

    if flags:
        return "|".join(flags)
    return "normal"


def flush_session(user_id, session_index, events, min_gap):
    if not events:
        return

    start_epoch = events[0][0]
    end_epoch = events[-1][0]
    duration = max(0, end_epoch - start_epoch)

    pages = [event[2] for event in events]
    pageviews = len(pages)
    unique_pages = len(set(pages))
    entry_page = pages[0]
    exit_page = pages[-1]
    path_sequence = ">".join(pages[:10])

    start_tag = datetime.datetime.utcfromtimestamp(start_epoch).strftime("%Y%m%dT%H%M%S")
    session_id = "{0}_{1}_{2:03d}".format(user_id, start_tag, session_index)
    flags = anomaly_flags(duration, pageviews, min_gap)

    out = [
        user_id,
        session_id,
        event_iso(start_epoch),
        event_iso(end_epoch),
        str(duration),
        str(pageviews),
        str(unique_pages),
        sanitize(entry_page),
        sanitize(exit_page),
        sanitize(path_sequence),
        flags,
    ]
    sys.stdout.write("\t".join(out) + "\n")


def main():
    session_gap_minutes = int(os.getenv("SESSION_GAP_MINUTES", "30"))
    gap_seconds = session_gap_minutes * 60

    current_user = None
    events = []
    last_epoch = None
    min_gap_in_session = None
    session_index = 0

    for line in sys.stdin:
        raw = line.rstrip("\n")
        if not raw:
            continue

        parts = raw.split("\t")
        if len(parts) < 6:
            sys.stderr.write("Skipping malformed line: {0}\n".format(raw))
            continue

        user_id = sanitize(parts[0])
        try:
            epoch = int(parts[1])
        except ValueError:
            sys.stderr.write("Skipping line with invalid epoch: {0}\n".format(raw))
            continue

        iso_time = sanitize(parts[2])
        page = sanitize(parts[3])
        event_type = sanitize(parts[4])
        device = sanitize(parts[5])

        if current_user is None:
            current_user = user_id
            session_index = 1
            events = [(epoch, iso_time, page, event_type, device)]
            last_epoch = epoch
            min_gap_in_session = None
            continue

        if user_id != current_user:
            flush_session(current_user, session_index, events, min_gap_in_session)
            current_user = user_id
            session_index = 1
            events = [(epoch, iso_time, page, event_type, device)]
            last_epoch = epoch
            min_gap_in_session = None
            continue

        gap = 0 if last_epoch is None else max(0, epoch - last_epoch)
        if last_epoch is not None and gap > gap_seconds:
            flush_session(current_user, session_index, events, min_gap_in_session)
            session_index += 1
            events = [(epoch, iso_time, page, event_type, device)]
            last_epoch = epoch
            min_gap_in_session = None
            continue

        if min_gap_in_session is None:
            min_gap_in_session = gap
        else:
            min_gap_in_session = min(min_gap_in_session, gap)

        events.append((epoch, iso_time, page, event_type, device))
        last_epoch = epoch

    if current_user is not None:
        flush_session(current_user, session_index, events, min_gap_in_session)


if __name__ == "__main__":
    main()
