#!/usr/bin/env python
import calendar
import csv
import datetime
import sys


def log_error(message):
    sys.stderr.write(str(message) + "\n")


def sanitize(value):
    return (value or "").replace("\t", " ").replace("\n", " ").strip()


def epoch_to_iso(epoch):
    return datetime.datetime.utcfromtimestamp(int(epoch)).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_dt(raw_value, formats):
    for fmt in formats:
        try:
            return datetime.datetime.strptime(raw_value, fmt)
        except ValueError:
            continue
    return None


def parse_epoch(timestamp_raw):
    raw = timestamp_raw.strip()
    if not raw:
        raise ValueError("empty timestamp")

    if raw.isdigit():
        ts = int(raw)
        if ts > 10000000000:
            ts = ts // 1000
        return ts

    # ISO with UTC suffix: 2026-02-20T10:00:00Z
    if raw.endswith("Z"):
        base = raw[:-1]
        dt = parse_dt(base, ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"])
        if dt is not None:
            return int(calendar.timegm(dt.timetuple()))

    # ISO with timezone offset: 2026-02-20T10:00:00+00:00
    if len(raw) > 6 and raw[-6] in ("+", "-") and raw[-3] == ":":
        base = raw[:-6]
        sign = 1 if raw[-6] == "+" else -1
        try:
            off_h = int(raw[-5:-3])
            off_m = int(raw[-2:])
        except ValueError:
            off_h = None
            off_m = None

        if off_h is not None and off_m is not None:
            dt = parse_dt(base, ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"])
            if dt is not None:
                epoch = int(calendar.timegm(dt.timetuple()))
                offset_seconds = sign * (off_h * 3600 + off_m * 60)
                return epoch - offset_seconds

    dt = parse_dt(raw, ["%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"])
    if dt is not None:
        return int(calendar.timegm(dt.timetuple()))

    raise ValueError("unsupported timestamp format: {0}".format(raw))


def main():
    reader = csv.DictReader(sys.stdin)
    required = set(["user_id", "event_time", "page"])
    if not reader.fieldnames:
        log_error("Input vacio o sin cabecera CSV")
        return

    missing = required - set(reader.fieldnames)
    if missing:
        raise ValueError(
            "Faltan columnas requeridas: {0}. Cabeceras recibidas: {1}".format(
                ", ".join(sorted(missing)), reader.fieldnames
            )
        )

    row_number = 1
    for row in reader:
        row_number += 1
        user_id = sanitize(row.get("user_id", ""))
        page = sanitize(row.get("page", ""))
        event_time = sanitize(row.get("event_time", ""))
        event_type = sanitize(row.get("event_type", "unknown")) or "unknown"
        device = sanitize(row.get("device", "unknown")) or "unknown"

        if not user_id or not page or not event_time:
            log_error("Linea {0} invalida: {1}".format(row_number, row))
            continue

        try:
            epoch = parse_epoch(event_time)
        except ValueError as exc:
            log_error("Linea {0}: {1}".format(row_number, exc))
            continue

        event_iso = epoch_to_iso(epoch)
        sys.stdout.write(
            "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(
                user_id, epoch, event_iso, page, event_type, device
            )
        )


if __name__ == "__main__":
    main()
