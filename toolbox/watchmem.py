import csv
import math
import time
import sys
from collections import deque
from dataclasses import dataclass
from numbers import Real
from typing import TextIO, Text, Tuple, List

import click
import psutil as ps


STD_FD = (sys.stdout.fileno(), sys.stderr.fileno())


def is_stream_std(stream: TextIO):
    return stream.fileno() in STD_FD


@dataclass
class WatchedResource:
    first: bool = True
    total: float = 0
    last: float = 0


@dataclass
class WatchedProcess:
    process: ps.Process
    mem: WatchedResource = WatchedResource()

    @property
    def pid(self):
        return self.process.pid

    @classmethod
    def create(cls, pid):
        return cls(ps.Process(pid))


def writeline(stream: TextIO, msg: Text, *, flush: bool = True):
    click.secho(msg, file=stream)


def write_csv_row(writer: csv.writer, parts):
    writer.writerow(parts)


def summarize(elapsed: Real, states: List[WatchedProcess]):
    for state in states:
        m = state.mem.total / elapsed
        click.echo(f"PID:                 {state.pid}", err=True)
        click.echo(f"Elapsed:             {elapsed:.4f}", err=True)
        click.echo(f"Total Memory Growth: {state.mem.total:.4f}", err=True)
        click.echo(f"Average Rate:        {m:.4f}", err=True)


def validate_pids(ctx, param, value):
    if not value:
        raise click.BadParameter("At least one PID must be passed.", ctx=ctx)
    return sorted(set(value))


def watch_options(f):
    f = click.option(
        "-f",
        "--log-file",
        type=click.File(mode="w", encoding="utf-8"),
        default="-",
        help="The log file to write to (defaults to STDOUT)",
    )(f)
    f = click.option(
        "-i",
        "--interval",
        type=float,
        default=5,
        show_default=True,
        help="Time between reads, in seconds.",
    )(f)
    click.option(
        "-d",
        "--duration",
        type=float,
        default=math.inf,
        help="How long to run, in seconds. (defaults to indefinite)",
    )(f)
    return f


@click.command()
@click.argument("pid", type=int, nargs=-1, callback=validate_pids)
@watch_options
def main(pid: Tuple[int], log_file: TextIO, interval: float, duration: float):
    """Check the memory usage of processes."""
    parts = [
        "{{0:>{0}}}",
        "{{1:>{1}}}",
        "{{2:>{2}.1f}}s",
        "{{3:> {3}.2f}}",
        "{{4:> {4}.2f}}",
        "{{5:> {5}.2f}}",
    ]
    widths = [6, 10, 10, 10, 10, 10]
    fmt = " ".join(parts)
    row = fmt.format(*widths)
    # row = "{0:>6} {1:>13.2f} {2:>10.1f}s {3:> 8.2f} {4:> 8.2f} {5:> 8.2f}"

    hdrfmt = "{0:>6} {1:>10} {2:>11} {3:>11} {4:>10} {5:>10}"
    header = hdrfmt.format(
        "PID", "Status", "Elapsed", "Memory", "Delta", "\u03A3 Delta"
    )
    writer = csv.writer(log_file)
    if not is_stream_std(log_file):
        writeline(sys.stderr, header)
        write_csv_row(writer, ["PID", "Status", "Timestamp", "Memory"])
    else:
        writeline(log_file, header)

    processes = sorted(set(pid))

    states = [WatchedProcess.create(p) for p in processes]

    def writerow(
        state: WatchedProcess,
        start_time: float,
        current_time: float,
        delta: float,
    ):
        _fmtparts = deque(row.split())
        fmtparts = list()
        while _fmtparts:
            part = _fmtparts.popleft()
            if part.count("{") and part.count("}"):
                fmtparts.append(part)
                continue
            if part.count("{") and not part.count("}"):
                rpart = _fmtparts.popleft()
                fmtparts.append(" ".join([part, rpart]))

        values = [
            state.pid,
            state.process.status(),
            current_time - start_time,
            state.mem.last,
            delta,
            state.mem.total,
        ]

        colors = [
            "cyan",
            "white",
            "white",
            "blue",
            ("red" if delta > 0 else "green"),
            ("red" if state.mem.total > 0 else "green"),
        ]
        if len(values) != len(fmtparts) != len(colors):
            return
        line = list()
        for i, elem in enumerate(zip(fmtparts, values, colors)):
            fmt, value, col = elem
            pad = [None for _ in range(i)] + [value]
            fmtval = click.style(fmt.format(*pad), fg=col)
            line.append(fmtval)
        # If the log file is not stdout or stderr
        if log_file.fileno() not in STD_FD:
            writeline(sys.stderr, " ".join(line))
            write_csv_row(
                writer,
                [
                    str(state.pid),
                    str(state.process.status()),
                    str(current_time),
                    str(state.mem.last),
                ],
            )
        else:
            writeline(log_file, " ".join(line))

    def sample(state: WatchedProcess, start_time: float):
        current_time = time.time()
        try:
            info = state.process.memory_info()
        except ps.NoSuchProcess:
            return False

        mem = info.rss / 1000
        delta = 0 if state.mem.first else mem - state.mem.last
        if state.mem.first:
            state.mem.first = False

        state.mem.total += delta
        state.mem.last = mem

        writerow(state, start_time, current_time, delta)
        return True

    start = time.time()
    while True:
        try:
            end = False
            for state in states:
                if not sample(state, start):
                    end = True
                    break
            if end:
                break
        except (KeyboardInterrupt, ps.NoSuchProcess):
            click.secho("\nBreaking", err=True, fg="red")
        else:
            elapsed = time.time() - start
            if elapsed >= duration:
                break
            time.sleep(interval)

    end = time.time()
    summarize(end - start, states)


if __name__ == "__main__":
    main()
