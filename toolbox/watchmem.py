import time
import math
from dataclasses import dataclass
from numbers import Real
from typing import TextIO, Text, Tuple, List

import click
import memory_profiler as mp


@dataclass
class WatchedProcess:
    pid: int
    first: bool = True
    total: float = 0
    last: float = 0


def writeline(stream: TextIO, msg: Text, *, flush: bool = True):
    stream.write(msg + "\n")
    if flush:
        stream.flush()


def summarize(elapsed: Real, states: List[WatchedProcess]):
    for state in states:
        m = state.total / elapsed
        click.echo(f"PID:                 {state.pid}", err=True)
        click.echo(f"Elapsed:             {elapsed:.4f}", err=True)
        click.echo(f"Total Memory Growth: {state.total:.4f}", err=True)
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
    row = "{0:>6} {1:>13.2f} {2:>10.1f}s {3:> 8.2f} {4:> 8.2f} {5:> 8.2f}"

    hdrfmt = "{0:>6} {1:>13} {2:>11} {3:>8} {4:>8} {5:>8}"
    header = hdrfmt.format(
        "PID", "Timestamp", "Elapsed", "Memory", "Delta", "\u03A3 Delta"
    )
    writeline(log_file, header)

    processes = sorted(set(pid))

    states = [WatchedProcess(p) for p in processes]

    def sample(state: WatchedProcess, start_time: float):
        mem, ts = mp.memory_usage(proc=state.pid, timestamps=True)[0]
        delta = 0 if state.first else mem - state.last
        if state.first:
            state.first = False

        state.total += delta
        state.last = mem
        current_time = time.time()
        msg = row.format(
            state.pid,
            current_time,
            current_time - start_time,
            mem,
            delta,
            state.total,
        )
        writeline(log_file, msg)

    start = time.time()
    while True:
        try:
            for state in states:
                sample(state, start)
        except (KeyboardInterrupt, click.Abort):
            click.secho("\nBreaking", err=True, fg="red")
            end = time.time()
            summarize(end - start, states)
        else:
            elapsed = time.time() - start
            if elapsed >= duration:
                break
            time.sleep(interval)


if __name__ == "__main__":
    main()
