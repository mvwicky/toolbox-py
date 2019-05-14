"""Miscellaneous Utilities."""
import os
import random
import string
import time
from functools import wraps
from pathlib import Path
from typing import Any, Callable, List, Optional, Text, Union

import requests

FILE_NAME_CHARS = string.ascii_letters + string.digits + " -"

ReqSession = Optional[requests.Session]
PathType = Union[Path, Text]
NumericType = Union[int, float]


def rand_sleep(mu: NumericType = 5, sigma: NumericType = 1) -> float:
    t = max(random.random(), random.gauss(mu, sigma))
    time.sleep(t)
    return t


def default_rand_func(
    min_sleep: Optional[float] = 1.0, max_sleep: Optional[float] = 5.0
) -> float:
    return random.uniform(min_sleep, max_sleep)


def get_request(
    url: Text,
    session: ReqSession = None,
    delay: Optional[bool] = False,
    rand_func: Optional[Callable[[], NumericType]] = None,
    rand_args: Optional[List[Any]] = list(),
    **kwargs,
) -> requests.Response:
    if session is None:
        req_func = requests.get
    else:
        req_func = session.get

    res = req_func(url, **kwargs)
    if delay and rand_func is not None:
        time.sleep(rand_func(*rand_args))

    return res


def norm_filename(
    inp: Text, allowed: Text = FILE_NAME_CHARS, max_len: int = 128
) -> Text:
    r = "".join([c for c in inp.strip() if c in allowed])
    if max_len and len(r) > max_len:
        r = r[:max_len]
    return r


def download(url: Text, path: PathType, session: ReqSession) -> int:
    res = get_request(url, session=session, stream=True)
    if not good_request(res):
        return 0
    with open(path, "wb") as f:
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    return os.path.getsize(path)


def good_request(res: requests.Response) -> bool:
    return (
        (res.status_code == requests.codes.ok)
        and ("content-length" in res.headers)  # noqa: W503
        and (res.headers["content-length"])  # noqa: W503
    )


def dirshort(obj: Any, exclude="__") -> List[Text]:
    return [e for e in dir(obj) if not e.startswith(exclude)]


TimeableFunction = Optional[Callable[..., Any]]
TimedFunction = Callable[..., Any]


def time_call(func: TimeableFunction = None) -> TimedFunction:
    if func is None:

        def decorator(func):
            return time_call(func)

        return decorator

    fp = FunctionTimer(func)

    @wraps(func)
    def new_fn(*args, **kwargs):
        return fp(*args, **kwargs)

    return new_fn


class FunctionTimer(object):
    timer = time.perf_counter

    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        func = self.func

        timer = self.timer
        start = timer()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = timer() - start
            func_name = func.__name__
            file_name = func.__code__.co_filename
            line_no = func.__code__.co_firstlineno
            print(
                f"{func_name} ({file_name}:{line_no}): {elapsed:.3f} seconds",
                func_name,
                file_name,
                line_no,
                elapsed,
            )


if __name__ == "__main__":
    pass
