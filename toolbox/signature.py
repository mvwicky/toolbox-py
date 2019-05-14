from inspect import Parameter, Signature, currentframe
from typing import Text, TypeVar

T = TypeVar("T")


def get_param_default(sig: Signature, param_name: Text, default: T) -> T:
    param = sig.parameters.get(param_name)
    if param is not None and param.default is not Parameter.empty:
        return param.default
    else:
        return default


def lineno() -> int:
    frame = currentframe()
    if frame is None:
        return -1
    return frame.f_back.f_lineno
