from dataclasses import MISSING
from typing import Optional, Any
from os import environ


class EnvAttribute:
    """
    Descriptor class that maps an attribute of a class to an environment variable.
    """
    def __init__(self, env_var: str, default: Optional[Any] = MISSING):
        self.env_var = env_var
        self.default = default

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        if self.default is not MISSING:
            value = environ.get(self.env_var, self.default)
        else:
            value = environ.get(self.env_var)

        # if this attribute has a type annotation, use it to cast the
        # string value from the environment variable to some other type
        if getattr(instance, '__annotations__', False) and self.name in instance.__annotations__:
            return instance.__annotations__[self.name](value)
        else:
            return value
