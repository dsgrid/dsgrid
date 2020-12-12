
from dataclasses import dataclass

@dataclass(frozen=True)
class OneToMany:
    from_type: str
    from_field: str
    to_type: str
    to_field: str


@dataclass(frozen=True)
class OneToOne:
    from_type: str
    from_field: str
    to_type: str
    to_field: str


@dataclass(frozen=True)
class ProgrammaticOneToOne:
    from_type: str
    convert_func: str  # really, a function. What is its type?
    to_type: str
    to_field: str
