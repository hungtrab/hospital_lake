"""Spark jobs run inside apache/spark:3.5.5-python3, whose interpreter is Python 3.8.

Python 3.8 evaluates annotations at import time, so PEP 585/604 forms such as
``list[dict]`` or ``str | None`` crash the job before Spark starts unless the
module uses ``from __future__ import annotations``.
"""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PEP585_BUILTINS = {"list", "dict", "tuple", "set", "frozenset", "type"}


def _runtime_annotations(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            arguments = node.args.posonlyargs + node.args.args + node.args.kwonlyargs
            arguments += [arg for arg in (node.args.vararg, node.args.kwarg) if arg]
            yield from (arg.annotation for arg in arguments if arg.annotation)
            if node.returns:
                yield node.returns
        elif isinstance(node, ast.AnnAssign):
            yield node.annotation


def _py38_incompatible(annotation):
    for node in ast.walk(annotation):
        if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Name) \
                and node.value.id in PEP585_BUILTINS:
            return True
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            return True
    return False


def test_spark_container_modules_import_on_python38():
    offenders = []
    for path in sorted([*ROOT.glob("spark/**/*.py"), *ROOT.glob("scripts/*.py")]):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        postponed = any(isinstance(node, ast.ImportFrom) and node.module == "__future__"
                        and any(alias.name == "annotations" for alias in node.names)
                        for node in tree.body)
        if postponed:
            continue
        for annotation in _runtime_annotations(tree):
            if _py38_incompatible(annotation):
                offenders.append(f"{path.relative_to(ROOT)}:{annotation.lineno}")
    assert not offenders, (
        "Add `from __future__ import annotations` (Spark image runs Python 3.8): "
        + ", ".join(offenders))
