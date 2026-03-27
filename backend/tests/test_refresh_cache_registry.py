"""Parity test: refresh_cache SOURCE_REGISTRY vs pull_with_cache callsites.

This test is static analysis only. It does not execute data pulls.
"""
from __future__ import annotations

import ast
import importlib
import json
import sys
from pathlib import Path
from typing import Any


BACKEND_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = BACKEND_ROOT / "src"

if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from src.scripts.refresh_cache import SOURCE_REGISTRY


UNRESOLVED = object()
EXCLUDED_FILES = {
    SRC_ROOT / "scripts" / "refresh_cache.py",
    SRC_ROOT / "utils" / "cache_utils.py",
}


def _normalize_kwargs(kwargs: dict[str, Any]) -> str:
    return json.dumps(kwargs, sort_keys=True, default=str)


def _registry_pairs() -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for source_name, _pull_fn, pull_kwargs in SOURCE_REGISTRY:
        pairs.add((source_name, _normalize_kwargs(pull_kwargs)))
    return pairs


def _safe_import(module_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except Exception:
        return UNRESOLVED


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _eval_expr(node: ast.AST | None, env: dict[str, Any]) -> Any:
    if node is None:
        return UNRESOLVED

    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Name):
        return env.get(node.id, UNRESOLVED)

    if isinstance(node, ast.Attribute):
        base = _eval_expr(node.value, env)
        if base is UNRESOLVED:
            return UNRESOLVED
        try:
            return getattr(base, node.attr)
        except Exception:
            return UNRESOLVED

    if isinstance(node, ast.Dict):
        out: dict[Any, Any] = {}
        for key_node, val_node in zip(node.keys, node.values):
            key = _eval_expr(key_node, env)
            val = _eval_expr(val_node, env)
            if key is UNRESOLVED or val is UNRESOLVED:
                return UNRESOLVED
            out[key] = val
        return out

    if isinstance(node, ast.List):
        vals = [_eval_expr(e, env) for e in node.elts]
        return UNRESOLVED if any(v is UNRESOLVED for v in vals) else vals

    if isinstance(node, ast.Tuple):
        vals = [_eval_expr(e, env) for e in node.elts]
        return UNRESOLVED if any(v is UNRESOLVED for v in vals) else tuple(vals)

    if isinstance(node, ast.Set):
        vals = [_eval_expr(e, env) for e in node.elts]
        return UNRESOLVED if any(v is UNRESOLVED for v in vals) else set(vals)

    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _eval_expr(node.left, env)
        right = _eval_expr(node.right, env)
        if left is UNRESOLVED or right is UNRESOLVED:
            return UNRESOLVED
        try:
            return left + right
        except Exception:
            return UNRESOLVED

    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        val = _eval_expr(node.operand, env)
        if val is UNRESOLVED:
            return UNRESOLVED
        try:
            return -val
        except Exception:
            return UNRESOLVED

    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for v in node.values:
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                parts.append(v.value)
                continue
            if isinstance(v, ast.FormattedValue):
                fv = _eval_expr(v.value, env)
                if fv is UNRESOLVED:
                    return UNRESOLVED
                parts.append(str(fv))
                continue
            return UNRESOLVED
        return "".join(parts)

    if isinstance(node, ast.Compare) and len(node.ops) == 1 and len(node.comparators) == 1:
        left = _eval_expr(node.left, env)
        right = _eval_expr(node.comparators[0], env)
        if left is UNRESOLVED or right is UNRESOLVED:
            return UNRESOLVED
        op = node.ops[0]
        if isinstance(op, ast.Is):
            return left is right
        if isinstance(op, ast.IsNot):
            return left is not right
        if isinstance(op, ast.Eq):
            return left == right
        if isinstance(op, ast.NotEq):
            return left != right
        return UNRESOLVED

    if isinstance(node, ast.Call):
        fn = _eval_expr(node.func, env)
        fn_name = _call_name(node.func)

        # dict(...) literal constructor
        if fn_name == "dict":
            out: dict[str, Any] = {}
            for kw in node.keywords:
                if kw.arg is None:
                    return UNRESOLVED
                val = _eval_expr(kw.value, env)
                if val is UNRESOLVED:
                    return UNRESOLVED
                out[kw.arg] = val
            return out

        # common string normalization in f-strings
        if isinstance(node.func, ast.Attribute):
            base = _eval_expr(node.func.value, env)
            method = node.func.attr
            if (
                isinstance(base, str)
                and method in {"lower", "upper", "strip"}
                and not node.args
                and not node.keywords
            ):
                return getattr(base, method)()

        # specific safe constructor used in forecast pipeline
        if callable(fn) and getattr(fn, "__name__", "") == "ScenarioConfig":
            args = [_eval_expr(a, env) for a in node.args]
            kwargs = {
                kw.arg: _eval_expr(kw.value, env)
                for kw in node.keywords
                if kw.arg is not None
            }
            if any(v is UNRESOLVED for v in args) or any(
                v is UNRESOLVED for v in kwargs.values()
            ):
                return UNRESOLVED
            try:
                return fn(*args, **kwargs)
            except Exception:
                return UNRESOLVED

        return UNRESOLVED

    return UNRESOLVED


def _assign_target(env: dict[str, Any], target: ast.AST, value: Any) -> None:
    if isinstance(target, ast.Name):
        env[target.id] = value
        return
    if isinstance(target, (ast.Tuple, ast.List)) and isinstance(value, (tuple, list)):
        for t, v in zip(target.elts, value):
            _assign_target(env, t, v)


def _get_call_arg_expr(
    call: ast.Call,
    param_name: str,
    positional_index: int,
) -> ast.AST | None:
    for kw in call.keywords:
        if kw.arg == param_name:
            return kw.value
    if len(call.args) > positional_index:
        return call.args[positional_index]
    return None


def _find_wrapper_functions(tree: ast.Module) -> dict[str, list[str]]:
    wrappers: dict[str, list[str]] = {}
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        param_names = [a.arg for a in (node.args.posonlyargs + node.args.args)]
        for sub in ast.walk(node):
            if not isinstance(sub, ast.Call):
                continue
            if _call_name(sub.func) != "pull_with_cache":
                continue
            src_expr = _get_call_arg_expr(sub, "source_name", 0)
            kw_expr = _get_call_arg_expr(sub, "pull_kwargs", 2)
            if (
                isinstance(src_expr, ast.Name)
                and src_expr.id in param_names
                and isinstance(kw_expr, ast.Name)
                and kw_expr.id in param_names
            ):
                wrappers[node.name] = param_names
                break
    return wrappers


def _build_module_env(tree: ast.Module) -> dict[str, Any]:
    env: dict[str, Any] = {"None": None, "True": True, "False": False}

    for stmt in tree.body:
        if isinstance(stmt, ast.Import):
            for alias in stmt.names:
                full_name = alias.name
                bound_name = alias.asname or full_name.split(".")[0]
                if full_name == "src.like_day_forecast.configs":
                    env[bound_name] = _safe_import(full_name)
                elif full_name == "src.like_day_forecast":
                    env[bound_name] = _safe_import(full_name)
                else:
                    env[bound_name] = UNRESOLVED
        elif isinstance(stmt, ast.ImportFrom):
            if stmt.module is None:
                continue
            for alias in stmt.names:
                if alias.name == "*":
                    continue
                bound_name = alias.asname or alias.name
                if stmt.module == "src.like_day_forecast" and alias.name == "configs":
                    mod = _safe_import(stmt.module)
                    env[bound_name] = getattr(mod, alias.name, UNRESOLVED)
                else:
                    env[bound_name] = UNRESOLVED
        elif isinstance(stmt, ast.Assign):
            val = _eval_expr(stmt.value, env)
            for target in stmt.targets:
                _assign_target(env, target, val)
        elif isinstance(stmt, ast.AnnAssign):
            val = _eval_expr(stmt.value, env) if stmt.value is not None else UNRESOLVED
            _assign_target(env, stmt.target, val)

    return env


def _bind_call_arguments(call: ast.Call, param_names: list[str]) -> dict[str, ast.AST]:
    bound: dict[str, ast.AST] = {}
    for idx, arg in enumerate(call.args):
        if idx < len(param_names):
            bound[param_names[idx]] = arg
    for kw in call.keywords:
        if kw.arg is not None:
            bound[kw.arg] = kw.value
    return bound


def _extract_pair_from_call(
    call: ast.Call,
    env: dict[str, Any],
    wrappers: dict[str, list[str]],
) -> tuple[str, dict[str, Any]] | None:
    callee = _call_name(call.func)
    src_expr: ast.AST | None = None
    kw_expr: ast.AST | None = None

    if callee == "pull_with_cache":
        src_expr = _get_call_arg_expr(call, "source_name", 0)
        kw_expr = _get_call_arg_expr(call, "pull_kwargs", 2)
    elif callee in wrappers:
        bound = _bind_call_arguments(call, wrappers[callee])
        src_expr = bound.get("source_name")
        kw_expr = bound.get("pull_kwargs")
    else:
        return None

    source_name = _eval_expr(src_expr, env)
    pull_kwargs = _eval_expr(kw_expr, env)
    if not isinstance(source_name, str) or not isinstance(pull_kwargs, dict):
        return None
    return source_name, pull_kwargs


def _record_calls_in_expr(
    node: ast.AST,
    env: dict[str, Any],
    wrappers: dict[str, list[str]],
    out_pairs: set[tuple[str, str]],
) -> None:
    if isinstance(node, ast.Call):
        pair = _extract_pair_from_call(node, env, wrappers)
        if pair is not None:
            source_name, kwargs = pair
            out_pairs.add((source_name, _normalize_kwargs(kwargs)))

    for child in ast.iter_child_nodes(node):
        _record_calls_in_expr(child, env, wrappers, out_pairs)


def _walk_statements(
    statements: list[ast.stmt],
    env: dict[str, Any],
    wrappers: dict[str, list[str]],
    out_pairs: set[tuple[str, str]],
) -> None:
    for stmt in statements:
        if isinstance(stmt, ast.Assign):
            _record_calls_in_expr(stmt.value, env, wrappers, out_pairs)
            value = _eval_expr(stmt.value, env)
            for target in stmt.targets:
                _assign_target(env, target, value)
            continue

        if isinstance(stmt, ast.AnnAssign):
            if stmt.value is not None:
                _record_calls_in_expr(stmt.value, env, wrappers, out_pairs)
                value = _eval_expr(stmt.value, env)
                _assign_target(env, stmt.target, value)
            continue

        if isinstance(stmt, ast.Expr):
            _record_calls_in_expr(stmt.value, env, wrappers, out_pairs)
            continue

        if isinstance(stmt, ast.Return):
            if stmt.value is not None:
                _record_calls_in_expr(stmt.value, env, wrappers, out_pairs)
            continue

        if isinstance(stmt, ast.For):
            iter_values = _eval_expr(stmt.iter, env)
            if isinstance(iter_values, (list, tuple, set)):
                for item in iter_values:
                    loop_env = dict(env)
                    _assign_target(loop_env, stmt.target, item)
                    _walk_statements(stmt.body, loop_env, wrappers, out_pairs)
                _walk_statements(stmt.orelse, dict(env), wrappers, out_pairs)
            else:
                loop_env = dict(env)
                _assign_target(loop_env, stmt.target, UNRESOLVED)
                _walk_statements(stmt.body, loop_env, wrappers, out_pairs)
                _walk_statements(stmt.orelse, dict(env), wrappers, out_pairs)
            continue

        if isinstance(stmt, ast.If):
            cond = _eval_expr(stmt.test, env)
            if cond is True:
                _walk_statements(stmt.body, dict(env), wrappers, out_pairs)
            elif cond is False:
                _walk_statements(stmt.orelse, dict(env), wrappers, out_pairs)
            else:
                _walk_statements(stmt.body, dict(env), wrappers, out_pairs)
                _walk_statements(stmt.orelse, dict(env), wrappers, out_pairs)
            continue

        if isinstance(stmt, ast.Try):
            _walk_statements(stmt.body, dict(env), wrappers, out_pairs)
            for handler in stmt.handlers:
                _walk_statements(handler.body, dict(env), wrappers, out_pairs)
            _walk_statements(stmt.orelse, dict(env), wrappers, out_pairs)
            _walk_statements(stmt.finalbody, dict(env), wrappers, out_pairs)
            continue

        if isinstance(stmt, ast.With):
            _walk_statements(stmt.body, dict(env), wrappers, out_pairs)
            continue

        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue

        for child in ast.iter_child_nodes(stmt):
            if isinstance(child, ast.AST):
                _record_calls_in_expr(child, env, wrappers, out_pairs)


def _function_env(func: ast.FunctionDef, module_env: dict[str, Any]) -> dict[str, Any]:
    env = dict(module_env)

    args = func.args.posonlyargs + func.args.args
    defaults = [None] * (len(args) - len(func.args.defaults)) + list(func.args.defaults)
    for arg, default in zip(args, defaults):
        if default is not None:
            env[arg.arg] = _eval_expr(default, env)

    for kwarg, default in zip(func.args.kwonlyargs, func.args.kw_defaults):
        if default is not None:
            env[kwarg.arg] = _eval_expr(default, env)

    return env


def _pairs_from_module(path: Path) -> set[tuple[str, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    module_env = _build_module_env(tree)
    wrappers = _find_wrapper_functions(tree)

    pairs: set[tuple[str, str]] = set()

    top_level = [
        n
        for n in tree.body
        if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    _walk_statements(top_level, dict(module_env), wrappers, pairs)

    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            env = _function_env(node, module_env)
            _walk_statements(node.body, env, wrappers, pairs)

    return pairs


def _scan_source_pairs() -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for path in SRC_ROOT.rglob("*.py"):
        if path in EXCLUDED_FILES:
            continue
        pairs |= _pairs_from_module(path)
    return pairs


def _format_diff(
    title: str,
    entries: list[tuple[str, str]],
) -> str:
    lines = [title]
    for source_name, kwargs_json in entries:
        lines.append(f"  - {source_name} {kwargs_json}")
    return "\n".join(lines)


def test_refresh_cache_registry_parity() -> None:
    discovered_pairs = _scan_source_pairs()
    registry_pairs = _registry_pairs()

    missing = sorted(discovered_pairs - registry_pairs)
    extra = sorted(registry_pairs - discovered_pairs)

    details: list[str] = []
    if missing:
        details.append(_format_diff("Missing in SOURCE_REGISTRY:", missing))
    if extra:
        details.append(_format_diff("Extra in SOURCE_REGISTRY:", extra))

    assert not missing and not extra, "\n\n".join(details)
