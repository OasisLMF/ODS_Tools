"""Generate settings-schema reference pages from the JSON Schemas (single source of truth).

ODS_Tools owns the settings schemas in ``ods_tools/data/*_settings_schema.json``. Rather than
hand-maintain field tables, this extension walks each schema at build time and writes MyST
Markdown into ``reference/_generated/`` which the reference pages include. Edit the schema
JSON, not the generated Markdown.

Runs on the Sphinx ``config-inited`` event; also runnable standalone for quick checks.
"""
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, os.pardir, os.pardir, os.pardir))
DATA_DIR = os.path.join(REPO_ROOT, "ods_tools", "data")
OUT_DIR = os.path.join(HERE, os.pardir, "reference", "_generated")

SCHEMAS = [
    ("model_settings_schema.json", "model_settings.md"),
    ("analysis_settings_schema.json", "analysis_settings.md"),
    ("combine_settings_schema.json", "combine_settings.md"),
]

MAX_DEPTH = 4  # how deep to expand nested objects into their own subsections
_dropped = []  # paths elided by MAX_DEPTH, reported by run() rather than dropped silently


def _cell(text):
    """Flatten a schema string into a single Markdown table cell.

    Angle brackets are escaped so placeholders survive the MyST parser: it reads ``<id>`` as a
    raw HTML tag and drops it, turning ``events_<id>.bin`` into ``events_.bin``. Only the text
    outside code spans is escaped, because inside backticks an entity is not decoded and would
    render as a literal ``&lt;``.

    Args:
        text: Any schema value — a description, type or constraint string.

    Returns:
        str: The value on one line, safe to place between table pipes.
    """
    text = str(text).replace("|", "\\|").replace("\n", " ").replace("\r", " ").strip()
    parts = text.split("`")
    if len(parts) % 2 == 0:
        # An odd number of backticks: there is no consistent inside/outside split to make, and
        # guessing inverts the parity and escapes exactly the wrong halves. Escape the lot.
        return text.replace("<", "&lt;").replace(">", "&gt;")
    for i in range(0, len(parts), 2):  # even indexes fall outside code spans
        parts[i] = parts[i].replace("<", "&lt;").replace(">", "&gt;")
    return "`".join(parts)


def _literal(value):
    """Render a schema value as the JSON a reader would type, not as a Python repr.

    These pages document JSON files, so ``True``/``None`` and bare strings are wrong: pasting
    ``default True`` into an analysis_settings.json is a syntax error, and ``default csv`` omits
    the quotes the file needs.

    Args:
        value: Any JSON-compatible value from the schema (default, const or enum member).

    Returns:
        str: The value in JSON form — ``true``, ``null``, ``"csv"``, ``1.5``.
    """
    return json.dumps(value)


def _resolve(schema, root):
    """Follow a ``$ref`` (and merge a single-branch allOf) to a concrete schema."""
    seen = 0
    while isinstance(schema, dict) and "$ref" in schema and seen < 10:
        ref = schema["$ref"]
        node = root
        for part in ref.lstrip("#/").split("/"):
            if isinstance(node, dict) and part in node:
                node = node[part]
            else:
                return schema  # unresolvable; leave as-is
        schema = node
        seen += 1
    if isinstance(schema, dict) and "allOf" in schema and len(schema["allOf"]) == 1:
        merged = {k: v for k, v in schema.items() if k != "allOf"}
        merged.update(_resolve(schema["allOf"][0], root))
        return merged
    return schema


def _type_str(s):
    if "enum" in s:
        # Keep the underlying type: an enum of integers and an enum of strings are otherwise
        # indistinguishable in the Type column.
        t = s.get("type")
        if isinstance(t, list):
            t = " / ".join(t)
        if not t:
            kinds = sorted({type(e).__name__ for e in s["enum"]})
            t = {"str": "string", "int": "integer", "float": "number", "bool": "boolean"}.get(
                kinds[0], kinds[0]) if len(kinds) == 1 else None
        return f"enum[{t}]" if t else "enum"
    t = s.get("type")
    if isinstance(t, list):
        return " / ".join(t)
    if t == "array":
        items = s.get("items", {})
        it = items.get("type") if isinstance(items, dict) else None
        return f"array[{it}]" if it else "array"
    if t:
        return t
    for k in ("oneOf", "anyOf"):
        if k in s:
            return " / ".join(sorted({b.get("type", "object") for b in s[k] if isinstance(b, dict)}))
    return "object" if "properties" in s else ""


# JSON Schema validation keywords, in the order they read best in a table cell.
_LIMITS = (
    ("minimum", "minimum"),
    ("maximum", "maximum"),
    ("exclusiveMinimum", "exclusive minimum"),
    ("exclusiveMaximum", "exclusive maximum"),
    ("multipleOf", "multiple of"),
    ("minLength", "min length"),
    ("maxLength", "max length"),
    ("minItems", "min items"),
    ("maxItems", "max items"),
    ("minProperties", "min properties"),
    ("maxProperties", "max properties"),
)


def _constraints(s, root=None):
    """Summarise a property's validation keywords for the Constraints column.

    Composite properties need more than one pass, because the keywords that matter sit on a
    nested schema rather than the property itself:

    * Arrays: ``minItems``/``uniqueItems`` constrain the list, while allowed values and
      per-value limits sit on ``items`` and are reported as "each item ...". Without that,
      ``items.enum`` lists — e.g. combine's ``group_event_set_fields`` — render as an empty
      cell, which is the one thing a generated reference must not do.
    * ``patternProperties``: a map whose keys are described by a regex. Reported as "keys
      matching ``<pattern>``" plus "each value ...", so ``vulnerability_adjustments`` says what
      the keys are and what a value may hold instead of just "object".
    * ``prefixItems``: a positional tuple, where each entry constrains one slot. Reported as
      "in order: ...", which is the only way ``replace_data``'s [integer, integer, number]
      triples appear at all.

    Args:
        s (dict): The (already ``$ref``-resolved) schema for one property.
        root (dict): The whole schema document, for resolving a ``$ref`` under ``items``.

    Returns:
        str: Semicolon-separated constraints, or "" if the property has none.
    """
    bits = []
    if "enum" in s:
        bits.append("one of: " + ", ".join(f"`{_literal(e)}`" for e in s["enum"]))
    if "const" in s:
        bits.append(f"always `{_literal(s['const'])}`")
    if "default" in s:
        bits.append(f"default `{_literal(s['default'])}`")
    for key, label in _LIMITS:
        if key in s:
            bits.append(f"{label} {s[key]}")
    if s.get("uniqueItems"):
        bits.append("unique items")
    if "pattern" in s:
        bits.append(f"pattern `{s['pattern']}`")
    if "format" in s:
        bits.append(f"format `{s['format']}`")
    if isinstance(s.get("prefixItems"), list):
        # A positional tuple: each entry constrains one slot, not the whole list.
        slots = []
        for entry in s["prefixItems"]:
            entry = _resolve(entry, root) if root is not None else entry
            inner = _constraints(entry, root)
            slots.append(f"{_type_str(entry)} ({inner})" if inner else _type_str(entry))
        bits.append("in order: " + ", ".join(slots))
    if s.get("type") == "array" and isinstance(s.get("items"), dict):
        items = _resolve(s["items"], root) if root is not None else s["items"]
        # arrays of objects expand into their own subsection (see _object_children),
        # so only summarise scalar item constraints here
        if isinstance(items, dict) and "properties" not in items:
            inner = _constraints(items, root)
            if inner:
                bits.append("each item " + inner)
    for pattern, value in (s.get("patternProperties") or {}).items():
        # A pattern-keyed map. Without this the row reads "object" with an empty Constraints
        # cell: the reader is told neither what the keys look like nor what a value may be.
        value = _resolve(value, root) if root is not None else value
        bits.append(f"keys matching `{pattern}`")
        if isinstance(value, dict) and "properties" not in value:
            described = _type_str(value)
            inner = _constraints(value, root)
            if inner:
                described = f"{described} ({inner})" if described else inner
            if described:
                bits.append("each value " + described)
    return "; ".join(bits)


def _object_children(s, root):
    """Return (child_object_schema, heading) if this property expands, else None."""
    s = _resolve(s, root)
    if s.get("type") == "object" and "properties" in s:
        return s
    if s.get("type") == "array":
        items = _resolve(s.get("items", {}), root)
        if isinstance(items, dict) and "properties" in items:
            return items
    for value in (s.get("patternProperties") or {}).values():
        value = _resolve(value, root)
        if isinstance(value, dict) and "properties" in value:
            return value
    return None


def _render(schema, root, level, out, path):
    """Render an object schema: a properties table plus subsections for nested objects."""
    schema = _resolve(schema, root)
    props = schema.get("properties", {})
    required = set(schema.get("required", []))
    header = "#" * min(level, 6)
    if path:
        out.append(f"\n{header} {path}\n")
    desc = schema.get("description") or schema.get("title")
    if desc and path:
        out.append(_cell(desc) + "\n")
    rows, nested = [], []
    for pname, praw in props.items():
        ps = _resolve(praw, root)
        rows.append([f"`{pname}`", _type_str(ps), "Yes" if pname in required else "",
                     _constraints(ps, root), ps.get("description") or ps.get("title") or ""])
        child = _object_children(praw, root)
        if child is not None:
            if level < MAX_DEPTH:
                nested.append((pname, child))
            else:
                # Past MAX_DEPTH the parent row still says "object" and the subsection never
                # appears. Unreached today (the deepest schema nests 3), but silence here would
                # look identical to a property that genuinely has no children.
                _dropped.append(f"{path}.{pname}" if path else pname)
    if rows:
        out.append("| Field | Type | Required | Constraints | Description |")
        out.append("| --- | --- | --- | --- | --- |")
        for r in rows:
            out.append("| " + " | ".join(_cell(c) for c in r) + " |")
    for pname, child in nested:
        child_path = f"{path}.{pname}" if path else pname
        _render(child, root, level + 1, out, child_path)


def generate_one(src, dst):
    with open(os.path.join(DATA_DIR, src), encoding="utf-8") as fh:
        schema = json.load(fh)
    out = [f"<!-- generated by _ext/gen_settings_reference.py from ods_tools/data/{src} -->"]
    _render(schema, schema, level=1, out=out, path="")
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, dst), "w", encoding="utf-8") as fh:
        fh.write("\n".join(out) + "\n")
    # count fields rendered
    return sum(1 for line in out if line.startswith("| `"))


def run(app=None, config=None):
    _dropped.clear()
    total = {dst: generate_one(src, dst) for src, dst in SCHEMAS}
    msg = "[gen_settings_reference] " + ", ".join(f"{k}:{v} fields" for k, v in total.items())
    warning = (f"[gen_settings_reference] MAX_DEPTH={MAX_DEPTH} elided nested properties: "
               + ", ".join(_dropped)) if _dropped else None
    if app is not None:
        from sphinx.util import logging
        logger = logging.getLogger(__name__)
        logger.info(msg)
        if warning:
            logger.warning(warning)
    else:
        print(msg)
        if warning:
            print(warning)


def setup(app):
    app.connect("config-inited", run)
    return {"parallel_read_safe": True, "parallel_write_safe": True}


if __name__ == "__main__":
    run()
