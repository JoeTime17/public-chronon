"""Generate test config files from source configs with test_id isolation.

The strategy is to copy *all* ``.py`` config files for a cloud into
test-scoped copies (e.g. ``exports.py`` → ``exports_abc123.py``),
rewriting imports in the copies so the entire dependency chain is
isolated.  After the test, the copies are deleted — originals are
never touched.

Tests reference configs by their logical name (e.g.
``"compiled/joins/gcp/demo.v1__1"``); the ``resolve_conf`` helper
transparently maps it to the test-scoped path by inserting the
test_id into the filename stem.
"""

import glob
import logging
import os
import re
import shutil

logger = logging.getLogger(__name__)

# Config directories that contain .py source files to template.
_CONFIG_DIRS = ("staging_queries", "group_bys", "joins", "models")


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def _discover_sources(chronon_root: str, cloud: str) -> list[str]:
    """Return all .py config files under *chronon_root*/{config_dir}/*cloud*/.

    Skips ``__init__.py`` files.
    """
    sources: list[str] = []
    for config_dir in _CONFIG_DIRS:
        pattern = os.path.join(chronon_root, config_dir, cloud, "*.py")
        sources.extend(
            p for p in glob.glob(pattern)
            if os.path.basename(p) != "__init__.py"
        )
    return sorted(sources)


def _collect_py_files(chronon_root: str, cloud: str) -> list[str]:
    """Return all .py config files under *chronon_root* for *cloud*."""
    pattern = os.path.join(chronon_root, "**", cloud, "*.py")
    return [p for p in glob.glob(pattern, recursive=True) if os.path.basename(p) != "__init__.py"]


# ---------------------------------------------------------------------------
# Conf resolution
# ---------------------------------------------------------------------------

def resolve_conf(conf_path: str, test_id: str) -> str:
    """Map a logical compiled conf path to its test-scoped equivalent.

    >>> resolve_conf("compiled/joins/gcp/demo.v1__1", "abc123")
    'compiled/joins/gcp/demo_abc123.v1__1'
    """
    parts = conf_path.rsplit("/", 1)
    if len(parts) != 2:
        return conf_path
    prefix, filename = parts
    dot_idx = filename.find(".")
    if dot_idx == -1:
        # No dot — just append test_id
        return f"{prefix}/{filename}_{test_id}"
    stem = filename[:dot_idx]
    rest = filename[dot_idx:]
    return f"{prefix}/{stem}_{test_id}{rest}"


def get_confs(cloud: str, test_id: str):
    """Return a callable that maps any logical conf path to its test-scoped path.

    Usage: ``confs("compiled/joins/gcp/demo.v1__1")``
    """
    from functools import partial
    return partial(resolve_conf, test_id=test_id)


# ---------------------------------------------------------------------------
# Import rewriting helpers
# ---------------------------------------------------------------------------

def _find_imported_stems(content: str, stem_renames: dict[str, str]) -> dict[str, str]:
    """Return the subset of *stem_renames* whose keys appear in import statements."""
    result: dict[str, str] = {}
    for line in content.splitlines():
        m = re.match(r".*\bimport\s+(.+)", line.strip())
        if not m:
            continue
        imported_names = m.group(1)
        for old, new in stem_renames.items():
            if re.search(rf"\b{re.escape(old)}\b", imported_names):
                result[old] = new
    return result


def _apply_renames(content: str, renames: dict[str, str]) -> str:
    """Replace module names with renamed versions throughout *content*.

    Uses a negative lookbehind for '.' so that attribute access like
    ``exports.user_activities`` is left alone while import-level names
    (``import user_activities``, ``user_activities.v1``) are renamed.

    Lines of the form ``from X.Y.Z import name`` (3+ dotted components
    in the from-path) are skipped: the imported ``name`` is an attribute
    of module ``Z``, not a sibling module, so it must not be renamed
    even if it happens to collide with a known stem.
    """
    sorted_renames = sorted(renames.items(), key=lambda kv: len(kv[0]), reverse=True)
    out_lines = []
    for line in content.splitlines(keepends=True):
        m = re.match(r"^\s*from\s+([\w.]+)\s+import\s+", line)
        if m and m.group(1).count(".") >= 2:
            out_lines.append(line)
            continue
        for old, new in sorted_renames:
            line = re.sub(rf"(?<!\.)\b{re.escape(old)}\b", new, line)
        out_lines.append(line)
    return "".join(out_lines)


def _rewrite_file(path: str, stem_renames: dict[str, str], dest: str = None) -> bool:
    """Rewrite a single file, only renaming stems that appear in its imports.

    Returns True if the file was modified.
    """
    dest_write = dest or path
    with open(path) as f:
        original = f.read()
    applicable = _find_imported_stems(original, stem_renames)
    if not applicable:
        return False
    updated = _apply_renames(original, applicable)
    if updated == original:
        return False
    with open(dest_write, "w") as f:
        f.write(updated)
    return True


# ---------------------------------------------------------------------------
# Generate / cleanup
# ---------------------------------------------------------------------------

def generate_test_configs(
    test_id: str,
    chronon_root: str,
    cloud: str = "gcp",
) -> list[str]:
    """Copy all source config files into test-scoped copies with rewritten imports.

    Every ``.py`` config file for the given cloud is copied to
    ``{stem}_{test_id}.py``.  Imports in each copy are rewritten so
    references to sibling modules point to the test-scoped copies.

    Originals are never modified.  Returns the list of created file paths.
    """
    sources = _discover_sources(chronon_root, cloud)
    stem_renames = {
        os.path.splitext(os.path.basename(src))[0]: f"{os.path.splitext(os.path.basename(src))[0]}_{test_id}"
        for src in sources
    }

    # Phase 1: copy each source to its test-scoped path.
    renamed: dict[str, str] = {}
    for src in sources:
        stem = os.path.splitext(os.path.basename(src))[0]
        dst = os.path.join(os.path.dirname(src), f"{stem}_{test_id}.py")
        shutil.copy(src, dst)
        logger.info("Copied %s -> %s", src, dst)
        renamed[src] = dst

    # Phase 2: rewrite imports in all copies.
    for src, dst in renamed.items():
        if _rewrite_file(src, stem_renames, dst):
            logger.info("Rewrote imports from %s in %s", src, dst)

    return list(renamed.values())


def cleanup_test_configs(
    test_id: str,
    chronon_root: str,
    cloud: str = "gcp",
) -> list[str]:
    """Delete test-scoped config copies created by ``generate_test_configs``."""
    sources = _discover_sources(chronon_root, cloud)
    deleted: list[str] = []

    for src in sources:
        stem = os.path.splitext(os.path.basename(src))[0]
        dst = os.path.join(os.path.dirname(src), f"{stem}_{test_id}.py")
        if os.path.exists(dst):
            os.remove(dst)
            logger.info("Deleted %s", dst)
            deleted.append(dst)

    return deleted
