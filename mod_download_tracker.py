#!/usr/bin/env python3
"""
Advanced Minecraft Mod Download Tracker
======================================

Tracks daily download totals and analytics for Minecraft mods on:
- Modrinth (public API)
- CurseForge (public website scraping via cloudscraper; no API key required)

Features
--------
- Multi-project support
- Daily snapshots stored in SQLite
- Daily delta calculation
- Loader breakdown (Fabric / NeoForge / Forge / Quilt / Unknown)
- Minecraft version breakdown
- 7-day rolling averages
- Spike detection versus trailing baseline
- Release tagging / event tracking
- CSV exports
- PNG charts
- Summary reports by project/platform/loader/Minecraft version
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import re
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

import cloudscraper
import matplotlib.pyplot as plt
import requests
from bs4 import BeautifulSoup


# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG: dict[str, Any] = {
    "db_path": "hearthguard_downloads.sqlite3",
    "output_dir": "tracker_output",

    "enable_modrinth": True,
    "enable_curseforge": True,

    "projects": [
        {
            "name": "HearthGuard",
            "modrinth": {"id": "hearthguard"},
            "curseforge": {
                "slug": "hearthguard",
                "base_url": "https://www.curseforge.com/minecraft/mc-mods/hearthguard/files/all",
            },
        },
    ],

    "release_tags": [
        {"date": "2026-04-11", "project": "HearthGuard", "tag": "1.0.3-release"},
    ],

    "spike_window_days": 7,
    "spike_min_multiplier": 2.0,
    "spike_min_absolute_increase": 10,

    "http_timeout_seconds": 30,
    "http_retries": 4,

    "chart_dpi": 140,
    "verbose_console": True,
}


MODRINTH_BASE = "https://api.modrinth.com/v2"


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class SnapshotRow:
    snapshot_date: str
    platform: str
    project_name: str
    project_platform_id: str
    item_id: str
    item_name: str
    version_label: str
    mod_version: str
    loader: str
    game_versions: str
    total_downloads: int

    def as_db_tuple(self) -> tuple:
        return (
            self.snapshot_date,
            self.platform,
            self.project_name,
            self.project_platform_id,
            self.item_id,
            self.item_name,
            self.version_label,
            self.mod_version,
            self.loader,
            self.game_versions,
            self.total_downloads,
        )


# ============================================================================
# UTILS
# ============================================================================

def utc_today() -> str:
    return dt.datetime.now(dt.UTC).date().isoformat()


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def normalize_list(values: Iterable[str]) -> str:
    seen: list[str] = []
    for value in values:
        if not value:
            continue
        v = str(value).strip()
        if v and v not in seen:
            seen.append(v)
    return ",".join(seen)


def parse_csv_field(value: str | None) -> list[str]:
    if not value:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def guess_loader_from_filename(filename: str) -> list[str]:
    lower = filename.lower()
    hits: list[str] = []
    for loader in ("fabric", "neoforge", "forge", "quilt"):
        if loader in lower:
            hits.append(loader)
    return hits


def canonical_loader_group(loaders_csv: str) -> str:
    loaders = {x.strip().lower() for x in parse_csv_field(loaders_csv)}
    if "fabric" in loaders:
        return "fabric"
    if "neoforge" in loaders:
        return "neoforge"
    if "forge" in loaders:
        return "forge"
    if "quilt" in loaders:
        return "quilt"
    return "unknown"


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def extract_mod_version(*candidates: str) -> str:
    for raw in candidates:
        if not raw:
            continue
        text = str(raw).strip()
        if not text:
            continue
        text = re.sub(r"\.(jar|zip)$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*\([^)]*\)\s*$", "", text)
        pair_matches = re.findall(
            r"\d+(?:\.\d+)+-(\d+(?:\.\d+)+(?:[-+][0-9A-Za-z.]+)?)",
            text,
        )
        if pair_matches:
            return pair_matches[-1]

        matches = re.findall(r"\d+(?:\.\d+)+(?:[-+][0-9A-Za-z.]+)?", text)
        if matches:
            return matches[-1]
    return "unknown"


def apply_known_version_fixes(
    *,
    item_name: str,
    version_label: str,
    mod_version: str,
    game_versions_csv: str,
) -> tuple[str, str, str]:
    """
    Normalize known historical filename/version labeling mistakes.
    """
    out_item = item_name
    out_label = version_label
    versions = parse_csv_field(game_versions_csv)

    if mod_version == "1.0.2":
        out_item = out_item.replace("26.1-1.0.2", "26.1.1-1.0.2")
        out_label = out_label.replace("26.1-1.0.2", "26.1.1-1.0.2")
        versions = ["26.1.1" if v == "26.1" else v for v in versions]

    return out_item, out_label, normalize_list(versions)


def extract_primary_mc_version(*candidates: str) -> str:
    for raw in candidates:
        if not raw:
            continue
        text = str(raw).strip()
        if not text:
            continue
        text = re.sub(r"\.(jar|zip)$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*\([^)]*\)\s*$", "", text)
        m = re.findall(r"(\d+(?:\.\d+)+)-\d+(?:\.\d+)+(?:[-+][0-9A-Za-z.]+)?", text)
        if m:
            return m[-1]
    return "unknown"


def row_primary_mc_version(row: dict[str, Any]) -> str:
    parsed = extract_primary_mc_version(row.get("item_name", ""), row.get("version_label", ""))
    if parsed != "unknown":
        return parsed
    versions = parse_csv_field(row.get("game_versions", ""))
    return versions[0] if versions else "unknown"


def mc_chart_bucket(version: str) -> str:
    v = (version or "").strip()
    if v.startswith("26."):
        return "26.*"
    return v or "unknown"


def request_text(
    session,
    url: str,
    *,
    headers: Optional[dict[str, str]] = None,
    params: Optional[dict[str, Any]] = None,
    timeout: int = 30,
    retries: int = 4,
) -> str:
    last_exc: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt < retries:
                    time.sleep(min(2 ** attempt, 12))
                    continue
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(min(2 ** attempt, 12))
                continue

    raise RuntimeError(f"Request failed for {url}: {last_exc}") from last_exc


def request_json(
    session,
    url: str,
    *,
    headers: Optional[dict[str, str]] = None,
    params: Optional[dict[str, Any]] = None,
    timeout: int = 30,
    retries: int = 4,
) -> Any:
    text = request_text(session, url, headers=headers, params=params, timeout=timeout, retries=retries)
    return json.loads(text)


# ============================================================================
# DB
# ============================================================================

def create_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            snapshot_date TEXT NOT NULL,
            platform TEXT NOT NULL,
            project_name TEXT NOT NULL,
            project_platform_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            item_name TEXT NOT NULL,
            version_label TEXT NOT NULL,
            mod_version TEXT NOT NULL DEFAULT 'unknown',
            loader TEXT NOT NULL,
            game_versions TEXT NOT NULL,
            total_downloads INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (snapshot_date, platform, item_id)
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_snapshots_item
        ON snapshots(platform, item_id, snapshot_date)
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_snapshots_project
        ON snapshots(project_name, platform, snapshot_date)
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS release_tags (
            release_date TEXT NOT NULL,
            project_name TEXT NOT NULL,
            tag TEXT NOT NULL,
            notes TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (release_date, project_name, tag)
        )
    """)

    conn.commit()

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(snapshots)")
    existing_columns = {str(r[1]) for r in cur.fetchall()}
    if "mod_version" not in existing_columns:
        conn.execute("ALTER TABLE snapshots ADD COLUMN mod_version TEXT NOT NULL DEFAULT 'unknown'")
        conn.commit()


def sync_release_tags(conn: sqlite3.Connection, tags: Sequence[dict[str, Any]]) -> None:
    if not tags:
        return

    rows = []
    for tag in tags:
        release_date = str(tag["date"])
        project_name = str(tag["project"])
        label = str(tag["tag"])
        notes = str(tag.get("notes", ""))
        rows.append((release_date, project_name, label, notes))

    with conn:
        conn.executemany("""
            INSERT INTO release_tags (release_date, project_name, tag, notes)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(release_date, project_name, tag) DO UPDATE SET
                notes = excluded.notes
        """, rows)


def upsert_snapshots(conn: sqlite3.Connection, rows: Sequence[SnapshotRow]) -> None:
    if not rows:
        return

    with conn:
        conn.executemany("""
            INSERT INTO snapshots (
                snapshot_date, platform, project_name, project_platform_id, item_id,
                item_name, version_label, mod_version, loader, game_versions, total_downloads
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_date, platform, item_id) DO UPDATE SET
                project_name = excluded.project_name,
                project_platform_id = excluded.project_platform_id,
                item_name = excluded.item_name,
                version_label = excluded.version_label,
                mod_version = excluded.mod_version,
                loader = excluded.loader,
                game_versions = excluded.game_versions,
                total_downloads = excluded.total_downloads
        """, [row.as_db_tuple() for row in rows])


# ============================================================================
# MODRINTH
# ============================================================================

def fetch_modrinth_versions(
    session,
    *,
    project_name: str,
    project_id_or_slug: str,
    timeout: int,
    retries: int,
) -> list[SnapshotRow]:
    payload = request_json(
        session,
        f"{MODRINTH_BASE}/project/{project_id_or_slug}/version",
        params={"include_changelog": "false"},
        timeout=timeout,
        retries=retries,
    )

    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected Modrinth response for {project_id_or_slug}")

    today = utc_today()
    rows: list[SnapshotRow] = []

    for version in payload:
        version_id = str(version.get("id", ""))
        version_name = str(version.get("name") or "")
        version_number = str(version.get("version_number") or version_name or version_id)
        downloads = safe_int(version.get("downloads"), 0)
        loaders = [str(x) for x in (version.get("loaders") or [])]
        game_versions = [str(x) for x in (version.get("game_versions") or [])]
        files = version.get("files") or []

        item_name = version_name or version_number or version_id
        for file_obj in files:
            if file_obj.get("primary") is True:
                item_name = str(file_obj.get("filename") or item_name)
                break
        else:
            if files:
                item_name = str(files[0].get("filename") or item_name)

        mod_version = extract_mod_version(item_name, version_number, version_name)
        game_versions_csv = normalize_list(game_versions)
        item_name, version_number, game_versions_csv = apply_known_version_fixes(
            item_name=item_name,
            version_label=version_number,
            mod_version=mod_version,
            game_versions_csv=game_versions_csv,
        )

        rows.append(SnapshotRow(
            snapshot_date=today,
            platform="modrinth",
            project_name=project_name,
            project_platform_id=project_id_or_slug,
            item_id=version_id,
            item_name=item_name,
            version_label=version_number,
            mod_version=mod_version,
            loader=normalize_list(loaders),
            game_versions=game_versions_csv,
            total_downloads=downloads,
        ))

    return rows


# ============================================================================
# CURSEFORGE SCRAPING
# ============================================================================

CF_ROW_PATTERN = re.compile(
    r'^R\s+'
    r'(?P<name>.+?)\s+'
    r'(?P<uploaded>[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})\s+'
    r'(?P<size>\S+\s+(?:KB|MB|GB))\s+'
    r'(?P<rest>.+?)\s+'
    r'(?P<downloads>[\d,]+)$'
)

def split_cf_rest(name: str, rest: str) -> tuple[list[str], list[str]]:
    tokens = rest.split()
    versions: list[str] = []
    loaders: list[str] = []

    loader_words = {"fabric", "neoforge", "forge", "quilt"}
    i = 0
    while i < len(tokens):
        token = tokens[i]
        lower = token.lower()

        if lower in loader_words:
            loaders.append(token)
            i += 1
            continue

        if token == "+" and i + 1 < len(tokens) and tokens[i + 1].isdigit():
            i += 2
            continue

        if re.fullmatch(r'[0-9]+(?:\.[0-9]+)*', token):
            versions.append(token)
            i += 1
            continue

        i += 1

    if not loaders:
        loaders = guess_loader_from_filename(name)

    return versions, loaders


def extract_curseforge_files_from_next_payload(html: str) -> list[dict[str, Any]]:
    """
    Parse server-rendered Next.js chunks and extract the structured `files` payload.
    """
    chunk_pattern = re.compile(r'_next_f\.push\(\[\d+,"((?:\\.|[^"\\])*)"\]\)')
    out: list[dict[str, Any]] = []
    seen_ids: set[int] = set()

    for raw_chunk in chunk_pattern.findall(html):
        try:
            decoded = json.loads(f"\"{raw_chunk}\"")
        except Exception:
            continue

        if "\"files\":[" not in decoded or ":" not in decoded:
            continue

        payload = decoded.split(":", 1)[1]
        try:
            obj = json.loads(payload)
        except Exception:
            continue

        if not (isinstance(obj, list) and len(obj) >= 4 and isinstance(obj[3], dict)):
            continue

        files = obj[3].get("files")
        if not isinstance(files, list):
            continue

        for file_obj in files:
            if not isinstance(file_obj, dict):
                continue
            file_id = safe_int(file_obj.get("id"), -1)
            if file_id < 0 or file_id in seen_ids:
                continue
            seen_ids.add(file_id)
            out.append(file_obj)

    return out


def scrape_curseforge_file_rows_from_html(html: str, project_slug: str) -> list[dict[str, Any]]:
    payload_files = extract_curseforge_files_from_next_payload(html)
    if payload_files:
        rows: list[dict[str, Any]] = []
        for file_obj in payload_files:
            name = str(file_obj.get("displayName") or file_obj.get("fileName") or "").strip()
            if not name:
                continue
            if project_slug.lower() not in name.lower():
                continue

            downloads = safe_int(file_obj.get("totalDownloads"), 0)
            versions = [str(x) for x in (file_obj.get("gameVersions") or [])]

            loaders: list[str] = []
            flavor = file_obj.get("flavor") or {}
            flavor_name = str(flavor.get("name") or "").strip()
            if flavor_name:
                loaders.append(flavor_name)
            for f in file_obj.get("flavors") or []:
                n = str((f or {}).get("name") or "").strip()
                if n:
                    loaders.append(n)
            if not loaders:
                loaders = guess_loader_from_filename(name)

            mod_version = extract_mod_version(name)
            item_name, version_label, game_versions_csv = apply_known_version_fixes(
                item_name=name,
                version_label=name,
                mod_version=mod_version,
                game_versions_csv=normalize_list(versions),
            )
            rows.append({
                "raw_item_name": name,
                "item_name": item_name,
                "version_label": version_label,
                "mod_version": mod_version,
                "game_versions": game_versions_csv,
                "loader": normalize_list(loaders),
                "total_downloads": downloads,
            })

        return rows

    # Fallback: plain-text parsing when structured payload is unavailable.
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [line.strip() for line in text.splitlines() if line.strip()]

    rows: list[dict[str, Any]] = []
    for line in lines:
        m = CF_ROW_PATTERN.match(line)
        if not m:
            continue

        name = m.group("name").strip()
        if project_slug.lower() not in name.lower():
            continue

        rest = m.group("rest").strip()
        downloads = safe_int(m.group("downloads").replace(",", ""), 0)
        versions, loaders = split_cf_rest(name, rest)

        mod_version = extract_mod_version(name)
        item_name, version_label, game_versions_csv = apply_known_version_fixes(
            item_name=name,
            version_label=name,
            mod_version=mod_version,
            game_versions_csv=normalize_list(versions),
        )
        rows.append({
            "raw_item_name": name,
            "item_name": item_name,
            "version_label": version_label,
            "mod_version": mod_version,
            "game_versions": game_versions_csv,
            "loader": normalize_list(loaders),
            "total_downloads": downloads,
        })

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        key = row.get("raw_item_name", row["item_name"])
        if key not in seen:
            seen.add(key)
            deduped.append(row)
    return deduped


def fetch_curseforge_files_by_scrape(
    session,
    *,
    project_name: str,
    project_slug: str,
    base_url: str,
    timeout: int,
    retries: int,
) -> list[SnapshotRow]:
    today = utc_today()
    page = 1
    page_size = 50
    all_rows: list[SnapshotRow] = []
    seen_names: set[str] = set()

    while True:
        html = request_text(
            session,
            base_url,
            params={"page": page, "pageSize": page_size, "showAlphaFiles": "hide"},
            timeout=timeout,
            retries=retries,
        )

        parsed_rows = scrape_curseforge_file_rows_from_html(html, project_slug)
        if not parsed_rows:
            break

        new_count = 0
        for row in parsed_rows:
            item_name = row["item_name"]
            source_name = row.get("raw_item_name", item_name)
            if item_name in seen_names:
                continue
            seen_names.add(item_name)
            new_count += 1

            item_id = f"{project_slug}:{source_name}"

            all_rows.append(SnapshotRow(
                snapshot_date=today,
                platform="curseforge",
                project_name=project_name,
                project_platform_id=project_slug,
                item_id=item_id,
                item_name=item_name,
                version_label=row["version_label"],
                mod_version=row.get("mod_version", "unknown"),
                loader=row["loader"],
                game_versions=row["game_versions"],
                total_downloads=row["total_downloads"],
            ))

        if new_count < page_size:
            break

        page += 1
        if page > 20:
            break

    return all_rows


# ============================================================================
# REPORT QUERIES
# ============================================================================

def compute_item_report_for_date(conn: sqlite3.Connection, snapshot_date: str) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            s.snapshot_date,
            s.platform,
            s.project_name,
            s.project_platform_id,
            s.item_id,
            s.item_name,
            s.version_label,
            s.mod_version,
            s.loader,
            s.game_versions,
            s.total_downloads,
            (
                SELECT s2.total_downloads
                FROM snapshots s2
                WHERE s2.platform = s.platform
                  AND s2.item_id = s.item_id
                  AND s2.snapshot_date < s.snapshot_date
                ORDER BY s2.snapshot_date DESC
                LIMIT 1
            ) AS previous_total
        FROM snapshots s
        WHERE s.snapshot_date = ?
        ORDER BY s.project_name, s.platform, s.item_name
    """, (snapshot_date,))

    rows = []
    for r in cur.fetchall():
        prev = r[11]
        total = safe_int(r[10], 0)
        daily = None if prev is None else total - safe_int(prev, 0)

        rows.append({
            "snapshot_date": r[0],
            "platform": r[1],
            "project_name": r[2],
            "project_platform_id": r[3],
            "item_id": r[4],
            "item_name": r[5],
            "version_label": r[6],
            "mod_version": r[7],
            "loader": r[8],
            "game_versions": r[9],
            "total_downloads": total,
            "previous_total": prev,
            "daily_downloads": daily,
            "loader_group": canonical_loader_group(r[8]),
        })
    return rows


def load_all_daily_item_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute("""
        SELECT
            s.snapshot_date,
            s.platform,
            s.project_name,
            s.project_platform_id,
            s.item_id,
            s.item_name,
            s.version_label,
            s.mod_version,
            s.loader,
            s.game_versions,
            s.total_downloads,
            (
                SELECT s2.total_downloads
                FROM snapshots s2
                WHERE s2.platform = s.platform
                  AND s2.item_id = s.item_id
                  AND s2.snapshot_date < s.snapshot_date
                ORDER BY s2.snapshot_date DESC
                LIMIT 1
            ) AS previous_total
        FROM snapshots s
        ORDER BY s.snapshot_date, s.project_name, s.platform, s.item_name
    """)

    rows = []
    for r in cur.fetchall():
        prev = r[11]
        total = safe_int(r[10], 0)
        daily = None if prev is None else total - safe_int(prev, 0)
        rows.append({
            "snapshot_date": r[0],
            "platform": r[1],
            "project_name": r[2],
            "project_platform_id": r[3],
            "item_id": r[4],
            "item_name": r[5],
            "version_label": r[6],
            "mod_version": r[7],
            "loader": r[8],
            "game_versions": r[9],
            "total_downloads": total,
            "previous_total": prev,
            "daily_downloads": daily,
            "loader_group": canonical_loader_group(r[8]),
        })
    return rows


def aggregate_daily_project_totals(item_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], int] = defaultdict(int)
    for row in item_rows:
        daily = row["daily_downloads"]
        if daily is None:
            continue
        key = (row["snapshot_date"], row["project_name"], row["platform"])
        grouped[key] += safe_int(daily, 0)

    records = []
    for (snapshot_date, project_name, platform), total in sorted(grouped.items()):
        records.append({
            "snapshot_date": snapshot_date,
            "project_name": project_name,
            "platform": platform,
            "daily_downloads": total,
        })
    return records


def aggregate_daily_loader_totals(item_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], int] = defaultdict(int)
    for row in item_rows:
        daily = row["daily_downloads"]
        if daily is None:
            continue
        key = (
            row["snapshot_date"],
            row["project_name"],
            row["platform"],
            row["loader_group"],
        )
        grouped[key] += safe_int(daily, 0)

    records = []
    for (snapshot_date, project_name, platform, loader_group), total in sorted(grouped.items()):
        records.append({
            "snapshot_date": snapshot_date,
            "project_name": project_name,
            "platform": platform,
            "loader_group": loader_group,
            "daily_downloads": total,
        })
    return records


def aggregate_daily_mc_version_totals(item_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], int] = defaultdict(int)
    for row in item_rows:
        daily = row["daily_downloads"]
        if daily is None:
            continue
        mc_version = row_primary_mc_version(row)
        key = (row["snapshot_date"], row["project_name"], row["platform"], mc_version)
        grouped[key] += safe_int(daily, 0)

    records = []
    for (snapshot_date, project_name, platform, mc_version), total in sorted(grouped.items()):
        records.append({
            "snapshot_date": snapshot_date,
            "project_name": project_name,
            "platform": platform,
            "mc_version": mc_version,
            "daily_downloads": total,
        })
    return records


def aggregate_daily_mod_version_totals(item_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str, str], int] = defaultdict(int)
    for row in item_rows:
        daily = row["daily_downloads"]
        if daily is None:
            continue
        key = (row["snapshot_date"], row["project_name"], row["platform"], row.get("mod_version", "unknown") or "unknown")
        grouped[key] += safe_int(daily, 0)

    records = []
    for (snapshot_date, project_name, platform, mod_version), total in sorted(grouped.items()):
        records.append({
            "snapshot_date": snapshot_date,
            "project_name": project_name,
            "platform": platform,
            "mod_version": mod_version,
            "daily_downloads": total,
        })
    return records


def add_rolling_average(
    records: Sequence[dict[str, Any]],
    *,
    group_keys: Sequence[str],
    value_key: str,
    output_key: str,
    window_days: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for rec in records:
        key = tuple(rec[k] for k in group_keys)
        grouped[key].append(dict(rec))

    out: list[dict[str, Any]] = []
    for _, group in grouped.items():
        group.sort(key=lambda x: x["snapshot_date"])
        history: list[float] = []
        for rec in group:
            val = float(rec.get(value_key, 0) or 0)
            trailing = history[-window_days:]
            rec[output_key] = None if not trailing else sum(trailing) / len(trailing)
            history.append(val)
            out.append(rec)

    out.sort(key=lambda x: tuple(x[k] for k in group_keys) + (x["snapshot_date"],))
    return out


def detect_spikes(
    records: Sequence[dict[str, Any]],
    *,
    value_key: str,
    baseline_key: str,
    min_multiplier: float,
    min_absolute_increase: int,
) -> list[dict[str, Any]]:
    spikes: list[dict[str, Any]] = []
    for rec in records:
        value = rec.get(value_key)
        baseline = rec.get(baseline_key)
        if value is None or baseline in (None, 0):
            continue

        value_f = float(value)
        baseline_f = float(baseline)
        if value_f >= baseline_f * min_multiplier and (value_f - baseline_f) >= min_absolute_increase:
            spike = dict(rec)
            spike["spike_multiplier"] = value_f / baseline_f if baseline_f else None
            spike["spike_absolute_increase"] = value_f - baseline_f
            spikes.append(spike)

    spikes.sort(key=lambda x: (x["snapshot_date"], x.get("spike_multiplier", 0)), reverse=True)
    return spikes


def load_release_tags(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute("""
        SELECT release_date, project_name, tag, notes
        FROM release_tags
        ORDER BY release_date, project_name, tag
    """)
    return [
        {
            "release_date": r[0],
            "project_name": r[1],
            "tag": r[2],
            "notes": r[3],
        }
        for r in cur.fetchall()
    ]


def attach_release_tags_to_records(
    records: Sequence[dict[str, Any]],
    tags: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    tag_map: dict[tuple[str, str], list[str]] = defaultdict(list)
    for tag in tags:
        key = (tag["release_date"], tag["project_name"])
        tag_map[key].append(tag["tag"])

    out = []
    for rec in records:
        key = (rec["snapshot_date"], rec["project_name"])
        rec2 = dict(rec)
        rec2["release_tags"] = ",".join(tag_map.get(key, []))
        out.append(rec2)
    return out


def summarize_latest_loader_breakdown(item_rows_today: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, int] = defaultdict(int)
    for row in item_rows_today:
        daily = row["daily_downloads"]
        if daily is None:
            continue
        key = row["loader_group"]
        grouped[key] += safe_int(daily, 0)

    out = []
    for loader_group, total in sorted(grouped.items()):
        out.append({
            "loader_group": loader_group,
            "daily_downloads": total,
        })
    return out


def summarize_latest_mc_breakdown(item_rows_today: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, int] = defaultdict(int)
    for row in item_rows_today:
        daily = row["daily_downloads"]
        if daily is None:
            continue
        key = row_primary_mc_version(row)
        grouped[key] += safe_int(daily, 0)

    out = []
    for mc_version, total in sorted(grouped.items()):
        out.append({
            "mc_version": mc_version,
            "daily_downloads": total,
        })
    return out


def summarize_latest_platform_breakdown(item_rows_today: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, int] = defaultdict(int)
    for row in item_rows_today:
        daily = row["daily_downloads"]
        if daily is None:
            continue
        key = row["platform"]
        grouped[key] += safe_int(daily, 0)

    out = []
    for platform, total in sorted(grouped.items()):
        out.append({
            "platform": platform,
            "daily_downloads": total,
        })
    return out


def summarize_latest_platform_totals(item_rows_today: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, int] = defaultdict(int)
    for row in item_rows_today:
        key = row["platform"]
        grouped[key] += safe_int(row["total_downloads"], 0)

    out = []
    for platform, total in sorted(grouped.items()):
        out.append({
            "platform": platform,
            "total_downloads": total,
        })
    return out


def summarize_latest_loader_totals(item_rows_today: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, int] = defaultdict(int)
    for row in item_rows_today:
        key = row["loader_group"]
        grouped[key] += safe_int(row["total_downloads"], 0)

    out = []
    for loader_group, total in sorted(grouped.items()):
        out.append({
            "loader_group": loader_group,
            "total_downloads": total,
        })
    return out


def summarize_latest_mc_totals(item_rows_today: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, int] = defaultdict(int)
    for row in item_rows_today:
        key = row_primary_mc_version(row)
        grouped[key] += safe_int(row["total_downloads"], 0)

    out = []
    for mc_version, total in sorted(grouped.items(), key=lambda kv: kv[1], reverse=True):
        out.append({
            "mc_version": mc_version,
            "total_downloads": total,
        })
    return out


def summarize_latest_mod_totals(item_rows_today: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, int] = defaultdict(int)
    for row in item_rows_today:
        key = row.get("mod_version", "unknown") or "unknown"
        grouped[key] += safe_int(row["total_downloads"], 0)

    out = []
    for mod_version, total in sorted(grouped.items(), key=lambda kv: kv[1], reverse=True):
        out.append({
            "mod_version": mod_version,
            "total_downloads": total,
        })
    return out


def summarize_latest_mod_breakdown(item_rows_today: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, int] = defaultdict(int)
    for row in item_rows_today:
        daily = row["daily_downloads"]
        if daily is None:
            continue
        key = row.get("mod_version", "unknown") or "unknown"
        grouped[key] += safe_int(daily, 0)

    out = []
    for mod_version, total in sorted(grouped.items()):
        out.append({
            "mod_version": mod_version,
            "daily_downloads": total,
        })
    return out


# ============================================================================
# EXPORTS
# ============================================================================

def write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


# ============================================================================
# CHARTS
# ============================================================================

def plot_line_chart(
    records: Sequence[dict[str, Any]],
    *,
    x_key: str,
    y_key: str,
    series_key: str,
    title: str,
    output_path: Path,
    dpi: int,
) -> bool:
    if not records:
        return False

    series_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rec in records:
        series_map[str(rec[series_key])].append(rec)

    plt.figure(figsize=(11, 6), dpi=dpi)
    for label, series in sorted(series_map.items()):
        series = sorted(series, key=lambda r: r[x_key])
        xs = [r[x_key] for r in series]
        ys = [r.get(y_key, 0) or 0 for r in series]
        plt.plot(xs, ys, marker="o", label=label)

    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel(y_key.replace("_", " ").title())
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    if len(series_map) <= 12:
        plt.legend()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path)
    plt.close()
    return True


def build_charts(
    output_dir: Path,
    daily_project_records: Sequence[dict[str, Any]],
    daily_loader_records: Sequence[dict[str, Any]],
    daily_mc_records: Sequence[dict[str, Any]],
    daily_mod_records: Sequence[dict[str, Any]],
    dpi: int,
) -> list[Path]:
    chart_paths: list[Path] = []

    by_platform_map: dict[tuple[str, str], int] = defaultdict(int)
    for rec in daily_project_records:
        key = (rec["snapshot_date"], rec["platform"])
        by_platform_map[key] += safe_int(rec["daily_downloads"], 0)

    by_platform: list[dict[str, Any]] = []
    for (snapshot_date, platform), daily_downloads in sorted(by_platform_map.items()):
        by_platform.append({
            "snapshot_date": snapshot_date,
            "platform": platform,
            "daily_downloads": daily_downloads,
            "series": platform,
        })

    path = output_dir / "charts" / "daily_downloads_by_platform.png"
    if plot_line_chart(
        by_platform,
        x_key="snapshot_date",
        y_key="daily_downloads",
        series_key="series",
        title="Daily Downloads by Platform",
        output_path=path,
        dpi=dpi,
    ):
        chart_paths.append(path)

    by_loader_map: dict[tuple[str, str], int] = defaultdict(int)
    for rec in daily_loader_records:
        key = (rec["snapshot_date"], rec["loader_group"])
        by_loader_map[key] += safe_int(rec["daily_downloads"], 0)

    by_loader: list[dict[str, Any]] = []
    for (snapshot_date, loader_group), daily_downloads in sorted(by_loader_map.items()):
        by_loader.append({
            "snapshot_date": snapshot_date,
            "loader_group": loader_group,
            "daily_downloads": daily_downloads,
            "series": loader_group,
        })

    path = output_dir / "charts" / "daily_downloads_by_loader.png"
    if plot_line_chart(
        by_loader,
        x_key="snapshot_date",
        y_key="daily_downloads",
        series_key="series",
        title="Daily Downloads by Loader",
        output_path=path,
        dpi=dpi,
    ):
        chart_paths.append(path)

    by_mc_map: dict[tuple[str, str], int] = defaultdict(int)
    for rec in daily_mc_records:
        key = (rec["snapshot_date"], mc_chart_bucket(str(rec["mc_version"])))
        by_mc_map[key] += safe_int(rec["daily_downloads"], 0)

    by_mc: list[dict[str, Any]] = []
    for (snapshot_date, mc_version), daily_downloads in sorted(by_mc_map.items()):
        by_mc.append({
            "snapshot_date": snapshot_date,
            "mc_version": mc_version,
            "daily_downloads": daily_downloads,
            "series": mc_version,
        })

    version_totals: dict[str, int] = defaultdict(int)
    for rec in by_mc:
        version_totals[rec["series"]] += safe_int(rec["daily_downloads"], 0)

    top_series = set(sorted(version_totals, key=version_totals.get, reverse=True)[:8])
    by_mc = [rec for rec in by_mc if rec["series"] in top_series]

    path = output_dir / "charts" / "daily_downloads_by_mc_version_top8.png"
    if plot_line_chart(
        by_mc,
        x_key="snapshot_date",
        y_key="daily_downloads",
        series_key="series",
        title="Daily Downloads by Minecraft Version (Top 8)",
        output_path=path,
        dpi=dpi,
    ):
        chart_paths.append(path)

    by_mod_map: dict[tuple[str, str], int] = defaultdict(int)
    for rec in daily_mod_records:
        key = (rec["snapshot_date"], rec["mod_version"])
        by_mod_map[key] += safe_int(rec["daily_downloads"], 0)

    by_mod: list[dict[str, Any]] = []
    for (snapshot_date, mod_version), daily_downloads in sorted(by_mod_map.items()):
        by_mod.append({
            "snapshot_date": snapshot_date,
            "mod_version": mod_version,
            "daily_downloads": daily_downloads,
            "series": mod_version,
        })

    mod_totals: dict[str, int] = defaultdict(int)
    for rec in by_mod:
        mod_totals[rec["series"]] += safe_int(rec["daily_downloads"], 0)

    top_mod_series = set(sorted(mod_totals, key=mod_totals.get, reverse=True)[:8])
    by_mod = [rec for rec in by_mod if rec["series"] in top_mod_series]

    path = output_dir / "charts" / "daily_downloads_by_mod_version_top8.png"
    if plot_line_chart(
        by_mod,
        x_key="snapshot_date",
        y_key="daily_downloads",
        series_key="series",
        title="Daily Downloads by Mod Version (Top 8)",
        output_path=path,
        dpi=dpi,
    ):
        chart_paths.append(path)

    return chart_paths


# ============================================================================
# CONSOLE OUTPUT
# ============================================================================

def print_header(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def print_today_item_report(item_rows_today: Sequence[dict[str, Any]]) -> None:
    print_header(f"{utc_today()} item-level report")
    if not item_rows_today:
        print("No rows found.")
        return

    for row in item_rows_today:
        daily = row["daily_downloads"]
        daily_str = "n/a" if daily is None else str(daily)
        print(
            f"{row['project_name']:<16} "
            f"{row['platform']:<11} "
            f"{row['loader_group']:<9} "
            f"{row['item_name'][:48]:<48} "
            f"total={row['total_downloads']:<8} "
            f"daily={daily_str}"
        )


def print_simple_table(title: str, rows: Sequence[dict[str, Any]], columns: Sequence[str], sort_key: Optional[str] = None, max_rows: Optional[int] = None) -> None:
    print_header(title)
    rows = list(rows)
    if sort_key:
        rows.sort(key=lambda r: r.get(sort_key, 0), reverse=True)
    if max_rows is not None:
        rows = rows[:max_rows]

    if not rows:
        print("No data.")
        return

    widths = {col: max(len(col), max(len(str(r.get(col, ""))) for r in rows)) for col in columns}
    print("  ".join(col.ljust(widths[col]) for col in columns))
    print("  ".join("-" * widths[col] for col in columns))
    for r in rows:
        print("  ".join(str(r.get(col, "")).ljust(widths[col]) for col in columns))


# ============================================================================
# MAIN ANALYTICS PIPELINE
# ============================================================================

def create_session():
    session = cloudscraper.create_scraper(
        browser={
            "browser": "chrome",
            "platform": "windows",
            "mobile": False,
        }
    )
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.curseforge.com/",
    })
    return session


def run_fetch(conn: sqlite3.Connection, config: dict[str, Any]) -> list[SnapshotRow]:
    projects = config["projects"]
    enable_modrinth = bool(config.get("enable_modrinth", True))
    enable_curseforge = bool(config.get("enable_curseforge", True))
    timeout = int(config.get("http_timeout_seconds", 30))
    retries = int(config.get("http_retries", 4))

    session = create_session()
    rows: list[SnapshotRow] = []

    try:
        for project in projects:
            project_name = str(project["name"])

            if enable_modrinth and project.get("modrinth"):
                modrinth_id = str(project["modrinth"]["id"])
                rows.extend(fetch_modrinth_versions(
                    session,
                    project_name=project_name,
                    project_id_or_slug=modrinth_id,
                    timeout=timeout,
                    retries=retries,
                ))

            if enable_curseforge and project.get("curseforge"):
                cf = project["curseforge"]
                rows.extend(fetch_curseforge_files_by_scrape(
                    session,
                    project_name=project_name,
                    project_slug=str(cf["slug"]),
                    base_url=str(cf["base_url"]),
                    timeout=timeout,
                    retries=retries,
                ))
    finally:
        session.close()

    upsert_snapshots(conn, rows)
    return rows


def build_analytics(conn: sqlite3.Connection, config: dict[str, Any]) -> dict[str, Any]:
    output_dir = ensure_dir(config.get("output_dir", "tracker_output"))
    today = utc_today()

    item_rows_today = compute_item_report_for_date(conn, today)
    item_rows_all = load_all_daily_item_rows(conn)

    daily_project = aggregate_daily_project_totals(item_rows_all)
    daily_project = add_rolling_average(
        daily_project,
        group_keys=("project_name", "platform"),
        value_key="daily_downloads",
        output_key="rolling_avg_7d",
        window_days=int(config.get("spike_window_days", 7)),
    )

    daily_loader = aggregate_daily_loader_totals(item_rows_all)
    daily_loader = add_rolling_average(
        daily_loader,
        group_keys=("project_name", "platform", "loader_group"),
        value_key="daily_downloads",
        output_key="rolling_avg_7d",
        window_days=int(config.get("spike_window_days", 7)),
    )

    daily_mc = aggregate_daily_mc_version_totals(item_rows_all)
    daily_mc = add_rolling_average(
        daily_mc,
        group_keys=("project_name", "platform", "mc_version"),
        value_key="daily_downloads",
        output_key="rolling_avg_7d",
        window_days=int(config.get("spike_window_days", 7)),
    )
    daily_mod = aggregate_daily_mod_version_totals(item_rows_all)
    daily_mod = add_rolling_average(
        daily_mod,
        group_keys=("project_name", "platform", "mod_version"),
        value_key="daily_downloads",
        output_key="rolling_avg_7d",
        window_days=int(config.get("spike_window_days", 7)),
    )

    tags = load_release_tags(conn)
    daily_project = attach_release_tags_to_records(daily_project, tags)

    spikes = detect_spikes(
        daily_project,
        value_key="daily_downloads",
        baseline_key="rolling_avg_7d",
        min_multiplier=float(config.get("spike_min_multiplier", 2.0)),
        min_absolute_increase=int(config.get("spike_min_absolute_increase", 10)),
    )

    latest_loader_breakdown = summarize_latest_loader_breakdown(item_rows_today)
    latest_mc_breakdown = summarize_latest_mc_breakdown(item_rows_today)
    latest_platform_breakdown = summarize_latest_platform_breakdown(item_rows_today)
    latest_platform_totals = summarize_latest_platform_totals(item_rows_today)
    latest_loader_totals = summarize_latest_loader_totals(item_rows_today)
    latest_mc_totals = summarize_latest_mc_totals(item_rows_today)
    latest_mod_totals = summarize_latest_mod_totals(item_rows_today)
    latest_mod_breakdown = summarize_latest_mod_breakdown(item_rows_today)

    write_csv(output_dir / "item_rows_today.csv", item_rows_today)
    write_csv(output_dir / "item_rows_all.csv", item_rows_all)
    write_csv(output_dir / "daily_project_totals.csv", daily_project)
    write_csv(output_dir / "daily_loader_totals.csv", daily_loader)
    write_csv(output_dir / "daily_mc_version_totals.csv", daily_mc)
    write_csv(output_dir / "daily_mod_version_totals.csv", daily_mod)
    write_csv(output_dir / "spikes.csv", spikes)
    write_csv(output_dir / "latest_platform_breakdown.csv", latest_platform_breakdown)
    write_csv(output_dir / "latest_loader_breakdown.csv", latest_loader_breakdown)
    write_csv(output_dir / "latest_mc_breakdown.csv", latest_mc_breakdown)
    write_csv(output_dir / "latest_mod_breakdown.csv", latest_mod_breakdown)
    write_csv(output_dir / "latest_platform_totals.csv", latest_platform_totals)
    write_csv(output_dir / "latest_loader_totals.csv", latest_loader_totals)
    write_csv(output_dir / "latest_mc_totals.csv", latest_mc_totals)
    write_csv(output_dir / "latest_mod_totals.csv", latest_mod_totals)

    summary_payload = {
        "generated_utc_date": today,
        "projects": [p["name"] for p in config["projects"]],
        "today_item_rows_count": len(item_rows_today),
        "spike_count": len(spikes),
        "latest_platform_totals": latest_platform_totals,
        "latest_loader_totals": latest_loader_totals,
        "latest_mc_totals": latest_mc_totals,
        "latest_mod_totals": latest_mod_totals,
        "latest_platform_breakdown": latest_platform_breakdown,
        "latest_loader_breakdown": latest_loader_breakdown,
        "latest_mc_breakdown": latest_mc_breakdown,
        "latest_mod_breakdown": latest_mod_breakdown,
        "release_tags": tags,
    }
    write_json(output_dir / "summary.json", summary_payload)

    chart_paths = build_charts(
        output_dir=output_dir,
        daily_project_records=daily_project,
        daily_loader_records=daily_loader,
        daily_mc_records=daily_mc,
        daily_mod_records=daily_mod,
        dpi=int(config.get("chart_dpi", 140)),
    )

    return {
        "today": today,
        "item_rows_today": item_rows_today,
        "item_rows_all": item_rows_all,
        "daily_project": daily_project,
        "daily_loader": daily_loader,
        "daily_mc": daily_mc,
        "daily_mod": daily_mod,
        "spikes": spikes,
        "latest_platform_totals": latest_platform_totals,
        "latest_loader_totals": latest_loader_totals,
        "latest_mc_totals": latest_mc_totals,
        "latest_mod_totals": latest_mod_totals,
        "latest_platform_breakdown": latest_platform_breakdown,
        "latest_loader_breakdown": latest_loader_breakdown,
        "latest_mc_breakdown": latest_mc_breakdown,
        "latest_mod_breakdown": latest_mod_breakdown,
        "release_tags": tags,
        "output_dir": str(output_dir),
        "chart_paths": [str(p) for p in chart_paths],
    }


def main() -> int:
    db_path = str(CONFIG.get("db_path", "hearthguard_downloads.sqlite3"))
    conn = sqlite3.connect(db_path)

    try:
        create_db(conn)
        sync_release_tags(conn, CONFIG.get("release_tags", []))
        run_fetch(conn, CONFIG)
        analytics = build_analytics(conn, CONFIG)

        if CONFIG.get("verbose_console", True):
            print_today_item_report(analytics["item_rows_today"])

            print_simple_table(
                "Today totals by platform",
                analytics["latest_platform_totals"],
                columns=("platform", "total_downloads"),
                sort_key="total_downloads",
            )

            print_simple_table(
                "Today totals by loader",
                analytics["latest_loader_totals"],
                columns=("loader_group", "total_downloads"),
                sort_key="total_downloads",
            )

            print_simple_table(
                "Today totals by Minecraft version",
                analytics["latest_mc_totals"],
                columns=("mc_version", "total_downloads"),
                sort_key="total_downloads",
                max_rows=15,
            )
            print_simple_table(
                "Today totals by mod version",
                analytics["latest_mod_totals"],
                columns=("mod_version", "total_downloads"),
                sort_key="total_downloads",
                max_rows=15,
            )

            print_simple_table(
                "Today daily platform breakdown",
                analytics["latest_platform_breakdown"],
                columns=("platform", "daily_downloads"),
                sort_key="daily_downloads",
            )

            print_simple_table(
                "Today daily loader breakdown",
                analytics["latest_loader_breakdown"],
                columns=("loader_group", "daily_downloads"),
                sort_key="daily_downloads",
            )

            print_simple_table(
                "Today daily Minecraft version breakdown",
                analytics["latest_mc_breakdown"],
                columns=("mc_version", "daily_downloads"),
                sort_key="daily_downloads",
                max_rows=15,
            )
            print_simple_table(
                "Today daily mod version breakdown",
                analytics["latest_mod_breakdown"],
                columns=("mod_version", "daily_downloads"),
                sort_key="daily_downloads",
                max_rows=15,
            )

            print_simple_table(
                "Detected spikes",
                analytics["spikes"],
                columns=("snapshot_date", "project_name", "platform", "daily_downloads", "rolling_avg_7d", "spike_multiplier", "release_tags"),
                sort_key="spike_multiplier",
                max_rows=15,
            )

            print_header("Files written")
            print(f"Database: {db_path}")
            print(f"Output directory: {analytics['output_dir']}")
            for chart in analytics["chart_paths"]:
                print(f"Chart: {chart}")

        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
