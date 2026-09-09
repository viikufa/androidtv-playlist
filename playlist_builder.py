#!/usr/bin/env python3
"""Сборка единого IPTV-плейлиста из нескольких источников с проверкой потоков.

Читает sources.yaml, скачивает каждый источник, дедуплицирует каналы
(tvg-id > нормализованное имя), проверяет живость HLS/HTTP-потоков и пишет:
  - dist/playlist.m3u8   — итоговый плейлист
  - dist/stats.json      — статистика сборки
  - dist/README.md       — сводка по источникам

Только стандартная библиотека Python 3.9+.
Использование: python3 playlist_builder.py [путь-к-sources.yaml] [выходной-каталог]
"""

import concurrent.futures
import json
import re
import ssl
import sys
import time
import urllib.request
from pathlib import Path
from typing import Optional, Tuple

try:
    import yaml
except ImportError:
    yaml = None

UA = "VLC/3.0.20 LibVLC/3.0.20"  # многие CDN отвечают только на известные UA
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE


def load_config(path: Path) -> dict:
    if yaml is None:
        raise SystemExit("Нужен PyYAML: pip3 install pyyaml")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def fetch(url: str, timeout: int, max_bytes: Optional[int] = None) -> Optional[bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as r:
            data = r.read(max_bytes) if max_bytes else r.read()
            if r.status != 200:
                return None
            return data or None
    except Exception:
        return None


def looks_alive(data: Optional[bytes]) -> bool:
    if not data:
        return False
    head = data[:512].lstrip()
    if head.startswith(b"#EXTM3U"):
        return True
    # HTML-заглушки и текстовые ошибки считаем мёртвыми
    if re.match(rb"<(!doctype|html)", head, re.I):
        return False
    if b"not found" in head.lower() or b"error" in head.lower():
        return False
    return True


def check_stream(url: str, timeout: int, retries: int) -> Tuple[str, bool, str]:
    """Возвращает (url, жив?, причина)."""
    reasons = []
    for attempt in range(retries + 1):
        data = fetch(url, timeout, max_bytes=16_384)
        if looks_alive(data):
            return url, True, ""
        reasons.append("нет ответа/пусто" if data is None else "не похож на поток")
        time.sleep(0.5)
    return url, False, "; ".join(reasons)


def parse_m3u(text: str) -> list:
    """Разбирает m3u в список {name, url, attrs, group}."""
    lines = text.splitlines()
    entries, pending = [], None
    for line in lines:
        line = line.strip()
        if line.startswith("#EXTINF"):
            attrs = dict(re.findall(r'([\w-]+)="([^"]*)"', line))
            name = line.split(",", 1)[1].strip() if "," in line else ""
            pending = {"name": name, "attrs": attrs}
        elif line.startswith("http") and pending is not None:
            pending["url"] = line
            pending["group"] = pending["attrs"].get("group-title", "")
            entries.append(pending)
            pending = None
    return entries


def norm_key(e: dict) -> str:
    tvg = e["attrs"].get("tvg-id", "").strip().lower()
    if tvg:
        return "id:" + tvg
    name = re.sub(r"[\s\-_/\\]+", "", e["name"].lower())
    return "name:" + name


def norm_name(s: str) -> str:
    """Нормализация имени канала: нижний регистр, ё→е, только буквы/цифры, без «hd»."""
    s = s.lower().replace("ё", "е")
    s = re.sub(r"[^a-zа-я0-9]", "", s)
    return re.sub(r"hd$", "", s)


def assign_fixed_order(entries: list, fixed_cfg: list) -> int:
    """Помечает каналы слотами fixed_order (e["_slot"]). Возвращает число помеченных.

    Кандидат — первое (по приоритету источников) неназначенное совпадение.
    Слоты привязаны к записи канала, не к URL: один поток может числиться
    за несколькими каналами-дубликатами.
    """
    marked = 0
    used = set()
    for slot, item in enumerate(fixed_cfg):
        variants = {norm_name(v) for v in item.get("match", [])}
        for e in entries:
            if id(e) in used:
                continue
            if norm_name(e["name"]) in variants:
                e["_slot"] = slot
                used.add(id(e))
                marked += 1
                break
    return marked


SUSPICIOUS = ("орбита", "дубль", "international", "+", "(челябинск", "(новокузнецк", "магазин")


def load_epg_channels(url: str, timeout: int) -> dict:
    """Скачивает XMLTV (gzip или xml) и возвращает {норм-имя: (id, display-name)}.

    При конфликте имён остаётся первая запись с «чистым» именем.
    """
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout * 4, context=SSL_CTX) as r:
        raw = r.read()
    if raw[:2] == b"\x1f\x8b":
        import gzip
        raw = gzip.decompress(raw)
    text = raw.decode("utf-8", "replace")
    channels = {}
    for m in re.finditer(r'<channel id="([^"]+)">\s*<display-name[^>]*>([^<]+)</display-name>', text):
        cid, name = m.group(1), m.group(2)
        key = norm_name(name)
        if not key or key in channels:
            continue
        channels[key] = (cid, name)
    return channels


def match_epg(entries: list, epg: dict) -> int:
    """Сопоставляет каналы с EPG по имени; переписывает tvg-id. Возвращает число совпадений.

    Приоритет: точное совпадение нормализованных имён, затем частичное
    (одно имя начинается с другого), «подозрительные» варианты (орбиты,
    дубли, международные версии) штрафуются.
    """
    matched = 0
    for e in entries:
        mine = norm_name(e["name"])
        if not mine:
            continue
        best, best_score = None, 0
        for key, (cid, dname) in epg.items():
            if key == mine:
                best, best_score = cid, 3
                break
            score = 0
            if key.startswith(mine) or mine.startswith(key):
                score = 1
            if not score:
                continue
            low = dname.lower()
            if any(s in low for s in SUSPICIOUS):
                score = 0.5
            if score > best_score:
                best, best_score = cid, score
        if best:
            e["attrs"]["tvg-id"] = best
            matched += 1
    return matched


def build(cfg: dict, out_dir: Path) -> None:
    timeout = int(cfg.get("timeout", 8))
    retries = int(cfg.get("retries", 1))
    conc = int(cfg.get("concurrency", 24))
    drop_dead = bool(cfg.get("drop_dead", True))

    seen, merged, dead = set(), [], []
    per_source = []

    for src in cfg["sources"]:
        raw = fetch(src["url"], timeout + 4)
        if raw is None:
            per_source.append({"source": src["name"], "status": "fetch failed", "entries": 0})
            continue
        entries = parse_m3u(raw.decode("utf-8", "replace"))
        unique = []
        for e in entries:
            key = norm_key(e)
            if key in seen:
                continue
            seen.add(key)
            unique.append(e)
        for e in unique:
            e["source"] = src["name"]
        merged.extend(unique)
        per_source.append({"source": src["name"], "status": "ok", "entries": len(unique)})
        print(f"[{src['name']}] получено {len(entries)}, уникальных {len(unique)}", file=sys.stderr)

    # Проверка живости
    urls = [e["url"] for e in merged]
    results, reasons = {}, {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=conc) as ex:
        for u, ok, reason in ex.map(
            lambda u: check_stream(u, timeout, retries), urls
        ):
            results[u] = ok
            reasons[u] = reason

    alive = [e for e in merged if results[e["url"]]]
    if drop_dead:
        final = alive
    else:
        for e in merged:
            if not results[e["url"]][0]:
                e["name"] = "[DEAD] " + e["name"]
        final = merged
    dead = [e for e in merged if not results[e["url"]]]

    # Сборка выходного плейлиста: сначала фиксированные слоты (мультиплексы +
    # Чаваш ЕН), затем остальные — по группам, «местные» в конце.
    fixed_cfg = cfg.get("fixed_order") or []
    final = (alive if drop_dead else merged)
    marked = assign_fixed_order(final, fixed_cfg)

    # EPG: сопоставление имён с XMLTV-источником, переписывание tvg-id
    epg_cfg = cfg.get("epg") or {}
    epg_url, epg_matched = epg_cfg.get("url"), None
    if epg_url:
        try:
            epg = load_epg_channels(epg_url, timeout)
            epg_matched = match_epg(final, epg)
            print(f"[EPG] сопоставлено {epg_matched} из {len(final)} каналов", file=sys.stderr)
        except Exception as exc:
            print(f"[EPG] не удалось загрузить {epg_url}: {exc}", file=sys.stderr)
            epg_url = None

    def group_rank(e):
        g = e["group"].lower()
        if "местн" in g or "local" in g or "регион" in g:
            return 1
        return 0

    final.sort(key=lambda e: (e.get("_slot", 999), group_rank(e), e["group"], e["name"]))

    now = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
    header = "#EXTM3U"
    if epg_url:
        header += f' x-tvg-url="{epg_url}"'
    out = [
        header,
        f"# Сборка {now}; каналов: {len(final)} (проверено живых) из "
        f"{len(merged)} уникальных; источники: {', '.join(s['name'] for s in cfg['sources'])}",
        f"# Порядок: эфирные мультиплексы (1-20 по списку РФ) и местные топ-каналы впереди; "
        f"номера каналов в атрибуте tvg-chno",
        (f"# Телепрограмма: {epg_url}; tvg-id переписаны под ID источника EPG"
         if epg_url else "# Телепрограмма: источник не настроен"),
    ]
    for e in final:
        attrs = e["attrs"].copy()
        attrs["group-title"] = e["group"] or "Прочее"
        if "_slot" in e:
            attrs["tvg-chno"] = str(e["_slot"] + 1)
        attr_str = " ".join(f'{k}="{v}"' for k, v in attrs.items())
        out.append(f"#EXTINF:-1 {attr_str},{e['name']}")
        out.append(e["url"])
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "playlist.m3u8").write_text("\n".join(out) + "\n", encoding="utf-8")

    stats = {
        "generated": now,
        "total_unique": len(merged),
        "alive": len(alive),
        "dead": len(dead),
        "published": len(final),
        "sources": per_source,
        "epg": {"url": epg_url, "matched": epg_matched},
        "dead_channels": [
            {"name": e["name"], "source": e["source"], "reason": reasons[e["url"]]}
            for e in dead
        ][:200],
    }
    (out_dir / "stats.json").write_text(
        json.dumps(stats, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print(
        f"Готово: {len(final)} каналов опубликовано "
        f"(мёртвых отброшено: {len(dead) if drop_dead else 0})",
        file=sys.stderr,
    )


if __name__ == "__main__":
    cfg_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("sources.yaml")
    out_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("dist")
    build(load_config(cfg_path), out_dir)
