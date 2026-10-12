#!/usr/bin/env python3
"""
Pull the N most recent messages from each Telegram channel in a list.

Setup:
    python -m pip install telethon

    Put your credentials in a .env file next to this script:
        TG_API_ID=1234567
        TG_API_HASH=0123456789abcdef0123456789abcdef
    or export them in your shell. Shell values override the .env file.
    Keep .env and *.session out of git.

Usage:
    python tg_scrape.py channels.txt
    python tg_scrape.py channels.txt --limit 100 --out data --format csv

channels.txt holds one channel per line, in any of these forms:
    durov
    @durov
    https://t.me/durov
Blank lines and lines starting with # are ignored.

On the first run Telethon asks for your phone number and the login code
Telegram sends you (plus your 2FA password if you set one). It saves the
session to <session>.session and reuses it after that. Guard that file like
a password: anyone holding it can act as your account.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    RPCError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.types import Channel, MessageService

log = logging.getLogger("tg_scrape")

USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{4,32}$")

CSV_FIELDS = [
    "channel", "id", "date", "edit_date", "text", "views", "forwards",
    "replies", "reactions", "media", "grouped_id", "reply_to",
    "forwarded", "post_author", "url",
]


def load_env_file(path: Path) -> None:
    """Read KEY=VALUE lines from a .env file. Existing env vars take priority."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key.startswith("export "):
            key = key[len("export "):].strip()
        os.environ.setdefault(key, value.strip().strip("\"'"))


def normalize(line: str) -> str | None:
    """Turn '@name', 't.me/name', 'https://t.me/s/name' etc. into 'name'."""
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    s = re.sub(r"^(https?://)?(www\.)?(t\.me|telegram\.me)/(s/)?", "", s, flags=re.I)
    s = s.lstrip("@").split("/")[0].split("?")[0]
    return s or None


def load_channels(path: Path) -> list[str]:
    seen, out = set(), []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        name = normalize(line)
        if name is None:
            continue
        if not USERNAME_RE.match(name):
            log.warning("line %d: %r doesn't look like a public username, skipping", lineno, line.strip())
            continue
        if name.lower() not in seen:
            seen.add(name.lower())
            out.append(name)
    return out


def message_to_row(msg, channel: str) -> dict:
    return {
        "channel": channel,
        "id": msg.id,
        "date": msg.date.isoformat() if msg.date else None,
        "edit_date": msg.edit_date.isoformat() if msg.edit_date else None,
        "text": msg.message or "",
        "views": msg.views,
        "forwards": msg.forwards,
        "replies": msg.replies.replies if msg.replies else None,
        "reactions": sum(r.count for r in msg.reactions.results) if msg.reactions else None,
        "media": type(msg.media).__name__ if msg.media else None,
        "grouped_id": msg.grouped_id,  # shared by every message in one album
        "reply_to": msg.reply_to_msg_id,
        "forwarded": msg.fwd_from is not None,
        "post_author": msg.post_author,
        "url": f"https://t.me/{channel}/{msg.id}",
    }


async def scrape_channel(client: TelegramClient, username: str, limit: int):
    entity = await client.get_entity(username)
    if not isinstance(entity, Channel):
        raise ValueError(f"@{username} is a user account, not a channel or group")

    rows = []
    # iter_messages yields newest first. Service messages ("channel created",
    # "message pinned") get skipped so they don't eat into the limit.
    async for msg in client.iter_messages(entity):
        if isinstance(msg, MessageService):
            continue
        rows.append(message_to_row(msg, username))
        if len(rows) >= limit:
            break
    return entity, rows


def write_output(out_dir: Path, fmt: str, username: str, entity, rows: list[dict]) -> Path:
    if fmt == "json":
        path = out_dir / f"{username}.json"
        payload = {
            "channel": username,
            "title": getattr(entity, "title", None),
            "channel_id": entity.id,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "count": len(rows),
            "messages": rows,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        path = out_dir / f"{username}.csv"
        # utf-8-sig so Excel shows emoji and non-Latin text correctly
        with path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
    return path


async def run(args: argparse.Namespace) -> int:
    load_env_file(Path(__file__).resolve().with_name(".env"))
    api_id = os.environ.get("TG_API_ID")
    api_hash = os.environ.get("TG_API_HASH")
    if not api_id or not api_hash:
        log.error("Set TG_API_ID and TG_API_HASH (from my.telegram.org) in a .env file "
                  "next to this script, or export them in your shell.")
        return 2

    channels = load_channels(Path(args.channels))
    if not channels:
        log.error("No valid channel usernames in %s", args.channels)
        return 2

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Telethon sleeps through any FLOOD_WAIT shorter than flood_sleep_threshold
    # on its own. Longer waits raise FloodWaitError, handled below.
    client = TelegramClient(args.session, int(api_id), api_hash,
                            flood_sleep_threshold=args.flood_sleep)

    ok, failed = [], []
    async with client:  # prompts for phone + code on the first run
        me = await client.get_me()
        log.info("Logged in as %s (id %s)", me.username or me.first_name, me.id)

        for i, name in enumerate(channels, 1):
            log.info("[%d/%d] @%s", i, len(channels), name)
            try:
                entity, rows = await scrape_channel(client, name, args.limit)
            except FloodWaitError as e:
                log.error("Telegram wants a %ds wait (longer than --flood-sleep). Stopping here; "
                          "re-run later and already-resolved channels will come from the session cache.",
                          e.seconds)
                failed.extend(channels[i - 1:])
                break
            except (UsernameInvalidError, UsernameNotOccupiedError) as e:
                log.warning("  @%s: username doesn't exist (%s)", name, e.__class__.__name__)
                failed.append(name)
                continue
            except ChannelPrivateError:
                log.warning("  @%s: private, or your account is banned from it", name)
                failed.append(name)
                continue
            except (ValueError, RPCError) as e:
                log.warning("  @%s: %s", name, e)
                failed.append(name)
                continue

            path = write_output(out_dir, args.format, name, entity, rows)
            log.info("  %d messages -> %s", len(rows), path)
            ok.append(name)
            await asyncio.sleep(args.delay)

    log.info("Done: %d succeeded, %d failed", len(ok), len(failed))
    if failed:
        log.info("Failed: %s", ", ".join(failed))
    return 0 if not failed else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scrape recent messages from Telegram channels.")
    p.add_argument("channels", help="text file with one channel username per line")
    p.add_argument("--limit", type=int, default=100, help="messages per channel (default 100)")
    p.add_argument("--out", default="output", help="output directory (default ./output)")
    p.add_argument("--format", choices=["json", "csv"], default="json", help="one file per channel")
    p.add_argument("--session", default="tg_scraper", help="session file name (default tg_scraper)")
    p.add_argument("--delay", type=float, default=1.0, help="seconds to pause between channels")
    p.add_argument("--flood-sleep", type=int, default=300,
                   help="auto-sleep through flood waits up to this many seconds (default 300)")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    logging.getLogger("telethon").setLevel(logging.WARNING)
    sys.exit(asyncio.run(run(parse_args())))


if __name__ == "__main__":
    main()