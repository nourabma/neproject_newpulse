from __future__ import annotations

import json
import os
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, List

import feedparser

FEEDS: Dict[str, str] = {
    "DW":           "https://rss.dw.com/rdf/rss-en-world",
    "France24":     "https://www.france24.com/en/rss",
    "CBC":          "https://www.cbc.ca/cmlink/rss-world",
    "TheHindu":     "https://www.thehindu.com/news/international/feeder/default.rss",
    "TimesOfIndia": "https://timesofindia.indiatimes.com/rssfeeds/296589292.cms",
}


@dataclass(frozen=True)
class Article:
    source: str
    title: str
    url: str
    ts: str


def _iso_z(entry) -> str:
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if parsed:
        try:
            return datetime(*parsed[:6], tzinfo=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        except (TypeError, ValueError):
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class FeedPoller:
    """Pull a handful of RSS feeds on a fixed cadence into JSONL files."""

    def __init__(
        self,
        out_dir: str = "data/incoming",
        interval_seconds: int = 50,
        feeds: Dict[str, str] = FEEDS,
        per_feed_limit: int = 20,
    ) -> None:
        self.out_dir = out_dir
        self.interval_seconds = interval_seconds
        self.feeds = feeds
        self.per_feed_limit = per_feed_limit
        self._stop = threading.Event()
        os.makedirs(self.out_dir, exist_ok=True)

    def stop(self) -> None:
        self._stop.set()

    def _fetch(self, source: str, url: str) -> List[Article]:
        try:
            feed = feedparser.parse(url)
        except Exception as exc:
            print(f"[ingest] {source}: fetch error: {exc!r}", file=sys.stderr)
            return []
        if getattr(feed, "bozo", False) and not feed.entries:
            print(f"[ingest] {source}: feed unreadable, skipping", file=sys.stderr)
            return []
        articles: List[Article] = []
        for entry in feed.entries[: self.per_feed_limit]:
            title = (entry.get("title") or "").strip()
            if not title:
                continue
            articles.append(
                Article(
                    source=source,
                    title=title,
                    url=entry.get("link", ""),
                    ts=_iso_z(entry),
                )
            )
        return articles

    def tick(self) -> int:
        bag: List[Article] = []
        for source, url in self.feeds.items():
            bag.extend(self._fetch(source, url))
        if not bag:
            print("[ingest] tick produced 0 rows", file=sys.stderr)
            return 0
        filename = f"feed-{int(time.time() * 1000)}.jsonl"
        path = os.path.join(self.out_dir, filename)
        with open(path, "w", encoding="utf-8") as fh:
            for article in bag:
                fh.write(json.dumps(asdict(article), ensure_ascii=False))
                fh.write("\n")
        print(f"[ingest] wrote {len(bag):3d} rows -> {filename}")
        return len(bag)

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as exc:
                print(f"[ingest] tick crashed: {exc!r}", file=sys.stderr)
            self._stop.wait(self.interval_seconds)


def main() -> None:
    FeedPoller().run()


if __name__ == "__main__":
    main()
