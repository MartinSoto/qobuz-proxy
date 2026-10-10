"""Identity of a published queue entry, read back from its stream URL.

The publisher keeps a mirror of the device queue as a list of EntryKey and
needs to recognise entries again when it reads the queue back from the device
(after a restart, a retarget, or whenever it is unsure what the device holds).
The URL is all the device preserves, so the key is derived from it: the Qobuz
track id and the queue item id (see SonosAudioProxyServer's module docstring
for the `s`/`v`/`i` parameters). An item id alone is unique only within one
load, but a mirror only ever describes one generation at a time.
"""

from dataclasses import dataclass
from typing import Optional
from urllib.parse import parse_qs, urlparse


@dataclass(frozen=True)
class EntryKey:
    """What identifies an entry within one queue generation."""

    track_id: str
    queue_item_id: int


@dataclass(frozen=True)
class ParsedEntryUrl:
    """What a published entry's URL says about it."""

    key: EntryKey
    session: Optional[str]  # `s`: session tag, if the URL carries a generation
    major: Optional[int]  # `v`: queue major version, if it carries one


def parse_entry_url(url: str) -> Optional[ParsedEntryUrl]:
    """Read an entry's identity back from its proxy URL.

    Returns None for anything that is not one of our entries with an item id:
    another service's URL, or an older URL from before item ids were part of it
    (those cannot be matched to a queue item and are treated as foreign).
    """
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")
    if len(parts) != 2 or parts[0] != "audio" or not parts[1]:
        return None
    query = parse_qs(parsed.query)
    try:
        item_id = int(query["i"][0])
    except (KeyError, IndexError, ValueError):
        return None
    major: Optional[int] = None
    if "v" in query:
        try:
            major = int(query["v"][0])
        except ValueError:
            major = None
    session = query["s"][0] if "s" in query else None
    return ParsedEntryUrl(key=EntryKey(parts[1], item_id), session=session, major=major)
