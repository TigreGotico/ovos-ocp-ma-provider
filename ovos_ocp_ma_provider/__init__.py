"""OVOS OCP Skills music provider for Music Assistant.

Connects to the OVOS messagebus and uses the OCP search protocol
(ovos.common_play.query) to query all installed OCP skills for media.
Results are returned as MA Track objects; URIs are streamed directly
via HTTP using the URLs returned by OCP skills.

The OCP search pipeline handles skill discovery, timeout management, and
result collection via OCPQuery (ovos-bus-client). This provider is a thin
bridge between MA's search API and OCP's bus-based query/response protocol.

One MA search → one OCP query broadcast → N skill responses → MA tracks.

OCP has no library of its own: a track only exists for as long as some
search result names it. To survive MA restarts (MA persists library/queue
entries across the process lifetime and re-resolves them by prov_track_id
on demand) every track a search returns is also written to a small JSON
file under the provider's storage directory, keyed by item_id. On startup
that file is read back to warm the in-memory caches, so get_track() and
get_stream_details() keep working for tracks found in a previous run
without requiring a fresh search. The store is bounded (oldest entries
evicted first) to avoid unbounded growth.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import tempfile
import threading
from collections import OrderedDict
from pathlib import Path
from typing import TYPE_CHECKING

from music_assistant_models.media_items import AudioFormat, SearchResults
from music_assistant_models.config_entries import ConfigEntry
from music_assistant_models.enums import (
    ConfigEntryType,
    ContentType,
    ImageType,
    MediaType,
    ProviderFeature,
    StreamType,
)
from music_assistant_models.media_items import (
    ItemMapping,
    MediaItemImage,
    MediaItemMetadata,
    ProviderMapping,
    Track,
)
from music_assistant_models.streamdetails import StreamDetails

from music_assistant.models.music_provider import MusicProvider

if TYPE_CHECKING:
    from music_assistant_models.config_entries import ProviderConfig
    from music_assistant_models.provider import ProviderManifest
    from music_assistant.mass import MusicAssistant
    from music_assistant.models import ProviderInstanceType

_LOGGER = logging.getLogger(__name__)

SUPPORTED_FEATURES = {
    ProviderFeature.SEARCH,
}

CONF_HOST = "host"
CONF_PORT = "port"
CONF_OCP_TIMEOUT = "ocp_timeout"
CONF_MIN_CONFIDENCE = "min_confidence"

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8181
DEFAULT_OCP_TIMEOUT = 10
DEFAULT_MIN_CONFIDENCE = 50  # MatchConfidence.AVERAGE — OCP scores are 0-100, not 0.0-1.0

_TRACK_CACHE_MAX = 2000  # max item_id → track entries kept (memory and on disk)
_STORE_FILENAME = "ocp_tracks.json"


async def setup(
    mass: MusicAssistant, manifest: ProviderManifest, config: ProviderConfig
) -> ProviderInstanceType:
    return OVOSOCPProvider(mass, manifest, config, SUPPORTED_FEATURES)


def _stable_id(*parts: str) -> str:
    """Deterministic 16-char item_id from variable string parts."""
    return hashlib.sha1("|".join(parts).encode()).hexdigest()[:16]


def _build_track(
    item_id: str,
    uri: str,
    title: str,
    artist_name: str,
    duration_s: int,
    image_url: str | None,
    provider_instance_id: str,
    provider_domain: str,
) -> Track:
    """Build an MA Track from already-normalized fields.

    Shared by the OCP search-result path and the on-disk store loader so
    both produce identical Track objects for the same item_id.
    """
    artist_id = _stable_id(provider_instance_id, "artist", artist_name)

    metadata = MediaItemMetadata()
    if image_url:
        metadata.images = [
            MediaItemImage(
                type=ImageType.THUMB,
                path=image_url,
                provider=provider_domain,
                remotely_accessible=True,
            )
        ]

    return Track(
        item_id=item_id,
        provider=provider_instance_id,
        name=title,
        duration=duration_s,
        provider_mappings={
            ProviderMapping(
                item_id=item_id,
                provider_domain=provider_domain,
                provider_instance=provider_instance_id,
                audio_format=AudioFormat(content_type=ContentType.UNKNOWN),
                available=True,
                details=uri,  # the playable OCP uri
            )
        },
        artists=[
            ItemMapping(
                item_id=artist_id,
                provider=provider_instance_id,
                name=artist_name,
                media_type=MediaType.ARTIST,
            )
        ],
        metadata=metadata,
    )


def _ocp_entry_to_track(
    entry: dict,
    provider_instance_id: str,
    provider_domain: str,
) -> Track | None:
    """Convert a single OCP MediaEntry dict to an MA Track.

    Returns None for entries without a URI or with non-audio playback type.
    """
    uri = entry.get("uri", "")
    if not uri:
        return None

    # OCP playback types: "audio", "video", "webview", "skill"
    playback = str(entry.get("playback", "audio")).lower()
    if playback in ("video", "webview"):
        return None

    title = entry.get("title") or uri
    artist_name = entry.get("artist") or entry.get("skill_id") or "Unknown"
    duration_ms = entry.get("length") or 0
    duration_s = int(duration_ms // 1000) if duration_ms else 0
    image_url = entry.get("image") or entry.get("bg_image")

    item_id = _stable_id(provider_instance_id, uri)
    return _build_track(
        item_id, uri, title, artist_name, duration_s, image_url,
        provider_instance_id, provider_domain,
    )


def _collect_tracks(
    raw_results: list[dict],
    provider_instance_id: str,
    provider_domain: str,
    min_confidence: float,
) -> list[Track]:
    """Flatten all OCP skill result dicts to a deduplicated list of MA Tracks."""
    seen_uris: set[str] = set()
    tracks: list[Track] = []

    for skill_result in raw_results:
        entries = sorted(
            skill_result.get("results", []),
            key=lambda e: float(e.get("match_confidence", 0) or 0),
            reverse=True,
        )
        for entry in entries:
            try:
                confidence = float(entry.get("match_confidence", 0) or 0)
            except (TypeError, ValueError):
                confidence = 0.0
            if confidence < min_confidence:
                continue
            uri = entry.get("uri", "")
            if not uri or uri in seen_uris:
                continue
            seen_uris.add(uri)
            track = _ocp_entry_to_track(entry, provider_instance_id, provider_domain)
            if track:
                tracks.append(track)

    return tracks


def _track_to_store_entry(track: Track) -> dict | None:
    """Reduce a Track to the plain-dict shape written to the on-disk store."""
    mapping = next(iter(track.provider_mappings), None)
    uri = mapping.details if mapping else None
    if not uri:
        return None
    image = None
    if track.metadata and track.metadata.images:
        image = track.metadata.images[0].path
    artist = track.artists[0] if track.artists else None
    return {
        "uri": uri,
        "title": track.name,
        "duration": track.duration or 0,
        "image": image,
        "artist_name": artist.name if artist else "Unknown",
    }


def _store_entry_to_track(
    item_id: str, entry: dict, provider_instance_id: str, provider_domain: str
) -> Track | None:
    """Rebuild a Track from a stored on-disk entry."""
    uri = entry.get("uri")
    if not uri:
        return None
    return _build_track(
        item_id,
        uri,
        entry.get("title") or uri,
        entry.get("artist_name") or "Unknown",
        int(entry.get("duration") or 0),
        entry.get("image"),
        provider_instance_id,
        provider_domain,
    )


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------

class OVOSOCPProvider(MusicProvider):
    """Music provider that queries OVOS OCP skills for media via the messagebus."""

    async def get_config_entries(self) -> tuple[ConfigEntry, ...]:
        return (
            ConfigEntry(
                key=CONF_HOST,
                type=ConfigEntryType.STRING,
                label="OVOS messagebus host",
                required=False,
                default_value=DEFAULT_HOST,
                description="Hostname or IP of the OVOS messagebus (must be reachable from MA).",
            ),
            ConfigEntry(
                key=CONF_PORT,
                type=ConfigEntryType.INTEGER,
                label="OVOS messagebus port",
                required=False,
                default_value=DEFAULT_PORT,
            ),
            ConfigEntry(
                key=CONF_OCP_TIMEOUT,
                type=ConfigEntryType.INTEGER,
                label="OCP search timeout (seconds)",
                required=False,
                default_value=DEFAULT_OCP_TIMEOUT,
                description=(
                    "How long to wait for OCP skills to respond. "
                    "Skills may extend this via the OCP protocol."
                ),
            ),
            ConfigEntry(
                key=CONF_MIN_CONFIDENCE,
                type=ConfigEntryType.FLOAT,
                label="Minimum match confidence (0-100)",
                required=False,
                default_value=DEFAULT_MIN_CONFIDENCE,
                description=(
                    "OCP results below this score are discarded. Matches OCP's own "
                    "0-100 match_confidence scale (50 = MatchConfidence.AVERAGE)."
                ),
            ),
        )

    # ---------------------------------------------------------------------------
    # On-disk track store (restart survival)
    # ---------------------------------------------------------------------------

    def _resolve_store_path(self) -> Path:
        """Where the per-instance track store lives.

        MA hands every provider a persistent storage_path on the running
        MusicAssistant instance; fall back to a tempdir so the provider is
        still importable/testable outside a live MA process.
        """
        base = Path(getattr(self.mass, "storage_path", None) or tempfile.gettempdir())
        store_dir = base / "providers" / self.instance_id
        store_dir.mkdir(parents=True, exist_ok=True)
        return store_dir / _STORE_FILENAME

    def _load_store(self) -> None:
        """Warm the in-memory track cache from the on-disk store, if present."""
        if not self._store_path.exists():
            return
        try:
            raw = json.loads(self._store_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as err:
            self.logger.warning("Could not read OCP track store %s: %s", self._store_path, err)
            return
        for item_id, entry in raw.items():
            track = _store_entry_to_track(item_id, entry, self.instance_id, self.domain)
            if track:
                self._track_cache[item_id] = track

    def _persist_store(self) -> None:
        """Write the current track cache to disk, keyed by item_id."""
        entries = {}
        for item_id, track in self._track_cache.items():
            entry = _track_to_store_entry(track)
            if entry:
                entries[item_id] = entry
        try:
            tmp_path = self._store_path.with_suffix(".json.tmp")
            tmp_path.write_text(json.dumps(entries), encoding="utf-8")
            tmp_path.replace(self._store_path)
        except OSError as err:
            self.logger.warning("Could not persist OCP track store %s: %s", self._store_path, err)

    # ---------------------------------------------------------------------------
    # Lifecycle
    # ---------------------------------------------------------------------------

    async def handle_async_init(self) -> None:
        try:
            from ovos_bus_client import MessageBusClient, Message  # noqa: PLC0415
            from ovos_bus_client.apis.ocp import OCPQuery  # noqa: PLC0415
            self.Message = Message
            self._MessageBusClient = MessageBusClient
            self._OCPQuery = OCPQuery
        except ImportError as err:
            from music_assistant_models.errors import ProviderUnavailableError
            raise ProviderUnavailableError(
                "ovos-bus-client not installed or too old (needs >=0.0.8)") from err

        host = self.config.get_value(CONF_HOST) or DEFAULT_HOST
        port = int(self.config.get_value(CONF_PORT) or DEFAULT_PORT)

        self.bus = self._MessageBusClient(host=host, port=port, route="/core", ssl=False)
        t = threading.Thread(target=self.bus.run_forever, daemon=True)
        t.start()
        self.bus.connected_event.wait(timeout=10)
        if not self.bus.connected_event.is_set():
            from music_assistant_models.errors import ProviderUnavailableError
            raise ProviderUnavailableError(
                f"Could not connect to OVOS messagebus at {host}:{port}")

        self.logger.info("OCP provider connected to OVOS messagebus at %s:%s", host, port)

        timeout = int(self.config.get_value(CONF_OCP_TIMEOUT) or DEFAULT_OCP_TIMEOUT)
        min_conf_raw = self.config.get_value(CONF_MIN_CONFIDENCE)
        self._min_confidence = float(min_conf_raw) if min_conf_raw is not None else DEFAULT_MIN_CONFIDENCE
        self._ocp_config = {
            "min_timeout": timeout,
            "max_timeout": timeout + 5,
            "allow_extensions": True,
            "early_stop_thresh": 90,
            "early_stop_grace_period": 0.5,
        }

        self._track_cache: OrderedDict[str, Track] = OrderedDict()
        self._store_path = self._resolve_store_path()
        self._load_store()

    # ------------------------------------------------------------------
    # Internal: synchronous OCP search (called via asyncio.to_thread)
    # ------------------------------------------------------------------

    def _run_ocp_query(self, phrase: str, ocp_media_type) -> list[dict]:
        """Broadcast an OCP query and block until all skills respond or timeout."""
        query = self._OCPQuery(
            query=phrase,
            bus=self.bus,
            media_type=ocp_media_type,
            config=self._ocp_config,
        )
        query.send(source_message=self.Message("music_assistant.search"))
        query.wait()
        return query.results

    # ------------------------------------------------------------------
    # MusicProvider interface
    # ------------------------------------------------------------------

    async def search(
        self,
        search_query: str,
        media_types: list[MediaType] | None = None,
        limit: int = 25,
    ) -> SearchResults:
        """Search OCP skills and return results as MA Tracks."""
        from ovos_utils.ocp import MediaType as OcpMediaType  # noqa: PLC0415

        ocp_media_type = OcpMediaType.GENERIC
        if media_types:
            if MediaType.TRACK in media_types:
                ocp_media_type = OcpMediaType.MUSIC
            elif MediaType.PODCAST in media_types:
                ocp_media_type = OcpMediaType.PODCAST
            elif MediaType.RADIO in media_types:
                ocp_media_type = OcpMediaType.RADIO

        self.logger.debug(
            "OCP search: %r (ocp_type=%s, min_conf=%.2f)",
            search_query, ocp_media_type.name, self._min_confidence,
        )

        raw = await asyncio.to_thread(self._run_ocp_query, search_query, ocp_media_type)

        tracks = _collect_tracks(raw, self.instance_id, self.domain, self._min_confidence)
        for track in tracks:
            self._track_cache[track.item_id] = track
        while len(self._track_cache) > _TRACK_CACHE_MAX:
            self._track_cache.popitem(last=False)
        self._persist_store()
        self.logger.debug(
            "OCP search %r: %d skills, %d tracks after filtering",
            search_query, len(raw), len(tracks),
        )
        return SearchResults(tracks=tracks[:limit])

    async def get_track(self, prov_track_id: str) -> Track:
        """Return a track by id.

        OCP has no library of its own: an id only resolves if some earlier
        search produced it. The in-memory cache is warmed from the on-disk
        store on startup, so this also resolves tracks found before an MA
        restart, as long as the store still holds that entry.
        """
        track = self._track_cache.get(prov_track_id)
        if track is None:
            from music_assistant_models.errors import MediaNotFoundError
            raise MediaNotFoundError(
                f"Track {prov_track_id!r} is not in the OCP track store — "
                "search for it again to refresh."
            )
        self._track_cache.move_to_end(prov_track_id)
        return track

    async def get_stream_details(
        self, item_id: str, media_type: MediaType = MediaType.TRACK
    ) -> StreamDetails:
        """Return stream details for a track using its stored OCP uri.

        :param item_id: ProviderMapping.item_id — stable hash of instance_id + uri.
        :param media_type: MA media type of the item.
        """
        track = self._track_cache.get(item_id)
        if track is None:
            from music_assistant_models.errors import MediaNotFoundError
            raise MediaNotFoundError(
                f"Stream URI for {item_id!r} not in the OCP track store — "
                "search for this track again to refresh."
            )
        mapping = next(iter(track.provider_mappings))
        uri = mapping.details
        self._track_cache.move_to_end(item_id)
        return StreamDetails(
            provider=self.domain,
            item_id=item_id,
            audio_format=AudioFormat(content_type=ContentType.UNKNOWN),
            media_type=media_type,
            stream_type=StreamType.HTTP,
            path=uri,
            can_seek=True,
            allow_seek=True,
        )

    async def get_library_tracks(self):
        """OCP is search-only and has no browsable library."""
        from music_assistant_models.errors import UnsupportedFeaturedException
        raise UnsupportedFeaturedException(
            f"{self.domain} has no library — search-only provider")
        yield  # pragma: no cover - makes this an async generator

    async def get_artist(self, prov_artist_id: str):
        """Artists are not independently resolvable; they only exist as track fields."""
        from music_assistant_models.errors import UnsupportedFeaturedException
        raise UnsupportedFeaturedException(
            f"{self.domain} does not support artist lookups")

    async def get_album(self, prov_album_id: str):
        """OCP has no concept of albums."""
        from music_assistant_models.errors import UnsupportedFeaturedException
        raise UnsupportedFeaturedException(
            f"{self.domain} does not support album lookups")

    async def get_playlist(self, prov_playlist_id: str):
        """OCP has no concept of playlists."""
        from music_assistant_models.errors import UnsupportedFeaturedException
        raise UnsupportedFeaturedException(
            f"{self.domain} does not support playlist lookups")

    async def get_radio(self, prov_radio_id: str):
        """Radio stations returned by search are tracks; there is no separate radio store."""
        from music_assistant_models.errors import UnsupportedFeaturedException
        raise UnsupportedFeaturedException(
            f"{self.domain} does not support radio lookups")

    async def unload(self, is_removed: bool = False) -> None:
        if getattr(self, "bus", None):
            self.bus.close()
        if is_removed and getattr(self, "_store_path", None) and self._store_path.exists():
            try:
                self._store_path.unlink()
            except OSError:
                pass
