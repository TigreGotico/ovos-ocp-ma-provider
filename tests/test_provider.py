"""Tests for ovos_ocp_ma_provider.

No live MA server and no live OVOS messagebus are needed: search() is
exercised by monkeypatching the synchronous OCP query hook
(`_run_ocp_query`), which is the only place the provider touches the bus.
"""

from __future__ import annotations

import json

import pytest
from music_assistant_models.errors import MediaNotFoundError, UnsupportedFeaturedException
from music_assistant_models.media_items import AudioFormat, SearchResults, Track

from ovos_ocp_ma_provider import _stable_id


def _fake_ocp_query_response(entries):
    """Shape returned by OCPQuery.results: one dict per responding skill."""
    return [{"skill_id": "skill.fake", "results": entries}]


TRACK_ENTRY = {
    "uri": "https://example.com/song.mp3",
    "title": "Bohemian Rhapsody",
    "artist": "Queen",
    "length": 355000,  # ms
    "image": "https://example.com/cover.jpg",
    "match_confidence": 90,
    "playback": "audio",
}


# ---------------------------------------------------------------------------
# search()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_search_returns_track_from_ocp_response(make_provider, monkeypatch):
    provider = make_provider()
    monkeypatch.setattr(
        provider, "_run_ocp_query",
        lambda phrase, media_type: _fake_ocp_query_response([TRACK_ENTRY]),
    )

    results = await provider.search("bohemian rhapsody")

    assert isinstance(results, SearchResults)
    assert len(results.tracks) == 1
    track = results.tracks[0]
    assert isinstance(track, Track)
    assert track.name == "Bohemian Rhapsody"
    assert track.artists[0].name == "Queen"
    assert track.duration == 355
    mapping = next(iter(track.provider_mappings))
    assert mapping.details == TRACK_ENTRY["uri"]
    assert mapping.audio_format == AudioFormat(content_type=mapping.audio_format.content_type)


@pytest.mark.asyncio
async def test_search_filters_below_min_confidence(make_provider, monkeypatch):
    provider = make_provider(min_confidence=95)
    low_conf = dict(TRACK_ENTRY, match_confidence=50)
    monkeypatch.setattr(
        provider, "_run_ocp_query",
        lambda phrase, media_type: _fake_ocp_query_response([low_conf]),
    )

    results = await provider.search("bohemian rhapsody")

    assert results.tracks == []


@pytest.mark.asyncio
async def test_search_dedupes_by_uri_across_skills(make_provider, monkeypatch):
    provider = make_provider()
    responses = [
        {"skill_id": "skill.a", "results": [TRACK_ENTRY]},
        {"skill_id": "skill.b", "results": [dict(TRACK_ENTRY, match_confidence=70)]},
    ]
    monkeypatch.setattr(provider, "_run_ocp_query", lambda phrase, media_type: responses)

    results = await provider.search("bohemian rhapsody")

    assert len(results.tracks) == 1


@pytest.mark.asyncio
async def test_search_skips_video_playback_entries(make_provider, monkeypatch):
    provider = make_provider()
    video_entry = dict(TRACK_ENTRY, playback="video")
    monkeypatch.setattr(
        provider, "_run_ocp_query",
        lambda phrase, media_type: _fake_ocp_query_response([video_entry]),
    )

    results = await provider.search("bohemian rhapsody")

    assert results.tracks == []


# ---------------------------------------------------------------------------
# get_track(): warm cache, cold cache (restart survival), not-found
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_track_warm_cache(make_provider, monkeypatch):
    provider = make_provider()
    monkeypatch.setattr(
        provider, "_run_ocp_query",
        lambda phrase, media_type: _fake_ocp_query_response([TRACK_ENTRY]),
    )
    results = await provider.search("bohemian rhapsody")
    item_id = results.tracks[0].item_id

    track = await provider.get_track(item_id)

    assert track.name == "Bohemian Rhapsody"


@pytest.mark.asyncio
async def test_get_track_not_found_raises(make_provider):
    provider = make_provider()

    with pytest.raises(MediaNotFoundError):
        await provider.get_track("nonexistent-item-id")


@pytest.mark.asyncio
async def test_get_track_survives_restart_via_on_disk_store(make_provider, fake_mass, monkeypatch):
    """The defect this guards: an MA restart must not lose tracks it already
    persisted to its library/queue. A new provider instance pointed at the
    same storage dir must resolve a track a previous instance searched for,
    with no new search performed.
    """
    provider_a = make_provider(mass=fake_mass)
    monkeypatch.setattr(
        provider_a, "_run_ocp_query",
        lambda phrase, media_type: _fake_ocp_query_response([TRACK_ENTRY]),
    )
    results = await provider_a.search("bohemian rhapsody")
    item_id = results.tracks[0].item_id

    # Simulate an MA restart: a brand-new provider instance, same storage dir,
    # no search performed before the lookup.
    provider_b = make_provider(mass=fake_mass)

    track = await provider_b.get_track(item_id)

    assert track.name == "Bohemian Rhapsody"
    assert track.item_id == item_id


def test_on_disk_store_file_actually_contains_the_track(make_provider, fake_mass, monkeypatch):
    """Direct proof the persistence path is exercised, independent of the
    restart test above: if the on-disk write is removed from the code, this
    file simply won't exist after search().
    """
    import asyncio

    provider = make_provider(mass=fake_mass)
    monkeypatch.setattr(
        provider, "_run_ocp_query",
        lambda phrase, media_type: _fake_ocp_query_response([TRACK_ENTRY]),
    )
    asyncio.run(provider.search("bohemian rhapsody"))

    assert provider._store_path.exists()
    stored = json.loads(provider._store_path.read_text())
    assert len(stored) == 1
    (entry,) = stored.values()
    assert entry["uri"] == TRACK_ENTRY["uri"]
    assert entry["title"] == "Bohemian Rhapsody"


# ---------------------------------------------------------------------------
# get_stream_details()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_stream_details_resolves_playable_uri(make_provider, monkeypatch):
    provider = make_provider()
    monkeypatch.setattr(
        provider, "_run_ocp_query",
        lambda phrase, media_type: _fake_ocp_query_response([TRACK_ENTRY]),
    )
    results = await provider.search("bohemian rhapsody")
    item_id = results.tracks[0].item_id

    details = await provider.get_stream_details(item_id)

    assert details.path == TRACK_ENTRY["uri"]
    assert details.can_seek is True


@pytest.mark.asyncio
async def test_get_stream_details_survives_restart(make_provider, fake_mass, monkeypatch):
    provider_a = make_provider(mass=fake_mass)
    monkeypatch.setattr(
        provider_a, "_run_ocp_query",
        lambda phrase, media_type: _fake_ocp_query_response([TRACK_ENTRY]),
    )
    results = await provider_a.search("bohemian rhapsody")
    item_id = results.tracks[0].item_id

    provider_b = make_provider(mass=fake_mass)
    details = await provider_b.get_stream_details(item_id)

    assert details.path == TRACK_ENTRY["uri"]


@pytest.mark.asyncio
async def test_get_stream_details_not_found_raises(make_provider):
    provider = make_provider()

    with pytest.raises(MediaNotFoundError):
        await provider.get_stream_details("nonexistent-item-id")


# ---------------------------------------------------------------------------
# Unsupported library surface: documented "unsupported" signal, not a bare
# NotImplementedError traceback.
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_artist_raises_documented_unsupported_signal(make_provider):
    provider = make_provider()

    with pytest.raises(UnsupportedFeaturedException):
        await provider.get_artist("some-artist-id")


@pytest.mark.asyncio
async def test_get_album_raises_documented_unsupported_signal(make_provider):
    provider = make_provider()

    with pytest.raises(UnsupportedFeaturedException):
        await provider.get_album("some-album-id")


@pytest.mark.asyncio
async def test_get_library_tracks_raises_documented_unsupported_signal(make_provider):
    provider = make_provider()

    with pytest.raises(UnsupportedFeaturedException):
        async for _ in provider.get_library_tracks():
            pass


# ---------------------------------------------------------------------------
# AudioFormat import / usage sanity
# ---------------------------------------------------------------------------

def test_audio_format_importable_and_used_by_module():
    from ovos_ocp_ma_provider import AudioFormat as ModuleAudioFormat
    assert ModuleAudioFormat is AudioFormat


def test_stable_id_is_deterministic():
    a = _stable_id("instance-1", "https://example.com/x.mp3")
    b = _stable_id("instance-1", "https://example.com/x.mp3")
    c = _stable_id("instance-2", "https://example.com/x.mp3")
    assert a == b
    assert a != c
