"""Test fixtures for ovos_ocp_ma_provider.

The Music Assistant server package (`music_assistant`) is not published to
PyPI — it only exists inside a running MA installation — so it cannot be a
real test dependency. This conftest installs a minimal stand-in for
`music_assistant.models.music_provider.MusicProvider` that mirrors the
attribute surface (`mass`, `manifest`, `config`, `domain`, `instance_id`,
`logger`) the real `Provider`/`MusicProvider` base classes expose, built
from the public `music_assistant_models` package (which *is* on PyPI and
is the real base MA providers build config/media objects against).
"""

from __future__ import annotations

import logging
import sys
import types
from dataclasses import dataclass, field

import pytest


def _install_music_assistant_stub() -> None:
    if "music_assistant.models.music_provider" in sys.modules:
        return

    class _StubMusicProvider:
        """Stand-in for music_assistant.models.music_provider.MusicProvider."""

        def __init__(self, mass, manifest, config, supported_features=None):
            self.mass = mass
            self.manifest = manifest
            self.config = config
            self._supported_features = supported_features or set()
            self.cache = getattr(mass, "cache", None)
            self.logger = logging.getLogger(f"test.{manifest.domain}")
            self.unloading = False

        @property
        def supported_features(self):
            return self._supported_features

        @property
        def domain(self) -> str:
            return self.manifest.domain

        @property
        def instance_id(self) -> str:
            return self.config.instance_id

        @property
        def name(self) -> str:
            return getattr(self.config, "name", None) or self.domain

        async def handle_async_init(self) -> None:
            """Overridden by real providers."""

        async def unload(self, is_removed: bool = False) -> None:
            """Overridden by real providers."""

    mass_pkg = types.ModuleType("music_assistant")
    models_pkg = types.ModuleType("music_assistant.models")
    mp_mod = types.ModuleType("music_assistant.models.music_provider")
    mp_mod.MusicProvider = _StubMusicProvider
    mass_pkg.models = models_pkg
    models_pkg.music_provider = mp_mod

    sys.modules.setdefault("music_assistant", mass_pkg)
    sys.modules.setdefault("music_assistant.models", models_pkg)
    sys.modules["music_assistant.models.music_provider"] = mp_mod


_install_music_assistant_stub()


@dataclass
class FakeManifest:
    domain: str = "ovos_ocp"


@dataclass
class FakeProviderConfig:
    instance_id: str = "ovos_ocp--test"
    name: str = "OVOS OCP (test)"
    values: dict = field(default_factory=dict)

    def get_value(self, key):
        return self.values.get(key)


@dataclass
class FakeMass:
    """Stand-in for music_assistant.mass.MusicAssistant.

    Only exposes what the provider touches: `storage_path`, the directory
    MA gives every provider for persistent on-disk state.
    """
    storage_path: str
    cache: object = None


@pytest.fixture
def fake_mass(tmp_path):
    return FakeMass(storage_path=str(tmp_path / "mass_storage"))


@pytest.fixture
def make_provider(fake_mass):
    """Factory: build an OVOSOCPProvider instance without running handle_async_init's
    messagebus connection (search/store logic is exercised directly)."""
    from ovos_ocp_ma_provider import OVOSOCPProvider, SUPPORTED_FEATURES

    def _make(instance_id: str = "ovos_ocp--test", min_confidence: float = 50, mass=None):
        manifest = FakeManifest()
        config = FakeProviderConfig(instance_id=instance_id)
        provider = OVOSOCPProvider(mass or fake_mass, manifest, config, SUPPORTED_FEATURES)
        # Minimal init: skip the real handle_async_init (it dials the OVOS bus).
        provider._min_confidence = min_confidence
        provider._track_cache = __import__("collections").OrderedDict()
        provider._store_path = provider._resolve_store_path()
        provider._load_store()
        return provider

    return _make
