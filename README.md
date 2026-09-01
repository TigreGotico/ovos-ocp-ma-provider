# ovos-ocp-ma-provider

A [Music Assistant](https://github.com/music-assistant/server) music provider that turns OpenVoiceOS OCP skills into a searchable source. It connects to the OVOS messagebus and broadcasts each search query to every installed OCP skill through the OCP search protocol (`ovos.common_play.query`), then turns the responses into Music Assistant tracks. Playback streams straight from the URL an OCP skill returned, over HTTP.

## Install

Install it as a Music Assistant provider plugin with [music-assistant-plugin-manager](https://github.com/TigreGotico/music-assistant-plugin-manager), not with a bare `pip install`:

```bash
music-assistant-community install ovos-ocp-ma-provider
```

The plugin manager installs the package into the Music Assistant server's own environment and registers it under the `music_assistant.provider` entry point, which a manual `pip install` into an unrelated environment will not do.

This package needs Python 3.11 or later, and a Music Assistant instance with a reachable OVOS messagebus.

## Configuration

| Setting | Default | Description |
| --- | --- | --- |
| OVOS messagebus host | `localhost` | Hostname or IP of the OVOS messagebus. Must be reachable from Music Assistant. |
| OVOS messagebus port | `8181` | Port of the OVOS messagebus. |
| OCP search timeout (seconds) | `10` | How long the provider waits for OCP skills to respond. Skills can extend this through the OCP protocol. |
| Minimum match confidence | `50` | OCP results below this score are discarded. Uses OCP's own 0-100 `match_confidence` scale. |

## How playback resolution works

A search broadcasts the query to every OCP skill and collects the results, dropping duplicates and anything below the configured confidence. Each result becomes a track whose id is a stable hash of the provider instance and the OCP URI.

Every track a search returns is also written to a small JSON file under the provider's Music Assistant storage directory, keyed by that id. When Music Assistant asks for a track or its stream details — whether right after a search or after resolving something it saved earlier in its library or play queue — the provider looks the id up in memory first, then in that file, and rebuilds the track from whichever it finds. This is what lets a track survive a Music Assistant restart instead of failing with "not found on provider" the next time it is queued.

## Limitations

This is a search-only provider: there is no library to browse, and no independent artist, album, playlist, or radio lookup — those are not concepts OCP skills expose on their own. A result stays resolvable for as long as it remains in the on-disk store; very old, never-replayed results are eventually evicted to bound the store's size.

## Related projects

- [music-assistant-plugin-manager](https://github.com/TigreGotico/music-assistant-plugin-manager): installs and manages Music Assistant provider plugins.
- [ovos-ma-player](https://github.com/TigreGotico/ovos-ma-player): the OpenVoiceOS OCP player provider for Music Assistant, the playback counterpart to this search provider.
- [hivemind-ma-player](https://github.com/TigreGotico/hivemind-ma-player): bridges Music Assistant playback into a HiveMind satellite.
- [ovos-media](https://github.com/OpenVoiceOS/ovos-media): the OVOS media/OCP subsystem this provider draws its search results from.
