# ovos-ocp-ma-provider

A [Music Assistant](https://github.com/music-assistant/server) music provider for OpenVoiceOS OCP skills. It connects to the OVOS messagebus and sends each search query to all installed OCP skills through the OCP search protocol (`ovos.common_play.query`). It turns the OCP results into Music Assistant tracks. Stream URLs from OCP skills go straight to the Music Assistant player over HTTP.

## Install

```bash
pip install -e .
```

With test dependencies:

```bash
pip install -e .[test]
```

This package needs Python 3.11 or later. It runs inside a Music Assistant instance and needs a reachable OVOS messagebus.

## Configuration

| Setting | Default | Description |
| --- | --- | --- |
| OVOS messagebus host | `localhost` | Hostname or IP of the OVOS messagebus. Must be reachable from Music Assistant. |
| OVOS messagebus port | `8181` | Port of the OVOS messagebus. |
| OCP search timeout (seconds) | `10` | How long the provider waits for OCP skills to respond. Skills can extend this through the OCP protocol. |
| Minimum match confidence | `0.5` | OCP results below this score are discarded. |

## Usage

After the messagebus host and port are set, Music Assistant sends every search through this provider to the OVOS OCP skills. Each OCP skill that has an audio result returns it with a match confidence score. The provider drops results below the configured minimum confidence and removes duplicate URIs. It returns the rest as tracks.

Track playback pulls the stream URL from an in-memory cache built during the search. A track that has not been searched for recently is not in the cache, so search for it again before playing it.

Only search is supported. There is no library browse and no artist or album lookup.

## Related projects

- [music-assistant-plugin-manager](https://github.com/TigreGotico/music-assistant-plugin-manager): installs and manages Music Assistant provider plugins.
- [ovos-ma-player](https://github.com/TigreGotico/ovos-ma-player): the OpenVoiceOS OCP player provider for Music Assistant, the playback counterpart to this search provider.
