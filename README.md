# Dispatcharr-Maid

A lightweight, self-hosted housekeeping and analysis UI for Dispatcharr.

## Legal disclaimer

Dispatcharr-Maid is a general-purpose management and analysis tool.

It does not provide IPTV content, streams, playlists, or access to media of any kind.
It does not endorse, promote, or encourage the use of illegal or unauthorised streams.

Users are solely responsible for ensuring that any content or services they connect to
are used in compliance with applicable laws and licensing requirements in their country.

The authors assume no liability for misuse of this software.


## Stream selection, enrichment, and ranking

Dispatcharr-Maid works from the existing channel configuration in Dispatcharr.

For a given channel, it reads the current list of provider streams already associated with that channel and applies logic derived from the original channel name rather than relying on provider-specific naming alone.

Dispatcharr-Maid then searches across all configured providers in Dispatcharr to identify additional matching streams for the same channel. This enrichment step uses only streams that already exist within Dispatcharr and does not source content externally.

During stream selection and enrichment, rules are applied such as:
- selecting regional variants
- filtering by device compatibility, for example Fire TV or Firestick

Before any changes are applied, a manual review step allows individual streams to be included or excluded using checkboxes.

Once streams are added back into the channel, Dispatcharr-Maid performs a full ffmpeg probe and analyses streams based on factors such as quality, resolution, and reliability. Streams are then ranked and reordered within the channel.

Dispatcharr-Maid also supports configurable stream depth per provider:
- selecting **1** retains only the highest-ranked stream from each provider
- selecting **2** retains the top two streams from each provider, and so on

Streams are ordered so the best stream from each provider appears first, followed by the second-best stream from each provider. This ordering aligns with Dispatcharr’s playback behaviour, allowing it to attempt the highest-quality stream from each provider before falling back to lower-ranked alternatives.

Installation
------------
Dispatcharr-Maid is designed to run as a self-hosted Docker service.

Refer to DOCKER_GUIDE.md for installation and startup instructions.
