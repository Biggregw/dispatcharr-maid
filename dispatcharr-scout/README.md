# Dispatcharr-Maid

Dispatcharr-Maid is a companion tool for Dispatcharr that enriches, analyses, and reorders channel streams to improve real-world playback reliability, particularly for IPTV and proxy-based clients.

## Key Capabilities

- Enriches channels by discovering matching streams across multiple providers
- Scores streams independently based on quality, stability, and compatibility
- Orders streams deterministically using a continuous score
- Keeps every discovered stream in the final ordered list
- Jobs can be saved, re-run, and managed via the Run Jobs screen

## How It Works

1. **Primary Match**
   Select channels using a simple, readable match string.

2. **Optional Refinement**
   Narrow results using include/exclude rules or Advanced Regex when required.

3. **Stream Enrichment**
   Matching streams are gathered across configured providers.

4. **Scoring**
   Each stream is evaluated independently based on resolution, bitrate, codec, and optional analysis.

5. **Deterministic Ordering**
   Streams are ordered using a single continuous score derived from available metadata
   (resolution, bitrate, FPS, codecs, and validation confidence).
   Provider identity can influence position but never inclusion.

## Matching and Advanced Regex

- **Primary Match** is the default and recommended approach
- Include/exclude rules refine results
- **Advanced Matching (Regex)** bypasses simple matching and should only be used by experienced users
- Regex-only mode disables Primary Match entirely

## Jobs and Run Jobs

Jobs define what to run and how to run it.
Saved Jobs can be viewed, executed, and deleted from the Run Jobs screen.

## Notes on Analysis

Stream analysis may include optional ffmpeg probing when enabled.
Analysis is used for scoring, not playback.
Proxy-based clients (e.g. Firestick) handle decoding client-side.
