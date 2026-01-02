# Dispatcharr-Maid

Dispatcharr-Maid is a companion tool for Dispatcharr that enriches, analyses, and reorders channel streams to improve real-world playback reliability, particularly for IPTV and proxy-based clients.

## Key Capabilities

- Enriches channels by discovering matching streams across multiple providers
- Scores streams independently based on quality, stability, and compatibility
- Applies resilience-aware ordering to ensure meaningful failover
- Limits per-provider dominance without assuming provider reliability
- Supports device-aware filtering and low-bitrate fallback promotion
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

5. **Resilience-Aware Ordering**
   Streams are ordered using tiers designed to improve real-world failover:
   - Tier 1: One best primary stream per provider
   - Tier 2: Meaningfully different variants (codec, bitrate, or resolution changes)
   - Tier 3: Low-bitrate fallback streams
   - Remaining streams are kept as overflow

6. **Limits, Not Assumptions**
   Provider limits cap how many streams are retained, but do not define ordering.
   Providers are not treated as failure domains.

## Stream Depth and Provider Limits

The “Streams per Provider” setting limits how many streams are kept from each provider.
It does not imply round-robin ordering.

Ordering is always determined after scoring and resilience grouping.

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
