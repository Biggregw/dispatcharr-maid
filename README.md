# Dispatcharr-Maid 🧹

A lightweight, self-hosted web interface for Dispatcharr stream management and optimization.

Transform your IPTV setup with intelligent stream enrichment, quality analysis, and automatic ranking—all through a clean web UI that requires no command-line experience.

---

## ⚖️ Legal Disclaimer

**Dispatcharr-Maid is a general-purpose management and analysis tool.**

- It does **not** provide IPTV content, streams, playlists, or access to media of any kind
- It does **not** endorse, promote, or encourage the use of illegal or unauthorized streams

**Users are solely responsible** for ensuring that any content or services they connect to are used in compliance with applicable laws and licensing requirements in their country.

The authors assume no liability for misuse of this software.

---

## ✨ Features

- 🌐 **Web-based interface** - No SSH required, manage everything from your browser
- 🔌 **Seamless Dispatcharr integration** - Connects to your existing setup on the same network
- 🔍 **Smart stream enrichment** - Automatically finds matching streams across all your providers
- 📊 **Quality analysis** - Full ffmpeg probe scoring based on reliability and technical quality
- 🎯 **Intelligent ranking** - Optimal stream ordering aligned with Dispatcharr's failover behavior
- ⚙️ **Configurable stream depth** - Control how many streams per provider to keep (1, 2, 3, etc.)
- 📱 **Device compatibility filters** - Optimize for FireStick/Fire TV (excludes 4K streams that cause buffering)
- 🌍 **Regional variant selection** - Include/exclude specific regional variants with wildcard filters
- ✅ **Manual review step** - Checkbox selection before applying any changes
- 💾 **Data persistence** - CSV files, logs, and configuration automatically preserved
- 🔄 **Auto-restart** - Survives server reboots with Docker's built-in restart policies
- 🐳 **Docker-based** - Clean, isolated environment with easy deployment

---

## 🎯 How It Works

**Five simple steps to optimized streams:**

1. **📂 Select Groups** → Choose which channel groups to work with
2. **📺 Select Channels** → Pick specific channels (e.g., BBC One, ESPN)
3. **🔍 Search & Filter** → Find matching streams across all providers with customizable filters
4. **✅ Review & Add** → Preview results and select streams to add via checkboxes
5. **⚡ Analyze & Optimize** → Full ffmpeg quality probe, scoring, and ranking

**The result:** Your channels contain the optimal streams in the optimal order for Dispatcharr's failover behavior.

---

## 📋 Prerequisites

Before you begin, make sure you have:

- ✅ **Docker and docker-compose** installed on your server
- ✅ **Dispatcharr** running on the same Docker network
- ✅ **Dispatcharr credentials** (username and password)

**Important (Docker networking):** This project’s `docker-compose.yml` joins an **existing external network** called `dispatcharr_default`. That network must already exist (typically created automatically by the Dispatcharr docker-compose project). If your Dispatcharr network has a different name, update `docker-compose.yml` accordingly (see [Docker Deployment Guide](DOCKER_GUIDE.md)).

---

## 🚀 Quick Start

Get up and running in minutes:

```bash
# 1. Download the latest release
wget https://github.com/Biggregw/dispatcharr-maid/archive/refs/heads/main.zip
unzip main.zip && cd dispatcharr-maid-main

# 2. Create your local config files (these stay private)
cp .env.example .env
cp config.yaml.example config.yaml

# Optional (nice provider names in Results/UI)
cp provider_names.json.example provider_names.json

# 3. Configure your credentials + settings
nano .env         # Set DISPATCHARR_BASE_URL / USER / PASS
nano config.yaml  # Set filters.channel_group_ids (or use the Web UI to save regex selection)

# 4. Start the container
docker compose up -d

# 5. Access the web interface
# Open http://YOUR-SERVER-IP:5000 in your browser
```

**Note:** Use Docker Compose v2 (`docker compose`). The legacy Compose v1 (`docker-compose` python package) can fail on newer Docker engines with errors like `KeyError: 'ContainerConfig'`.

**🎉 That's it!** You should see the Dispatcharr Maid dashboard.

📖 **Need detailed setup instructions?** See the **[Docker Deployment Guide](DOCKER_GUIDE.md)** for comprehensive installation, configuration, and troubleshooting.

---

## 📸 Screenshots

> **Coming soon:** Web interface screenshots showing the dashboard, stream selection, and results views.

<!-- Placeholder for future screenshots
![Dashboard](docs/screenshots/dashboard.png)
*Clean, intuitive web interface - no command line needed*

![Stream Selection](docs/screenshots/stream-selection.png)
*Visual stream selection with checkbox controls and real-time filtering*
-->

---

## 🔧 Stream Selection, Enrichment, and Ranking

### How Dispatcharr-Maid Works

**Dispatcharr-Maid works from your existing Dispatcharr channel configuration.**

For a given channel, it reads the current list of provider streams already associated with that channel and applies logic derived from the original channel name rather than relying on provider-specific naming alone.

Dispatcharr-Maid then **searches across all configured providers** in Dispatcharr to identify additional matching streams for the same channel. This enrichment step uses only streams that already exist within Dispatcharr and does not source content externally.

### Filtering and Selection

During stream selection and enrichment, you can apply rules such as:

- **Regional variants** - Include or exclude specific regions (e.g., "BBC One Yorkshire")
- **Device compatibility** - Filter for FireStick/Fire TV (excludes 4K streams that may buffer)
- **Custom filters** - Include/exclude patterns using wildcards (e.g., `york*`, `linc*`)

Before any changes are applied, a **manual review step** allows individual streams to be included or excluded using checkboxes—you have complete control.

### Quality Analysis and Ranking

Once streams are added back into the channel, Dispatcharr-Maid performs a **full ffmpeg probe** and analyzes streams based on factors such as:

- Technical quality (bitrate, codec, resolution)
- Stream reliability and responsiveness
- Connection speed and stability

Streams are then **ranked and reordered** within the channel.

### Configurable Stream Depth

Dispatcharr-Maid supports **configurable stream depth per provider**:

- Selecting **1** retains only the highest-ranked stream from each provider
- Selecting **2** retains the top two streams from each provider
- And so on...

### Optimal Stream Ordering

Streams are ordered so the **best stream from each provider appears first**, followed by the second-best stream from each provider, and so on.

**Example with 3 providers and depth = 2:**

```
Provider A #1, Provider B #1, Provider C #1, Provider A #2, Provider B #2, Provider C #2
```

This ordering **aligns perfectly with Dispatcharr's playback behavior**, allowing it to:

1. Attempt the highest-quality stream from each provider first
2. Fall back to the next provider if a stream fails
3. Try second-tier streams only after all top streams are exhausted

The result: **Maximum reliability with optimal quality.**

### Provider Discovery & Capacity Visibility

Dispatcharr-Maid can auto-discover provider IDs, names, and capacity metadata
directly from Dispatcharr (via `manage.py` or an authenticated API call). This
means you no longer need to maintain provider_map.json manually—manual overrides
in provider_names.json are still supported and take precedence.

Because most providers enforce strict connection limits, **provider diversity**
is critical: spreading channels across multiple providers reduces the risk of
hitting per-provider max_streams limits. The existing round-robin ordering
above is intentionally preserved to keep failover behavior consistent while
maximizing diversity.

### Provider Usage (Viewing Activity) via Access Logs (Optional)

If your IPTV client uses the **Xtream/M3U connection served by Dispatcharr**, you can
estimate real-world provider usage by parsing your reverse-proxy access logs
(for example, **Nginx Proxy Manager** logs containing playback requests like
`/live/<user>/<pass>/<stream_id>.ts`).

Dispatcharr-Maid exposes an API endpoint:

- `GET /api/usage/providers?days=7&proxy_host=1`

Configure `config.yaml` under `usage:` and mount your proxy logs into the Maid
container (see `config.yaml.example`).

---

## 📚 Documentation

Complete guides for every aspect of Dispatcharr Maid:

| Guide | Description |
|-------|-------------|
| **[Docker Deployment Guide](DOCKER_GUIDE.md)** | Complete installation, configuration, networking, and advanced setup |
| **[Web App Guide](WEB_APP_GUIDE.md)** | Step-by-step instructions for using the web interface |
| **[Web Monitor Guide](WEB_MONITOR_GUIDE.md)** | Legacy/optional read-only monitor (most users should use the main web UI) |
| **[Results Dashboard Guide](RESULTS_DASHBOARD_GUIDE.md)** | Understanding and analyzing your results |

---

## 🔍 Troubleshooting

Running into issues? Here are some quick tips:

**Container won't start:**
```bash
docker-compose logs dispatcharr-maid-web
```

**Can't connect to Dispatcharr:**
```bash
# Make sure you're using the container name, not localhost
# In .env: DISPATCHARR_BASE_URL=http://dispatcharr:9191
docker exec dispatcharr-maid-web ping dispatcharr
```

**Web interface not accessible:**
```bash
# Check if the web container is running
docker-compose ps dispatcharr-maid-web

# Check if port 5000 is available
sudo netstat -tlnp | grep 5000
```

📖 **For detailed troubleshooting**, see the **[Docker Guide troubleshooting section](DOCKER_GUIDE.md#-troubleshooting)**.

**Still stuck?** Open an issue on GitHub with:
- Your setup details (OS, Docker version, Dispatcharr version)
- Relevant log output
- Steps to reproduce the issue

---

## 🤝 Contributing

Contributions are welcome! Whether you're:

- 🐛 Reporting bugs
- 💡 Suggesting features
- 📝 Improving documentation
- 🔧 Submitting code improvements

Please feel free to open an issue or submit a pull request.

### How to Contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📄 License

This project is available for personal, non-commercial use. Please review the repository license file for full details.

---

## 🙏 Acknowledgments

Built to complement the excellent work of the **[Dispatcharr](https://github.com/Dispatcharr/Dispatcharr)** project.

Special thanks to the open-source community for the tools and libraries that make this possible:
- Flask for the web framework
- FFmpeg for stream analysis
- Docker for containerization
- And many more...

---

## ☕ Buy Me a Coffee

If Dispatcharr-Maid saves you time or helps keep things running smoothly, you can support development with a coffee.

https://buymeacoffee.com/biggregw

---

## 🌟 Star This Project

If you find Dispatcharr-Maid useful, please consider giving it a ⭐ on GitHub! It helps others discover the project.

---

## 🐼 Made with ❤️ for the Dispatcharr community 🐼

