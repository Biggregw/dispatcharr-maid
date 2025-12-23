# 📊 Detailed Results Dashboard - User Guide

## What's New?

After running an analysis, you now have access to a **comprehensive results dashboard** with:

✅ **Interactive Charts** - Visual representation of your data  
✅ **Provider Breakdown** - See which providers are performing well  
✅ **Channel Analysis** - Identify problematic channels  
✅ **Error Analysis** - Understand why streams are failing  
✅ **Quality Distribution** - See the quality of your streams  
✅ **Export Options** - Download data as CSV  

---

## 🚀 How to Access

### From Main Control Panel:
Click the **"📊 View Detailed Results"** button at the top

### After Job Completion:
Click the **"📊 View Detailed Results & Charts"** button

### Direct URL:
```
http://YOUR-SERVER-IP:5000/results
```

---

## 📋 What You'll See

### 1. **Overall Summary**

```
┌─────────────────────────────────────┐
│ Total Streams Analyzed:    5,432   │
│ Successful:               5,187    │
│ Failed:                     245    │
│ Success Rate:              95.5%   │
└─────────────────────────────────────┘
```

**Plus Smart Alerts:**
- ⚠️ Low success rate warnings
- ❌ Problem provider alerts
- 💡 Recommendations

---

### 2. **Visual Charts**

#### Quality Distribution (Doughnut Chart)
Shows how many streams are:
- **Excellent** (90-100): Green
- **Good** (70-89): Blue
- **Fair** (50-69): Orange
- **Poor** (<50): Red

**Use case:** Quick overview of overall quality

---

#### Provider Performance (Bar Chart)
Shows success rate for each provider:
- Top 10 providers
- Color-coded by performance
- Sorted worst to best

**Use case:** Identify which providers to keep/remove

---

#### Resolution Distribution (Pie Chart)
Shows breakdown of stream resolutions:
- 4K (3840x2160)
- 1080p (1920x1080)
- 720p (1280x720)
- 540p or lower

**Use case:** See if you're getting the quality you pay for

---

#### Error Types (Bar Chart)
Shows most common errors:
- Timeout
- Connection refused
- No video codec
- Bitrate too low

**Use case:** Understand why streams are failing

---

### 3. **Detailed Tables**

#### Providers Tab

| Provider | Total | Success | Failed | Success Rate | Avg Quality |
|----------|-------|---------|--------|--------------|-------------|
| Provider 1 | 1,234 | 1,224 | 10 | 99.2% ⭐ | 94.2 |
| Provider 2 | 982 | 934 | 48 | 95.1% ✓ | 89.7 |
| Provider 3 | 678 | 456 | 222 | 67.3% ⚠️ | 67.1 |
| Provider 4 | 445 | 201 | 244 | 45.2% ❌ | 52.3 |

**Features:**
- Sorted by performance (worst first)
- Color-coded badges
- Average quality score

**Actions you can take:**
- Identify providers to remove (low success rate)
- See which providers have best quality
- Spot patterns in failures

---

#### Channels Tab

| Channel | Total | Success | Failed | Success Rate |
|---------|-------|---------|--------|--------------|
| #001 BBC One | 12 | 12 | 0 | 100% ⭐ |
| #002 BBC Two | 12 | 11 | 1 | 91.7% ✓ |
| #045 Sky F1 | 15 | 7 | 8 | 46.7% ❌ |

**Features:**
- Shows all analyzed channels
- Identify channels with no working streams
- See which channels have multiple providers

**Actions you can take:**
- Find channels that need more providers
- Identify channels to remove
- See which channels are most reliable

---

#### Errors Tab

Lists all error types with counts:

```
❌ Timeout
   128 occurrences

❌ Connection refused  
   67 occurrences

❌ No video codec detected
   34 occurrences

❌ Bitrate below minimum (1500kbps)
   16 occurrences
```

**Features:**
- Sorted by frequency
- Shows all unique error types
- Helps identify systemic issues

**Actions you can take:**
- If many timeouts → Increase timeout in config
- If connection refused → Provider might be blocking you
- If no codec → Streams might be dead links

---

## 📥 Export Options

### Export CSV
**Button:** "📥 Export CSV"

Downloads complete analysis results including:
- Channel numbers and names
- Provider IDs
- Stream URLs
- Quality scores
- Resolutions
- Bitrates
- Error messages
- Timestamps

**Use for:**
- Detailed offline analysis
- Sharing with others
- Import into Excel/Google Sheets
- Creating your own reports

---

### Print Report
**Button:** "🖨️ Print Report"

Generates print-friendly version:
- Removes navigation
- Optimizes for paper
- Includes all charts
- Includes all tables

**Use for:**
- PDF generation (print to PDF)
- Physical reports
- Documentation

---

## 💡 How to Use This Data

### Scenario 1: Weekly Maintenance

**Goal:** Keep only good providers

**Steps:**
1. View Results Dashboard
2. Go to "Providers" tab
3. Sort by Success Rate (already sorted)
4. Look for providers <70% success
5. Note Provider IDs
6. Remove them from Dispatcharr

**Result:** Better overall quality!

---

### Scenario 2: Troubleshooting Failures

**Goal:** Understand why streams are failing

**Steps:**
1. View Results Dashboard
2. Check Overall Summary alerts
3. Go to "Errors" tab
4. Identify most common error
5. Take action based on error type:
   - **Timeout** → Increase timeout in config
   - **Connection refused** → Check provider status
   - **No codec** → Streams might be dead
   - **Low bitrate** → Lower quality threshold

**Result:** Fewer failures next time!

---

### Scenario 3: Quality Optimization

**Goal:** Get best quality streams

**Steps:**
1. View Results Dashboard
2. Check Quality Distribution chart
3. If too many "Fair" or "Poor":
   - Go to Providers tab
   - Find providers with low avg quality
   - Consider replacing them
4. Check Resolution Distribution
5. If too many 720p or lower:
   - Look for 1080p providers
   - Update your provider list

**Result:** Higher quality streams!

---

### Scenario 4: Channel Audit

**Goal:** Ensure all channels have backups

**Steps:**
1. View Results Dashboard
2. Go to "Channels" tab
3. Look for channels with low total counts (<5)
4. These channels have few providers
5. Add more streams for these channels

**Result:** Better redundancy!

---

## 🎯 Understanding the Metrics

### Success Rate
**What it means:** Percentage of streams that passed analysis

**Good:** >95%
**OK:** 85-95%
**Bad:** <85%

**Action if bad:**
- Check provider status
- Increase timeout
- Review error types

---

### Average Quality Score
**What it means:** Composite score based on:
- Bitrate (higher = better)
- Resolution (1080p > 720p)
- FPS (50fps > 25fps)
- Codec (HEVC > H264)
- Dropped frames (fewer = better)

**Good:** >85
**OK:** 70-85
**Bad:** <70

**Action if bad:**
- Replace low-quality providers
- Check if provider is throttling
- Verify stream URLs are correct

---

### Error Types

**Timeout:**
- Stream took too long to respond
- Might be slow provider or dead link
- Action: Increase timeout or remove provider

**Connection Refused:**
- Provider is blocking you
- Might be rate limiting
- Action: Spread out requests or change provider

**No Video Codec:**
- Stream has no video track
- Likely a dead link
- Action: Remove stream

**Bitrate Too Low:**
- Stream quality below threshold
- Action: Lower threshold or replace stream

---

## 📊 Chart Interpretation

### Quality Distribution

**Ideal:** Most streams in "Excellent" (green)
**Concerning:** Many in "Fair" or "Poor"

**If too many poor streams:**
1. Check Provider Performance chart
2. Identify low-performing providers
3. Remove or replace them

---

### Provider Performance

**Look for:**
- Providers below 70% (red bars)
- Consistent performers (green bars)
- Outliers (one provider much worse)

**Action:**
- Keep providers >90%
- Review providers 70-90%
- Remove providers <70%

---

### Resolution Distribution

**Ideal:** Most in 1080p
**Concerning:** Many in 540p or lower

**Action:**
- Find channels with low resolution
- Look for 1080p alternatives
- Consider upgrading providers

---

### Error Types

**Patterns to watch:**
- One error dominates = Systemic issue
- Many different errors = Multiple problems
- Errors from one provider = Provider issue

**Action:**
- Address most common error first
- Use Errors tab to see details
- Cross-reference with Provider table

---

## 🔄 Workflow Recommendations

### Daily Quick Check
1. Open Results Dashboard
2. Check Overall Summary
3. Note any alerts
4. Done! (30 seconds)

### Weekly Deep Dive
1. Open Results Dashboard
2. Review all 4 charts
3. Check Providers tab for problems
4. Review Errors tab
5. Export CSV for records
6. Take actions on bad providers
7. (10 minutes)

### Monthly Audit
1. Run full analysis
2. Open Results Dashboard
3. Review each tab thoroughly
4. Export CSV and print report
5. Compare to last month
6. Document trends
7. Make provider changes
8. (30 minutes)

---

## 💾 Data Retention

**Where results are stored:**
```
jobs/<job_id>/csv/03_iptv_stream_measurements.csv
```

**Viewing past runs:**
- Use the **Run** dropdown on the Results page to switch between completed jobs
- Or open directly with: `/results?job_id=<job_id>`
- CSV export is **job-scoped** when a job is selected (the download filename includes the job id)

**Retention policy (optional):**
- By default, the server keeps the **last 50 jobs** in `logs/job_history.json`
- You can enable automatic pruning of old job folders + history entries by setting either:
  - `web.results_retention_days` in `config.yaml`, or
  - `DISPATCHARR_MAID_RESULTS_RETENTION_DAYS` in the environment (takes precedence)

**Backup your results:**
```bash
# Replace <job_id> with the run you want to keep
cp jobs/<job_id>/csv/03_iptv_stream_measurements.csv \
   csv/backup_<job_id>_$(date +%Y%m%d).csv
```

---

## 🐛 Troubleshooting

### "No results available"
**Cause:** Haven't run an analysis yet
**Fix:** Go to Control Panel → Run any analysis job

### Charts not loading
**Cause:** JavaScript error or network issue
**Fix:** Refresh page (Ctrl+F5)

### Missing data in tables
**Cause:** CSV file corrupted or incomplete
**Fix:** Re-run analysis

### Export CSV not working
**Cause:** File permissions or missing file
**Fix:** Check `csv/` folder exists and has data

---

## 🎓 Pro Tips

1. **Run analysis weekly** to track trends
2. **Export CSV after each run** to build history
3. **Focus on providers <70%** - easy wins
4. **Check Errors tab first** when troubleshooting
5. **Use Quality chart** for quick health check
6. **Print reports** for offline review
7. **Compare before/after** when making changes

---

## 🚀 Next Steps

After viewing results:

1. **Identify actions** from the data
2. **Make changes** in Dispatcharr:
   - Remove bad providers
   - Add backup streams
   - Reorder based on quality
3. **Re-run analysis** to verify improvements
4. **Compare results** to see if changes helped

---

## 📈 Example Use Case

**Situation:** Overall success rate is 87% (below target of 95%)

**Investigation:**
1. Open Results Dashboard
2. See alert: "3 providers below 70%"
3. Go to Providers tab
4. Find Provider 45: 32% success rate
5. Go to Errors tab
6. See: "Connection refused" is #1 error
7. Cross-check: Most "Connection refused" from Provider 45

**Action:**
1. Remove Provider 45 from Dispatcharr
2. Re-run analysis
3. Success rate now 95.2%! ✅

**Time:** 5 minutes to identify and fix!

---

**Enjoy your detailed insights!** 📊

This dashboard gives you everything you need to maintain high-quality IPTV streams with minimal effort.
