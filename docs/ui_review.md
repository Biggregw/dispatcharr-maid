# Dispatcharr Maid Web UI Review

## User walkthrough (first-time perspective)
1. **Landing / Control Panel (`/`)**
   - Header and stepper show four phases: Select Groups → Select Channels → Run Jobs → Quality Check.
   - Initial card lists channel groups with counts, checkboxes, and a "Next: Select Channels" button. There is no inline hint about whether groups are required before proceeding.
   - A progress indicator and "phase" hints appear later but are empty on first load.

2. **Select Channels step**
   - Channel search and per-group accordions appear after clicking Next. Selecting specific channels is optional, but the UI defaults to “All channels” without explicitly stating that leaving everything unchecked keeps all channels. Navigation buttons allow back/next.

3. **Run Jobs step**
   - Primary actions: **Refresh Channel Streams**, **Quality Check (Apply Changes)**, and **Quality Check (Preview Plan)**. Helper text describes purposes, but ordering mixes refresh (for discovery) with quality actions (for scoring/cleanup), which may blur sequencing.
   - Saved Jobs dropdown supports reruns and deletion; status messages appear inline. Advanced regex/filters and refresh filters are shown, but the distinction between “Advanced Regex (layered)” vs “regex-only override” requires careful reading.
   - Running a refresh opens a modal with preview list and “Add Selected Streams,” implying refresh is channel-scoped, but the stepper still highlights global phases, not per-channel context.

4. **Quality Check step**
   - Analysis profile selector (Fast/Balanced/Deep) and Streams Per Provider input control limits. Two primary buttons either apply changes immediately or preview a plan. The distinction is clearly labeled, but there is no secondary confirmation before applying changes.

5. **Results page (`/results`)**
   - Top bar provides back-to-control-panel, export CSV, and job picker (defaults to latest completed job). Tabs switch between Streams, Providers, Channels, Errors, and Planned Changes. Helper copy explains plan-only runs and regex generation in the Streams tab.
   - Plan tab allows committing planned changes directly; commit button is disabled until selection, but the flow from preview to commit is not reiterated.

## Friction points / UX footguns
- **Unclear prerequisites on first screen:** The landing view does not state whether at least one group must be selected before proceeding or what happens if none are selected.
- **Channel selection default ambiguity:** Leaving all channels unchecked is treated as “all channels,” but the UI only hints at this deep in helper text; a first-time user may think they must pick channels.
- **Action stacking on Run Jobs:** Refresh and Quality Check actions live side by side with similar styling, so users may run a Quality Check before refreshing streams or without understanding prerequisites.
- **Regex terminology overload:** Terms like “Advanced Regex (layered)” vs “regex-only override,” plus include/exclude filters and presets, can be overwhelming without a simple decision aid.
- **Risky one-click apply:** “Quality Check (Apply Changes)” executes changes immediately without a confirmation step; mis-clicks or mis-configured limits could alter Dispatcharr unintentionally.
- **Saved job lifecycle is opaque:** Jobs are “saved automatically when you run them,” but the dropdown label and status don’t indicate which settings are captured or how overrides interact with current selections.
- **Plan commit path hidden:** On the Results page, the Planned Changes tab allows commits, but nothing on the main flow reminds users they must visit Results after a Preview Plan run.

## Minimal recommendations to improve clarity
- Add a short note on the Groups step explaining the minimum selection rule and what happens if none are picked.
- In the Channels step, surface a clear banner: “No channels selected = all channels in chosen groups.”
- Separate the Run Jobs actions visually or via subtitles to reinforce the intended order: refresh discovery first, then quality checks.
- Add a concise tooltip or mini-decision list for regex options (layered vs override vs include/exclude) to guide users toward the simplest choice.
- Require a lightweight confirmation (e.g., modal) before running “Quality Check (Apply Changes)” when a Preview Plan hasn’t been reviewed.
- Update the Saved Jobs description to state exactly what is saved (matching chain, analysis settings, selection) and whether current selections override the saved pattern.
- After a Preview Plan run, surface a toast or inline link reminding users to open the Results → Planned Changes tab to commit.
