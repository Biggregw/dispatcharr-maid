document.addEventListener('DOMContentLoaded', () => {
  const modal = document.getElementById('quality-insight-modal');
  const openButton = document.getElementById('quality-insight-open');
  if (!modal || !openButton) {
    return;
  }

  const closeButtons = modal.querySelectorAll('[data-quality-insight-close]');
  const listEl = modal.querySelector('.quality-insight-list');
  const loadingEl = modal.querySelector('.quality-insight-loading');
  const emptyEl = modal.querySelector('.quality-insight-empty');
  const errorEl = modal.querySelector('.quality-insight-error');

  const setVisibility = (element, isVisible) => {
    if (!element) {
      return;
    }
    element.classList.toggle('quality-insight-hidden', !isVisible);
  };

  const formatWindowLabel = (windowHours) => {
    const hours = Number(windowHours);
    if (!Number.isFinite(hours) || hours <= 0) {
      return 'last 7 days';
    }
    if (hours >= 24 && hours % 24 === 0) {
      const days = hours / 24;
      const unit = days === 1 ? 'day' : 'days';
      return `last ${days} ${unit}`;
    }
    const unit = hours === 1 ? 'hour' : 'hours';
    return `last ${hours} ${unit}`;
  };

  const formatSeconds = (value) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return '—';
    }
    return `${Number(value).toFixed(0)}s`;
  };

  const formatRate = (value) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return '—';
    }
    return `${(Number(value) * 100).toFixed(1)}%`;
  };

  const renderInsights = (items) => {
    if (!listEl) {
      return;
    }
    listEl.innerHTML = '';
    items.forEach((item) => {
      const details = document.createElement('details');
      details.className = 'quality-insight-item';

      const summary = document.createElement('summary');
      const name = document.createElement('span');
      const nameText = item.channel_name || 'Unknown channel';
      const idText = item.channel_id ? ` (ID ${item.channel_id})` : '';
      name.textContent = `${nameText}${idText}`;

      const rag = document.createElement('span');
      const ragStatus = item.rag_status || 'green';
      rag.className = `quality-insight-rag rag-${ragStatus}`;
      rag.textContent = ragStatus;

      summary.appendChild(name);
      summary.appendChild(rag);
      details.appendChild(summary);

      const detail = document.createElement('div');
      detail.className = 'quality-insight-detail';

      if (Array.isArray(item.reasons) && item.reasons.length > 0) {
        const reasonLabel = document.createElement('div');
        reasonLabel.textContent = 'Reasons:';
        detail.appendChild(reasonLabel);

        const list = document.createElement('ul');
        list.className = 'quality-insight-reasons';
        item.reasons.forEach((reason) => {
          const li = document.createElement('li');
          li.textContent = reason;
          list.appendChild(li);
        });
        detail.appendChild(list);
      } else {
        const emptyReason = document.createElement('div');
        emptyReason.textContent = 'No recent suggestions for this channel.';
        detail.appendChild(emptyReason);
      }

      const note = document.createElement('div');
      const windowHours = item.window_hours || 12;
      const windowLabel = formatWindowLabel(windowHours);
      note.className = 'quality-insight-note';
      note.textContent = `Observational only. Based on the ${windowLabel}.`;
      detail.appendChild(note);

      if (item.playback_evidence) {
        const evidence = item.playback_evidence;
        const evidenceWrap = document.createElement('div');
        evidenceWrap.className = 'quality-insight-playback';

        const evidenceTitle = document.createElement('div');
        evidenceTitle.className = 'quality-insight-playback-title';
        evidenceTitle.textContent = 'Real playback evidence (access logs)';
        evidenceWrap.appendChild(evidenceTitle);

        const channelSummary = evidence.channel_summary;
        if (channelSummary) {
          const summaryList = document.createElement('ul');
          summaryList.className = 'quality-insight-playback-summary';

          const recentWindow = evidence.metadata?.recent_window_hours;
          const recentLabel = recentWindow ? formatWindowLabel(recentWindow) : 'recent window';
          const earlyThreshold = evidence.metadata?.early_abandonment_threshold_seconds;

          const summaryItems = [
            `Total sessions: ${channelSummary.total_sessions ?? 0}`,
            `Recent sessions (${recentLabel}): ${channelSummary.recent_sessions ?? 0}`,
            `Median session duration: ${formatSeconds(channelSummary.median_session_duration_seconds)}`,
            `Average session duration: ${formatSeconds(channelSummary.average_session_duration_seconds)}`,
            `Early abandonment: ${channelSummary.early_abandonment_count ?? 0} (${formatRate(channelSummary.early_abandonment_rate)})`,
            `Recent early abandonment trend: ${channelSummary.recent_early_abandonment_trend || 'insufficient data'}`,
          ];
          summaryItems.forEach((text) => {
            const li = document.createElement('li');
            li.textContent = text;
            summaryList.appendChild(li);
          });

          if (earlyThreshold) {
            const li = document.createElement('li');
            li.textContent = `Early abandonment threshold: < ${earlyThreshold}s`;
            summaryList.appendChild(li);
          }

          evidenceWrap.appendChild(summaryList);
        } else {
          const emptyEvidence = document.createElement('div');
          emptyEvidence.className = 'quality-insight-playback-empty';
          emptyEvidence.textContent = 'No playback sessions observed in this window.';
          evidenceWrap.appendChild(emptyEvidence);
        }

        if (Array.isArray(evidence.stream_summaries) && evidence.stream_summaries.length > 0) {
          const streamTitle = document.createElement('div');
          streamTitle.className = 'quality-insight-playback-streams-title';
          streamTitle.textContent = 'Top streams by playback sessions';
          evidenceWrap.appendChild(streamTitle);

          const streamList = document.createElement('ul');
          streamList.className = 'quality-insight-playback-streams';

          evidence.stream_summaries.forEach((stream) => {
            const li = document.createElement('li');
            li.textContent = `Stream ${stream.stream_id}: ${stream.total_sessions} sessions, ` +
              `${formatSeconds(stream.median_session_duration_seconds)} median, ` +
              `${formatRate(stream.early_abandonment_rate)} early abandonment, ` +
              `recent trend ${stream.recent_early_abandonment_trend || 'insufficient data'}`;
            streamList.appendChild(li);
          });

          evidenceWrap.appendChild(streamList);
        }

        if (evidence.correlation_note) {
          const correlation = document.createElement('div');
          correlation.className = 'quality-insight-playback-correlation';
          correlation.textContent = evidence.correlation_note;
          evidenceWrap.appendChild(correlation);
        }

        detail.appendChild(evidenceWrap);
      }

      if (item.channel_id) {
        const actions = document.createElement('div');
        actions.className = 'quality-insight-actions';

        const acknowledgeButton = document.createElement('button');
        acknowledgeButton.type = 'button';
        acknowledgeButton.className = 'quality-insight-review';
        acknowledgeButton.textContent = 'Mark as reviewed';
        acknowledgeButton.addEventListener('click', async () => {
          acknowledgeButton.disabled = true;
          try {
            const response = await fetch('/api/quality-insights/acknowledge', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
              },
              body: JSON.stringify({
                channel_id: item.channel_id,
              }),
            });
            if (!response.ok) {
              throw new Error(`Request failed with status ${response.status}`);
            }
            await response.json();
            await loadInsights();
          } catch (error) {
            // eslint-disable-next-line no-console
            console.error(error);
          } finally {
            acknowledgeButton.disabled = false;
          }
        });

        actions.appendChild(acknowledgeButton);
        detail.appendChild(actions);
      }

      details.appendChild(detail);
      listEl.appendChild(details);
    });
  };

  const loadInsights = async () => {
    setVisibility(loadingEl, true);
    setVisibility(emptyEl, false);
    setVisibility(errorEl, false);
    if (listEl) {
      listEl.innerHTML = '';
    }

    try {
      const response = await fetch('/api/quality-insights');
      if (!response.ok) {
        throw new Error(`Request failed with status ${response.status}`);
      }
      const data = await response.json();
      setVisibility(loadingEl, false);
      if (!Array.isArray(data) || data.length === 0) {
        setVisibility(emptyEl, true);
        return;
      }
      renderInsights(data);
    } catch (error) {
      setVisibility(loadingEl, false);
      setVisibility(errorEl, true);
      if (errorEl) {
        errorEl.textContent = 'Unable to load quality insight data right now.';
      }
      // eslint-disable-next-line no-console
      console.error(error);
    }
  };

  const openModal = () => {
    modal.classList.add('open');
    modal.setAttribute('aria-hidden', 'false');
    loadInsights();
  };

  const closeModal = () => {
    modal.classList.remove('open');
    modal.setAttribute('aria-hidden', 'true');
  };

  openButton.addEventListener('click', openModal);
  closeButtons.forEach((button) => button.addEventListener('click', closeModal));

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && modal.classList.contains('open')) {
      closeModal();
    }
  });
});
