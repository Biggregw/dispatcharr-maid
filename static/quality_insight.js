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
      note.className = 'quality-insight-note';
      note.textContent = `Observational only. Based on the last ${windowHours} hours.`;
      detail.appendChild(note);

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
