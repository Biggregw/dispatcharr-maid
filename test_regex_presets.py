import copy
import dispatcharr_web_app as app


def _preset_data(**overrides):
    data = {
        'name': 'Auto Name',
        'regex': 'bbc',
        'regex_mode': 'filter',
        'groups': [1],
        'channels': [10],
        'streams_per_provider': 2,
        'base_search_text': 'BBC One',
        'include_filter': 'inc',
        'exclude_filter': 'exc',
        'exclude_plus_one': False,
        'analysis_profile': 'balanced',
    }
    data.update(overrides)
    return data


def test_same_identity_replaces_and_updates(monkeypatch):
    presets = []
    initial = app._build_regex_preset_payload(_preset_data(name='Custom Name', include_filter='north'))
    presets.append(copy.deepcopy(initial))

    updated = app._build_regex_preset_payload(
        _preset_data(
            name='Auto Name',
            include_filter='north',
            streams_per_provider=3,
        )
    )

    replaced = app._replace_or_insert_preset(presets, updated, preserve_existing_name=True)

    assert replaced is True
    assert len(presets) == 1
    assert presets[0]['name'] == 'Custom Name'
    assert presets[0]['include_filter'] == 'north'
    assert presets[0]['streams_per_provider'] == 3
    assert presets[0]['identity'] == initial['identity']


def test_different_identity_creates_new_entry():
    presets = [app._build_regex_preset_payload(_preset_data(name='First', groups=[1], analysis_profile='balanced'))]
    new = app._build_regex_preset_payload(_preset_data(name='Second', groups=[1], analysis_profile='deep'))

    replaced = app._replace_or_insert_preset(presets, new, preserve_existing_name=True)

    assert replaced is False
    assert len(presets) == 2
    assert {p['name'] for p in presets} == {'First', 'Second'}


def test_renamed_job_retains_name_on_replacement():
    presets = [app._build_regex_preset_payload(_preset_data(name='Renamed Job'))]
    follow_up = app._build_regex_preset_payload(_preset_data(name='Auto Name Again'))

    app._replace_or_insert_preset(presets, follow_up, preserve_existing_name=True)

    assert presets[0]['name'] == 'Renamed Job'
    assert presets[0]['regex'] == follow_up['regex']


def test_identity_changes_when_filters_change():
    presets = [app._build_regex_preset_payload(_preset_data(name='First', include_filter='north'))]
    changed = app._build_regex_preset_payload(_preset_data(name='Second', include_filter='south'))

    replaced = app._replace_or_insert_preset(presets, changed, preserve_existing_name=True)

    assert replaced is False
    assert len(presets) == 2


def test_maybe_auto_save_replaces_by_identity(monkeypatch):
    saved = []

    def fake_load():
        return saved

    def fake_save(presets):
        saved.clear()
        saved.extend(presets)

    monkeypatch.setattr(app, '_load_stream_name_regex_presets', fake_load)
    monkeypatch.setattr(app, '_save_stream_name_regex_presets', fake_save)

    req = {
        'groups': [1],
        'channels': [10],
        'base_search_text': 'BBC One',
        'stream_name_regex_override': 'bbc.*',
        'include_filter': 'inc',
        'exclude_filter': 'exc',
        'exclude_plus_one': False,
        'analysis_profile': 'fast',
    }

    assert app._maybe_auto_save_job_request(dict(req), preview_only=False) is True
    assert len(saved) == 1

    # Same identity should update in place, not duplicate.
    req['include_filter'] = 'inc-updated'
    assert app._maybe_auto_save_job_request(dict(req), preview_only=False) is True
    assert len(saved) == 1
    assert saved[0]['include_filter'] == 'inc-updated'


def test_preview_auto_save_does_not_run(monkeypatch):
    calls = {'loaded': 0, 'saved': 0}

    def fake_load():
        calls['loaded'] += 1
        return []

    def fake_save(presets):
        calls['saved'] += 1

    monkeypatch.setattr(app, '_load_stream_name_regex_presets', fake_load)
    monkeypatch.setattr(app, '_save_stream_name_regex_presets', fake_save)

    saved = app._maybe_auto_save_job_request(
        {
            'groups': [1],
            'channels': [10],
            'base_search_text': 'BBC One',
            'stream_name_regex': 'bbc',
            'streams_per_provider': 2,
        },
        preview_only=True,
    )

    assert saved is False
    assert calls['loaded'] == 0
    assert calls['saved'] == 0
