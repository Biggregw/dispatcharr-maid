from unittest import mock

from engine.interface import run_engine


def test_run_engine_calls_engine_steps_in_order():
    api = mock.Mock()
    config = mock.Mock()
    callbacks = {"fetch": mock.Mock(), "analyze": mock.Mock()}

    with mock.patch("engine.interface.fetch_streams") as fetch_streams:
        with mock.patch("engine.interface.analyze_streams") as analyze_streams:
            with mock.patch("engine.interface.score_streams") as score_streams:
                with mock.patch("engine.interface.reorder_streams") as reorder_streams:
                    reorder_streams.return_value = "ordered"

                    result = run_engine(
                        api,
                        config,
                        progress_callbacks=callbacks,
                        stream_provider_map_override={1: "provider"},
                        force_full_analysis=True,
                    )

    fetch_streams.assert_called_once_with(
        api,
        config,
        output_file=None,
        progress_callback=callbacks["fetch"],
        stream_provider_map_override={1: "provider"},
    )
    analyze_streams.assert_called_once_with(
        config,
        input_csv=None,
        output_csv=None,
        fails_csv=None,
        progress_callback=callbacks["analyze"],
        force_full_analysis=True,
    )
    score_streams.assert_called_once_with(
        api,
        config,
        input_csv=None,
        output_csv=None,
        update_stats=False,
    )
    reorder_streams.assert_called_once_with(api, config, input_csv=None)
    assert result == "ordered"


def test_run_engine_defaults_match_existing_behaviour():
    api = mock.Mock()
    config = mock.Mock()

    with mock.patch("engine.interface.fetch_streams") as fetch_streams:
        with mock.patch("engine.interface.analyze_streams") as analyze_streams:
            with mock.patch("engine.interface.score_streams") as score_streams:
                with mock.patch("engine.interface.reorder_streams") as reorder_streams:
                    run_engine(api, config)

    fetch_streams.assert_called_once_with(
        api,
        config,
        output_file=None,
        progress_callback=None,
        stream_provider_map_override=None,
    )
    analyze_streams.assert_called_once_with(
        config,
        input_csv=None,
        output_csv=None,
        fails_csv=None,
        progress_callback=None,
        force_full_analysis=False,
    )
    score_streams.assert_called_once_with(
        api,
        config,
        input_csv=None,
        output_csv=None,
        update_stats=False,
    )
    reorder_streams.assert_called_once_with(api, config, input_csv=None)
