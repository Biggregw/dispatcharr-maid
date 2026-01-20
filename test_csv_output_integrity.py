from stream_analysis import _build_csv_writer, _normalize_csv_row, _read_analysis_csv


def test_analysis_csv_handles_commas_and_newlines(tmp_path):
    csv_path = tmp_path / "analysis.csv"
    fieldnames = ["channel_id", "stream_name", "stream_url", "status", "notes"]
    row = {
        "channel_id": "10",
        "stream_name": "News, Local",
        "stream_url": "http://example.com/stream?token=abc,123",
        "status": "Error",
        "notes": "ffmpeg error:\r\nline1, with comma\nline2 \"quoted\"",
    }

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = _build_csv_writer(handle, fieldnames)
        writer.writeheader()
        writer.writerow(_normalize_csv_row(row))

    df = _read_analysis_csv(csv_path)
    assert list(df.columns) == fieldnames
    assert len(df) == 1
    assert df.loc[0, "notes"] == "ffmpeg error:\nline1, with comma\nline2 \"quoted\""
    assert df.loc[0, "stream_name"] == "News, Local"
    assert df.loc[0, "stream_url"] == "http://example.com/stream?token=abc,123"
