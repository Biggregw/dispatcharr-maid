# Command Log

Commands run while reviewing the optimize pipeline:

- `ls`
- `find .. -name AGENTS.md -print`
- `rg -n "optimiz|optimi" -S`
- `rg -n "full_cleanup|refresh_optimize|optimize" dispatcharr_web_app.py stream_analysis.py -S`
- `sed -n '1500,2200p' dispatcharr_web_app.py`
- `sed -n '2200,2600p' dispatcharr_web_app.py`
- `rg -n "stream_last_measured_days" -S`
- `sed -n '1,260p' stream_analysis.py`
- `rg -n "analyze_streams\(|stream_last_measured_days" stream_analysis.py -S`
- `sed -n '820,1040p' stream_analysis.py`
- `rg -n "def fetch_streams" -n stream_analysis.py`
- `sed -n '659,820p' stream_analysis.py`
- `nl -ba dispatcharr_web_app.py | sed -n '1830,2140p'`
- `nl -ba stream_analysis.py | sed -n '900,1020p'`
