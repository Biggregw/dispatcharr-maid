#!/bin/bash
echo "=== VALIDATION CHECKLIST ==="
echo ""

# 1. Check imports
echo "1. Checking imports in dispatcharr_web_app.py..."
if grep -q "refresh_channel_streams" dispatcharr_web_app.py; then
    echo "   ✓ refresh_channel_streams imported"
else
    echo "   ✗ MISSING: refresh_channel_streams import"
fi

# 2. Check Job class
echo ""
echo "2. Checking Job class __init__..."
INIT_LINE=$(grep -A 2 "def __init__" dispatcharr_web_app.py | grep "job_id, job_type, groups, channels")
if echo "$INIT_LINE" | grep -q "include_filter.*exclude_filter.*streams_per_provider"; then
    echo "   ✓ Job __init__ has all parameters"
else
    echo "   ✗ MISSING: Job __init__ parameters incomplete"
    echo "   Found: $INIT_LINE"
fi

# 3. Check Job class stores filters
echo ""
echo "3. Checking Job class stores filters..."
if grep -q "self.include_filter = include_filter" dispatcharr_web_app.py && \
   grep -q "self.exclude_filter = exclude_filter" dispatcharr_web_app.py; then
    echo "   ✓ Job stores filters"
else
    echo "   ✗ MISSING: Job doesn't store filters"
fi

# 4. Check api_start_job extracts variables
echo ""
echo "4. Checking api_start_job extracts filter variables..."
START_JOB=$(grep -A 120 "def api_start_job" dispatcharr_web_app.py)
if echo "$START_JOB" | grep -q "include_filter = data.get" && \
   echo "$START_JOB" | grep -q "exclude_filter = data.get" && \
   echo "$START_JOB" | grep -q "streams_per_provider = data.get"; then
    echo "   ✓ Variables extracted from request"
else
    echo "   ✗ MISSING: Variables not extracted"
fi

# 5. Check Job creation
echo ""
echo "5. Checking Job creation passes all parameters..."
JOB_CREATE=$(grep -A 40 "job = Job(" dispatcharr_web_app.py | tr '\n' ' ')
if echo "$JOB_CREATE" | grep -q "include_filter" && \
   echo "$JOB_CREATE" | grep -q "exclude_filter" && \
   echo "$JOB_CREATE" | grep -q "streams_per_provider"; then
    echo "   ✓ Job created with all parameters"
else
    echo "   ✗ MISSING: Job creation incomplete"
    echo "   Found: $JOB_CREATE"
fi

# 6. Check refresh_optimize handler exists
echo ""
echo "6. Checking refresh_optimize handler in run_job_worker..."
if grep -q "elif job.job_type == 'refresh_optimize':" dispatcharr_web_app.py; then
    echo "   ✓ refresh_optimize handler exists"
else
    echo "   ✗ MISSING: refresh_optimize handler"
fi

# 7. Check refresh_optimize calls function correctly
echo ""
echo "7. Checking refresh_optimize calls refresh_channel_streams..."
REFRESH_CALL=$(grep -A 5 "refresh_channel_streams(" dispatcharr_web_app.py | grep "job.include_filter")
if [ -n "$REFRESH_CALL" ]; then
    echo "   ✓ Calls with job.include_filter and job.exclude_filter"
else
    echo "   ✗ MISSING: Not using job filters"
fi

# 8. Check JavaScript sends filters
echo ""
echo "8. Checking JavaScript sends filter values..."
if grep -q "include_filter.*includeFilter" templates/app.html && \
   grep -q "exclude_filter.*excludeFilter" templates/app.html; then
    echo "   ✓ JavaScript sends filters"
else
    echo "   ✗ MISSING: JavaScript doesn't send filters"
fi

# 9. Check refresh_channel_streams function exists
echo ""
echo "9. Checking refresh_channel_streams function in stream_analysis.py..."
if grep -q "^def refresh_channel_streams" stream_analysis.py; then
    echo "   ✓ Function exists"
else
    echo "   ✗ MISSING: refresh_channel_streams function"
fi

# 10. Check refresh_channel_streams signature supports filters
echo ""
echo "10. Checking refresh_channel_streams signature supports filters..."
SIG_LINE=$(grep "^def refresh_channel_streams" stream_analysis.py)
if echo "$SIG_LINE" | grep -q "include_filter" && \
   echo "$SIG_LINE" | grep -q "exclude_filter" && \
   echo "$SIG_LINE" | grep -q "stream_name_regex_override"; then
    echo "   ✓ refresh_channel_streams supports filters + regex override"
else
    echo "   ✗ MISSING: refresh_channel_streams signature missing expected parameters"
    echo "   Found: $SIG_LINE"
fi

echo ""
echo "=== VALIDATION COMPLETE ==="
