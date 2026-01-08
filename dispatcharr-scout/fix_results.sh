#!/bin/bash
cd templates
cp results.html results.html.charts_backup
cat > results.html << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Analysis Results</title>
    <style>
        body { font-family: Arial; max-width: 1200px; margin: 40px auto; padding: 20px; background: #f5f5f5; }
        .card { background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background: #667eea; color: white; }
        .btn { padding: 10px 20px; background: #667eea; color: white; border: none; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; margin: 5px; }
        .stat { font-size: 2em; font-weight: bold; margin: 10px 0; }
        .success { color: #38a169; }
        .failed { color: #e53e3e; }
    </style>
</head>
<body>
    <h1>📊 Analysis Results</h1>
    <a href="/" class="btn">← Back to Control Panel</a>
    <a href="/api/results/csv" class="btn">📥 Export CSV</a>
    
    <div class="card">
        <h2>📈 Overall Summary</h2>
        <div id="summary">Loading...</div>
    </div>
    
    <div class="card">
        <h2>🏆 Provider Performance</h2>
        <div id="providers">Loading...</div>
    </div>
    
    <div class="card">
        <h2>📺 Channels</h2>
        <div id="channels">Loading...</div>
    </div>
    
    <div class="card">
        <h2>⚠️ Errors</h2>
        <div id="errors">Loading...</div>
    </div>
    
    <script>
        fetch('/api/results/detailed')
            .then(r => r.json())
            .then(data => {
                if (!data.success) {
                    document.body.innerHTML = '<div class="card"><h2>No results available</h2><p>Run an analysis first.</p><a href="/" class="btn">← Back</a></div>';
                    return;
                }
                
                const r = data.results;
                
                // Summary
                document.getElementById('summary').innerHTML = `
                    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px;">
                        <div><div style="color: #718096;">Total Analyzed</div><div class="stat">${r.total}</div></div>
                        <div><div style="color: #718096;">Successful</div><div class="stat success">${r.successful}</div></div>
                        <div><div style="color: #718096;">Failed</div><div class="stat failed">${r.failed}</div></div>
                        <div><div style="color: #718096;">Success Rate</div><div class="stat">${r.success_rate}%</div></div>
                    </div>
                `;
                
                // Providers
                const providers = Object.entries(r.provider_stats || {});
                if (providers.length > 0) {
                    let provHTML = '<table><tr><th>Provider</th><th>Total</th><th>Success</th><th>Failed</th><th>Success Rate</th><th>Avg Quality</th></tr>';
                    providers.sort((a, b) => a[1].success_rate - b[1].success_rate).forEach(([id, stats]) => {
                        const badge = stats.success_rate >= 90 ? '⭐' : stats.success_rate >= 70 ? '✓' : stats.success_rate >= 50 ? '⚠️' : '❌';
                        provHTML += `<tr>
                            <td>Provider ${id} ${badge}</td>
                            <td>${stats.total}</td>
                            <td class="success">${stats.successful}</td>
                            <td class="failed">${stats.failed}</td>
                            <td><strong>${stats.success_rate}%</strong></td>
                            <td>${stats.avg_quality}</td>
                        </tr>`;
                    });
                    provHTML += '</table>';
                    document.getElementById('providers').innerHTML = provHTML;
                } else {
                    document.getElementById('providers').innerHTML = '<p>No provider data available</p>';
                }
                
                // Channels
                const channels = Object.entries(r.channel_stats || {});
                if (channels.length > 0) {
                    let chanHTML = '<table><tr><th>Channel</th><th>Total Streams</th><th>Success</th><th>Failed</th><th>Success Rate</th></tr>';
                    channels.sort((a, b) => parseInt(a[0]) - parseInt(b[0])).forEach(([num, stats]) => {
                        chanHTML += `<tr>
                            <td><strong>#${num}</strong> ${stats.name}</td>
                            <td>${stats.total}</td>
                            <td class="success">${stats.successful}</td>
                            <td class="failed">${stats.failed}</td>
                            <td><strong>${stats.success_rate}%</strong></td>
                        </tr>`;
                    });
                    chanHTML += '</table>';
                    document.getElementById('channels').innerHTML = chanHTML;
                } else {
                    document.getElementById('channels').innerHTML = '<p>No channel data available</p>';
                }
                
                // Errors
                const errors = Object.entries(r.error_types || {});
                if (errors.length > 0) {
                    let errHTML = '<table><tr><th>Error Type</th><th>Occurrences</th></tr>';
                    errors.sort((a, b) => b[1] - a[1]).forEach(([type, count]) => {
                        errHTML += `<tr><td>${type}</td><td class="failed">${count}</td></tr>`;
                    });
                    errHTML += '</table>';
                    document.getElementById('errors').innerHTML = errHTML;
                } else {
                    document.getElementById('errors').innerHTML = '<p style="color: green;">✅ No errors! All streams working perfectly!</p>';
                }
            })
            .catch(err => {
                document.body.innerHTML = '<div class="card"><h2>Error loading results</h2><p>' + err + '</p><a href="/" class="btn">← Back</a></div>';
            });
    </script>
</body>
</html>
EOF
echo "✅ Created simple table-based results page"
