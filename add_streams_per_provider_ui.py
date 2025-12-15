import re

# Update the web app HTML to add streams_per_provider control
with open('templates/app.html', 'r') as f:
    html = f.read()

# Find the Step 3 action buttons section and add the control before it
insert_point = '<div class="action-buttons">'

new_control = '''            <div class="setting-box" style="margin-bottom: 20px; padding: 15px; background: #f7fafc; border-radius: 8px;">
                <label style="display: block; margin-bottom: 8px; font-weight: 600; color: #2d3748;">
                    🔄 Streams Per Provider
                </label>
                <div style="display: flex; align-items: center; gap: 15px;">
                    <input type="number" id="streamsPerProvider" min="1" max="10" value="1" 
                           style="width: 80px; padding: 8px; border: 2px solid #e2e8f0; border-radius: 6px; font-size: 1.1em;">
                    <span style="color: #718096; font-size: 0.9em;">
                        How many streams to keep per provider (1 = best only, 2 = top 2 from each, etc.)
                    </span>
                </div>
                <div style="margin-top: 8px; padding: 10px; background: #eef2ff; border-left: 3px solid #667eea; border-radius: 4px; font-size: 0.85em; color: #4338ca;">
                    <strong>Example:</strong> With 3 providers and value = 2:<br>
                    Provider A #1, Provider B #1, Provider C #1, Provider A #2, Provider B #2, Provider C #2
                </div>
            </div>

            <div class="action-buttons">'''

html = html.replace(insert_point, new_control)

with open('templates/app.html', 'w') as f:
    f.write(html)

print("✅ Added streams_per_provider UI control")
