import sys
sys.path.insert(0, '/app')

# Patch the generate_job_summary function
code = open('dispatcharr_web_app.py', 'r').read()

# Find and replace the error analysis section
old_code = '''        # Error analysis
        error_types = {}
        if 'error_message' in df.columns:
            failed_df = df[df['status'] != 'OK']
            for error in failed_df['error_message'].value_counts().items():
                if pd.notna(error[0]):
                    error_types[error[0]] = int(error[1])'''

new_code = '''        # Error analysis
        error_types = {}
        failed_df = df[df['status'] != 'OK']
        if len(failed_df) > 0:
            # Use status column for error types
            for error in failed_df['status'].value_counts().items():
                if pd.notna(error[0]) and error[0] != 'OK':
                    error_types[error[0]] = int(error[1])'''

code = code.replace(old_code, new_code)

open('dispatcharr_web_app.py', 'w').write(code)
print("✅ Fixed error detection")
