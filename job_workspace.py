from pathlib import Path
import shutil
import uuid


def create_job_workspace(job_id=None, base_config_path='config.yaml', jobs_root='jobs'):
    """Create a unique working directory for a job and seed it with the base config."""
    job_id = job_id or str(uuid.uuid4())
    workspace_root = Path(jobs_root)
    workspace_root.mkdir(parents=True, exist_ok=True)

    workspace = workspace_root / job_id
    workspace.mkdir(parents=True, exist_ok=True)

    base_config = Path(base_config_path)
    target_config = workspace / base_config.name

    if base_config.exists() and not target_config.exists():
        shutil.copy(base_config, target_config)

    # Copy provider_names.json if it exists (for custom provider name mappings)
    provider_names = Path('provider_names.json')
    target_provider_names = workspace / 'provider_names.json'
    if provider_names.exists() and not target_provider_names.exists():
        shutil.copy(provider_names, target_provider_names)

    return workspace, target_config
