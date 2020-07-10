import base64
import fsspec
import requests

# Function to overwrite file in GitHub
DEFAULT_COMMITTER = {
    "name": "Los Angeles ITA data team",
    "email": "ITAData@lacity.org",
}


def upload_file_to_github(
    token,
    repo,
    branch,
    path,
    local_file_path,
    commit_message,
    committer=DEFAULT_COMMITTER,
):
    BASE = "https://api.github.com"

    # Get the sha of the previous version
    r = requests.get(
        f"{BASE}/repos/{repo}/contents/{path}",
        params={"ref": branch},
        headers={"Authorization": f"token {token}"},
    )
    r.raise_for_status()
    sha = r.json()["sha"]

    # Upload the new version
    with fsspec.open(local_file_path, "rb") as f:
        contents = f.read()

    r = requests.put(
        f"{BASE}/repos/{repo}/contents/{path}",
        headers={"Authorization": f"token {token}"},
        json={
            "message": commit_message,
            "committer": DEFAULT_COMMITTER,
            "branch": branch,
            "sha": sha,
            "content": base64.b64encode(contents).decode("utf-8"),
        },
    )
    r.raise_for_status()
