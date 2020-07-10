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
    """
    Parameters
    ----------
    token: str
        GitHub personal access token and corresponds to GITHUB_TOKEN
        in Civis credentials.
    repo: str
        Repo name, such as 'CityofLosAngeles/covid19-indicators`
    branch: str
        Branch name, such as 'master'
    path: str
        Path to the file within the repo.
    local_file_path: str
        Path to the local file to be uploaded to the repo, which can differ
        from the path within the GitHub repo.
    commit_message: str
        Commit message used when making the git commit.
    commiter: dict
        name and email associated with the committer.
        Defaults to ITA robot user, if another committer is not provided..
    """

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
            "committer": committer,
            "branch": branch,
            "sha": sha,
            "content": base64.b64encode(contents).decode("utf-8"),
        },
    )
    r.raise_for_status()
