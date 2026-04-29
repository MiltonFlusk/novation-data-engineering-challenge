from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def get_data_root():
    docker_data_root = Path("/data")

    if docker_data_root.exists():
        return docker_data_root

    return PROJECT_ROOT / "data"


def resolve_path(path_value):
    path_value = str(path_value).replace("\\", "/")

    if path_value.startswith("/data/"):
        if Path("/data").exists():
            return path_value

        return str(PROJECT_ROOT / path_value.lstrip("/"))

    if path_value.startswith("data/"):
        relative_part = path_value.replace("data/", "", 1)
        return str(get_data_root() / relative_part)

    return str(PROJECT_ROOT / path_value)