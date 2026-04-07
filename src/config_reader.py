import os
import yaml

repo_path = "/Workspace/Repos/dynamodebanjanuchiha@gmail.com/etl-medallion-project"

def read_config(path=None):
    if path is None:
        path = os.path.join(repo_path, "config", "config.yaml")
    with open(path, "r") as f:
        return yaml.safe_load(f)

def get_input_file():
    config = read_config()
    return config["files"]["input_file"]
