import yaml
def read_config(path="config/config.yaml"):
  with open(path,'r') as file:
    return yaml.safe_load(file)

def get_input_files():
  config = read_config()
  return config.get("input_file",[])
