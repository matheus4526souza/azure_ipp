import os
def get_env(name: str) -> str:
    string = os.getenv(name)
    if not string:
        raise ValueError(f'{name} is not set in the environment')
    return string