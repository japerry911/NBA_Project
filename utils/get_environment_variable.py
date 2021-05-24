from ast import literal_eval
from os import environ, getenv
from typing import Dict, List

from exceptions import MissingEnvironmentVariable


def get_environment_variable(name: str):
    """gets environment variable either from OS or from Fargate Environment
    :param str name: the name of the environment variable being looked for
    :raises:
        MissingEnvironmentVariable: if the environment variable can not be
            found
    :return: returns the matching value of environment variable, if applicable
    """
    value = getenv(name)

    if value is not None:
        return value

    env_list_fargate = get_fargate_env()

    if env_list_fargate is not None:
        for env in env_list_fargate:
            value = get_env_from_fargate_dict(name, env)
            if value is not None:
                return value

    raise MissingEnvironmentVariable(f"Missing Env Variable - {name}")


def get_env_from_fargate_dict(name: str, env: Dict):
    """gets the environment variable from Fargate Environment Dictionary
    :param str name: environment variable being looked for
    :param Dict env: environment as a key/value Dictionary
    :return: value of found variable within Fargate Environment Dictionary
    """
    if name in env.keys():
        return env[name]
    else:
        return None


def get_fargate_env() -> List:
    """Fetches Fargate Environment if it is available
    :return: returns List of Fargate Environments
    :rtype: List
    """
    env_list = []
    for env in environ:
        if 'ENVIRONMENT_RESOURCES' in env:
            env_list.append(literal_eval(getenv(env)))
    return env_list
