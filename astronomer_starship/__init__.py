__version__ = "2.0.4"


def get_provider_info():
    return {
        "package-name": "astronomer-starship",  # Required
        "name": "Astronomer Starship",  # Required
        "description": "Airflow Migration Utility",  # Required
        "versions": [__version__],  # Required
    }
