def test_starship():
    from astronomer_starship import __version__

    assert __version__ is not None


def test_airflow():
    from airflow import __version__

    assert __version__ is not None
