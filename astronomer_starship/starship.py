from airflow import __version__

[major, _] = __version__.split(".", maxsplit=1)

if int(major) == 2:
    pass
elif int(major) == 3:
    pass
