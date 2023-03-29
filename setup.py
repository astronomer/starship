import os

# hack - remove a symlink while building
del os.link

from setuptools import setup

if __name__ == "__main__":
    setup()
