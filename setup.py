from setuptools import find_packages, setup
from os.path import dirname, join

def read_file(filename):
    with open(join(dirname(__file__), filename)) as f:
        return f.read()

setup(
    name="pybeamline",
    packages=find_packages(include=["pybeamline"]),
    version="0.1.0",
    description="Python version of Beamline (based on ReactiveX not Flink)",
    author="Andrea Burattin",
    license="Apache-2.0",
    install_requires=read_file("requirements.txt").split("\n"),
)
