from setuptools import find_packages, setup
from os.path import dirname, join


def read_file(filename):
    with open(join(dirname(__file__), filename)) as f:
        return f.read()


with open("requirements.txt", "r") as f:
    required_packages = f.read().splitlines()
with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="pybeamline",
    packages=[
        "pybeamline",
        "pybeamline.filters",
        "pybeamline.sources",
        "pybeamline.mappers",
        "pybeamline.algorithms",
        "pybeamline.algorithms.conformance",
        "pybeamline.algorithms.discovery",
    ],
    version="1.0.8",
    description="Python version of Beamline (based on ReactiveX)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Andrea Burattin",
    license="Apache-2.0",
    install_requires=required_packages
)
