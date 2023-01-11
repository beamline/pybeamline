from setuptools import find_packages, setup
from os.path import dirname, join


def read_file(filename):
    with open(join(dirname(__file__), filename)) as f:
        return f.read()


setup(
    name="pybeamline",
    packages=[
        "pybeamline",
        "pybeamline.filters",
        "pybeamline.sources",
        "pybeamline.mappers"
    ],
    version="0.0.1b6",
    description="Python version of Beamline (based on ReactiveX)",
    author="Andrea Burattin",
    license="Apache-2.0",
    install_requires=[
        'pm4py',
        'reactivex',
        'pandas'
    ]
)
