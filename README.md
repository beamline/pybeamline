# Streaming process mining with `pyBeamline`

`pyBeamline` is a Python version of Beamline. While the same set of ideas and principles of Beamline have been ported into `pyBeamline`, the underlying goal and technology is very different.

pyBeamline is based on ReactiveX and its Python binding RxPY. RxPY is a library for composing asynchronous and event-based programs using observable sequences and pipable query operators in Python. Using pyBeamline it is possible to inject process mining operators into the computation.

This Jupyter notebook contains the main functionalities currently exposed by `pyBeamline`. For a complete documentation of the library see https://www.beamline.cloud/pybeamline/. In the rest of the notebook it is assumed that the `pyBeamline` package is already installed.

In the rest of this document, the main functionalities are exposed.

## Sources

```python
# Let's ignore some PM4PY warnings in the notebook
import warnings
warnings.filterwarnings("ignore")
```
```python
from pybeamline.sources import string_test_source

string_test_source(["ABC", "ACB", "EFG"]) \
    .subscribe(lambda x: print(str(x)))
```
```python
from pybeamline.sources import xes_log_source_from_file

xes_log_source_from_file("tests/log.xes") \
    .subscribe(lambda x: print(str(x)))
```
```python
from pybeamline.sources import log_source

log_source(["ABC", "ACB", "EFG"]) \
    .subscribe(lambda x: print(str(x)))

log_source("tests/log.xes") \
    .subscribe(lambda x: print(str(x)))
```

## Filters
```python
from pybeamline.sources import log_source
from pybeamline.filters import excludes_activity_filter

log_source("tests/log.xes").pipe(
    excludes_activity_filter("a11"),
).subscribe(lambda x: print(str(x)))

# Similar functionalities for these filters:
# - excludes_on_event_attribute_equal_filter
# - retains_on_trace_attribute_equal_filter
# - excludes_on_trace_attribute_equal_filter
# - retains_activity_filter
# - excludes_activity_filter
```

## Discovery techniques

Mining of directly-follows relations:

```python
from pybeamline.sources import log_source
from pybeamline.mappers import infinite_size_directly_follows_mapper

log_source(["ABC", "ACB"]).pipe(
    infinite_size_directly_follows_mapper()
).subscribe(lambda x: print(str(x)))
```

Mining of a Heuristics net using Lossy Counting:

```python
from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting

log_source(["ABCD", "ABCD"]).pipe(
    heuristics_miner_lossy_counting(model_update_frequency=4)
).subscribe(lambda x: print(str(x)))
```

Mining of a Heuristics net using Lossy Counting with Budget:

```python
from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting_budget

log_source(["ABCD", "ABCD"]).pipe(
    heuristics_miner_lossy_counting_budget(model_update_frequency=4)
).subscribe(lambda x: print(str(x)))
```

Heuristics miner with Lossy Counting and Heuristics miner with Lossy Counting with Budgets are presented in:
* [Control-flow Discovery from Event Streams](https://andrea.burattin.net/publications/2014-cec)  
A. Burattin, A. Sperduti, W. M. P. van der Aalst  
In *Proceedings of the Congress on Evolutionary Computation* (IEEE WCCI CEC 2014); Beijing, China; July 6-11, 2014.
* [Heuristics Miners for Streaming Event Data](https://andrea.burattin.net/publications/2012-corr-stream)  
A. Burattin, A. Sperduti, W. M. P. van der Aalst  
In *CoRR* abs/1212.6383, Dec. 2012.

## Conformance checking

Currently only conformance checking using behavioral profiles is supported:

```python
from pybeamline.algorithms.conformance import mine_behavioral_model_from_stream, behavioral_conformance

source = log_source(["ABCD", "ABCD"])
reference_model = mine_behavioral_model_from_stream(source)
print(reference_model)

log_source(["ABCD", "ABCD"]).pipe(
    excludes_activity_filter("A"),
    behavioral_conformance(reference_model)
).subscribe(lambda x: print(str(x)))
```

The technique is described in
* [Online Conformance Checking Using Behavioural Patterns](https://andrea.burattin.net/publications/2018-bpm)  
A. Burattin, S. van Zelst, A. Armas-Cervantes, B. van Dongen, J. Carmona  
In *Proceedings of BPM 2018*; Sydney, Australia; September 2018.

## Sliding window

This technique allows to apply any existing process mininig technique on streaming data

```python
from pybeamline.sources import log_source
from pybeamline.mappers import sliding_window_to_log
from reactivex.operators import window_with_count
import pm4py

def mine(log):
    print(pm4py.discover_dfg_typed(log))

log_source(["ABC", "ABD"]).pipe(
    window_with_count(3),
    sliding_window_to_log()
).subscribe(mine)
```