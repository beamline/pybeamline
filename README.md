<div class="cell markdown">

# Streaming process mining with `pyBeamline`

</div>

<div class="cell markdown">

`pyBeamline` is a Python version of Beamline. While the same set of
ideas and principles of Beamline have been ported into `pyBeamline`, the
underlying goal and technology is very different.

pyBeamline is based on ReactiveX and its Python binding RxPY. RxPY is a
library for composing asynchronous and event-based programs using
observable sequences and pipable query operators in Python. Using
pyBeamline it is possible to inject process mining operators into the
computation.

This Jupyter notebook contains the main functionalities currently
exposed by `pyBeamline`. For a complete documentation of the library see
<https://www.beamline.cloud/pybeamline/>. In the rest of the notebook it
is assumed that the `pyBeamline` package is already installed.

In the rest of this document, the main functionalities are exposed.

</div>

<div class="cell markdown">

### Sources

</div>

<div class="cell code" execution_count="13">

``` python
# Let's ignore some PM4PY warnings in the notebook
import warnings
warnings.filterwarnings("ignore")
```

</div>

<div class="cell code" execution_count="14">

``` python
from pybeamline.sources import string_test_source

string_test_source(["ABC", "ACB", "EFG"]) \
    .subscribe(lambda x: print(str(x)))
```

<div class="output stream stdout">

    (A, case_1, Process, 2023-06-21 09:38:33.265476 - {} - {} - {})
    (B, case_1, Process, 2023-06-21 09:38:33.265476 - {} - {} - {})
    (C, case_1, Process, 2023-06-21 09:38:33.265476 - {} - {} - {})
    (A, case_2, Process, 2023-06-21 09:38:33.265476 - {} - {} - {})
    (C, case_2, Process, 2023-06-21 09:38:33.265476 - {} - {} - {})
    (B, case_2, Process, 2023-06-21 09:38:33.265476 - {} - {} - {})
    (E, case_3, Process, 2023-06-21 09:38:33.265476 - {} - {} - {})
    (F, case_3, Process, 2023-06-21 09:38:33.265476 - {} - {} - {})
    (G, case_3, Process, 2023-06-21 09:38:33.265476 - {} - {} - {})

    <reactivex.disposable.disposable.Disposable at 0x173527f59a0>

</div>

</div>

<div class="cell code" execution_count="15">

``` python
from pybeamline.sources import xes_log_source_from_file

xes_log_source_from_file("tests/log.xes") \
    .subscribe(lambda x: print(str(x)))
```

<div class="output stream stderr">

    parsing log, completed traces :: 100%|██████████| 2/2 [00:00<00:00, 908.74it/s]

    (a11, c1, log-file, 2023-06-21 09:38:35.571279 - {'lifecycle:transition': 'complete', 'act': 'a11'} - {'variant': 'Variant 1', 'creator': 'Fluxicon Disco', 'variant-index': 1} - {})
    (a12, c1, log-file, 2023-06-21 09:38:35.572279 - {'lifecycle:transition': 'complete', 'act': 'a12'} - {'variant': 'Variant 1', 'creator': 'Fluxicon Disco', 'variant-index': 1} - {})
    (a21, c2, log-file, 2023-06-21 09:38:35.572279 - {'lifecycle:transition': 'complete', 'act': 'a21'} - {'variant': 'Variant 2', 'creator': 'Fluxicon Disco', 'variant-index': 2} - {})
    (a22, c2, log-file, 2023-06-21 09:38:35.572279 - {'lifecycle:transition': 'complete', 'act': 'a22'} - {'variant': 'Variant 2', 'creator': 'Fluxicon Disco', 'variant-index': 2} - {})
    (a23, c2, log-file, 2023-06-21 09:38:35.572279 - {'lifecycle:transition': 'complete', 'act': 'a23'} - {'variant': 'Variant 2', 'creator': 'Fluxicon Disco', 'variant-index': 2} - {})

    <reactivex.disposable.disposable.Disposable at 0x173528457c0>

</div>

</div>

<div class="cell code" execution_count="16">

``` python
from pybeamline.sources import log_source

log_source(["ABC", "ACB", "EFG"]) \
    .subscribe(lambda x: print(str(x)))

log_source("tests/log.xes") \
    .subscribe(lambda x: print(str(x)))
```

<div class="output stream stdout">

    (A, case_1, Process, 2023-06-21 09:38:39.484791 - {} - {} - {})
    (B, case_1, Process, 2023-06-21 09:38:39.484791 - {} - {} - {})
    (C, case_1, Process, 2023-06-21 09:38:39.484791 - {} - {} - {})
    (A, case_2, Process, 2023-06-21 09:38:39.484791 - {} - {} - {})
    (C, case_2, Process, 2023-06-21 09:38:39.484791 - {} - {} - {})
    (B, case_2, Process, 2023-06-21 09:38:39.484791 - {} - {} - {})
    (E, case_3, Process, 2023-06-21 09:38:39.484791 - {} - {} - {})
    (F, case_3, Process, 2023-06-21 09:38:39.484791 - {} - {} - {})
    (G, case_3, Process, 2023-06-21 09:38:39.484791 - {} - {} - {})

    parsing log, completed traces :: 100%|██████████| 2/2 [00:00<00:00, 7973.96it/s]

</div>

<div class="output stream stdout">

    (a11, c1, log-file, 2023-06-21 09:38:39.491467 - {'lifecycle:transition': 'complete', 'act': 'a11'} - {'variant': 'Variant 1', 'creator': 'Fluxicon Disco', 'variant-index': 1} - {})
    (a12, c1, log-file, 2023-06-21 09:38:39.492473 - {'lifecycle:transition': 'complete', 'act': 'a12'} - {'variant': 'Variant 1', 'creator': 'Fluxicon Disco', 'variant-index': 1} - {})
    (a21, c2, log-file, 2023-06-21 09:38:39.492473 - {'lifecycle:transition': 'complete', 'act': 'a21'} - {'variant': 'Variant 2', 'creator': 'Fluxicon Disco', 'variant-index': 2} - {})
    (a22, c2, log-file, 2023-06-21 09:38:39.492473 - {'lifecycle:transition': 'complete', 'act': 'a22'} - {'variant': 'Variant 2', 'creator': 'Fluxicon Disco', 'variant-index': 2} - {})
    (a23, c2, log-file, 2023-06-21 09:38:39.492473 - {'lifecycle:transition': 'complete', 'act': 'a23'} - {'variant': 'Variant 2', 'creator': 'Fluxicon Disco', 'variant-index': 2} - {})

    <reactivex.disposable.disposable.Disposable at 0x17352845340>

</div>

</div>

<div class="cell markdown">

### Filters

</div>

<div class="cell code" execution_count="34">

``` python
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

<div class="output stream stderr">

    parsing log, completed traces :: 100%|██████████| 2/2 [00:00<00:00, 1849.34it/s]

    (a12, c1, log-file, 2023-06-21 09:40:46.263579 - {'lifecycle:transition': 'complete', 'act': 'a12'} - {'variant': 'Variant 1', 'creator': 'Fluxicon Disco', 'variant-index': 1} - {})
    (a21, c2, log-file, 2023-06-21 09:40:46.264566 - {'lifecycle:transition': 'complete', 'act': 'a21'} - {'variant': 'Variant 2', 'creator': 'Fluxicon Disco', 'variant-index': 2} - {})
    (a22, c2, log-file, 2023-06-21 09:40:46.265082 - {'lifecycle:transition': 'complete', 'act': 'a22'} - {'variant': 'Variant 2', 'creator': 'Fluxicon Disco', 'variant-index': 2} - {})
    (a23, c2, log-file, 2023-06-21 09:40:46.265082 - {'lifecycle:transition': 'complete', 'act': 'a23'} - {'variant': 'Variant 2', 'creator': 'Fluxicon Disco', 'variant-index': 2} - {})

    <reactivex.disposable.disposable.Disposable at 0x173521178b0>

</div>

</div>

<div class="cell markdown">

### Discovery techniques

</div>

<div class="cell markdown">

Mining of directly-follows relations:

</div>

<div class="cell code" execution_count="40">

``` python
from pybeamline.sources import log_source
from pybeamline.mappers import infinite_size_directly_follows_mapper

log_source(["ABC", "ACB"]).pipe(
    infinite_size_directly_follows_mapper()
).subscribe(lambda x: print(str(x)))
```

<div class="output stream stdout">

    ('A', 'B')
    ('B', 'C')
    ('A', 'C')
    ('C', 'B')

    <reactivex.disposable.disposable.Disposable at 0x173529a5b80>

</div>

</div>

<div class="cell markdown">

Mining of a Heuristics net using Lossy Counting:

</div>

<div class="cell code" execution_count="43">

``` python
from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting

log_source(["ABCD", "ABCD"]).pipe(
    heuristics_miner_lossy_counting(model_update_frequency=4)
).subscribe(lambda x: print(str(x)))
```

<div class="output stream stdout">

    {'A': (node:A connections:{B:[0.5]}), 'B': (node:B connections:{C:[0.5]}), 'C': (node:C connections:{})}
    {'C': (node:C connections:{D:[0.5]}), 'D': (node:D connections:{}), 'A': (node:A connections:{B:[0.6666666666666666]}), 'B': (node:B connections:{C:[0.6666666666666666]})}

    <reactivex.disposable.disposable.Disposable at 0x173521cfbb0>

</div>

</div>

<div class="cell markdown">

Mining of a Heuristics net using Lossy Counting with Budget:

</div>

<div class="cell code" execution_count="44">

``` python
from pybeamline.algorithms.discovery import heuristics_miner_lossy_counting_budget

log_source(["ABCD", "ABCD"]).pipe(
    heuristics_miner_lossy_counting_budget(model_update_frequency=4)
).subscribe(lambda x: print(str(x)))
```

<div class="output stream stdout">

    {'A': (node:A connections:{B:[0.5]}), 'B': (node:B connections:{C:[0.5]}), 'C': (node:C connections:{D:[0.5]}), 'D': (node:D connections:{})}
    {'A': (node:A connections:{B:[0.6666666666666666]}), 'B': (node:B connections:{C:[0.6666666666666666]}), 'C': (node:C connections:{D:[0.6666666666666666]}), 'D': (node:D connections:{})}

    <reactivex.disposable.disposable.Disposable at 0x173521cfd60>

</div>

</div>

<div class="cell markdown">

### Conformance checking

</div>

<div class="cell markdown">

Currently only conformance checking using behavioral profiles is
supported:

</div>

<div class="cell code" execution_count="49">

``` python
from pybeamline.algorithms.conformance import mine_behavioral_model_from_stream, behavioral_conformance

source = log_source(["ABCD", "ABCD"])
reference_model = mine_behavioral_model_from_stream(source)
print(reference_model)

log_source(["ABCD", "ABCD"]).pipe(
    excludes_activity_filter("A"),
    behavioral_conformance(reference_model)
).subscribe(lambda x: print(str(x)))
```

<div class="output stream stdout">

    ([('A', 'B'), ('B', 'C'), ('C', 'D')], {('A', 'B'): (0, 0), ('B', 'C'): (1, 1), ('C', 'D'): (2, 2)}, {('A', 'B'): 2, ('B', 'C'): 1, ('C', 'D'): 0})
    (1.0, 0.5, 1)
    (1.0, 1.0, 1)
    (1.0, 0.5, 1)
    (1.0, 1.0, 1)


    <reactivex.disposable.disposable.Disposable at 0x17352e173a0>

</div>

</div>

<div class="cell markdown">

### Sliding window

This technique allows to apply any existing process mininig technique on
streaming data

</div>

<div class="cell code" execution_count="39">

``` python
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

<div class="output stream stdout">

    Counter({('A', 'B'): 1, ('B', 'C'): 1})
    Counter({('A', 'B'): 1, ('B', 'D'): 1})

    <reactivex.disposable.disposable.Disposable at 0x17352a192e0>

</div>

</div>
