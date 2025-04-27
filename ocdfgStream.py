# The following script will be used to develop an streamed OCDFG
# Hence for each event the OCDFG will be updated, and whether is directly follows or not is based
# on probability
from pybeamline.mappers.infinite_size_oc_directly_follows_graph import infinite_size_oc_directly_follows_graph
from pybeamline.sources.dict_ocel_test_source import dict_test_ocel_source
from reactivex.subject import Subject
from reactivex import operators as ops
import tkinter as tk
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
from pm4py import view_ocdfg

from pybeamline.sources.dict_to_ocel import dict_test_ocel_log

test_events_phaseflow = [
    {"activity": "Register Customer", "objects": {"Customer": ["c1"]}},
    {"activity": "Create Order", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
    {"activity": "Add Item", "objects": {"Order": ["o1"], "Item": ["i1"]}},
    {"activity": "Reserve Item", "objects": {"Item": ["i1"]}},
    {"activity": "Pack Item", "objects": {"Item": ["i1"], "Order": ["o1"]}},
    {"activity": "Ship Item", "objects": {"Item": ["i1"], "Shipment": ["s1"]}},
    {"activity": "Send Invoice", "objects": {"Order": ["o1"], "Invoice": ["inv1"]}},
    {"activity": "Receive Review", "objects": {"Customer": ["c1"], "Order": ["o1"]}},
]

combined_log = dict_test_ocel_source([(test_events_phaseflow, 1)], shuffle=True)
test_log = dict_test_ocel_log([(test_events_phaseflow, 1)], shuffle=False)

dfg = ocdfg_discovery.apply(test_log)
view_ocdfg(dfg)

# Convert the stream to a list of events for manual control
event_list = []
combined_log.subscribe(lambda e: event_list.append(e))
# Create an iterator over events
event_iter = iter(event_list)
# Create a subject (acts like an observable + observer)
click_subject = Subject()

# Function to safely get next event
def get_next_event(_):
    try:
        event = next(event_iter)
        click_subject.on_next(event)  # <-- Push real event to stream
    except StopIteration:
        print("No more events.")

# Link subject to handler
click_subject.pipe(
    ops.do_action(lambda e: print(f"Emitting event: {e}")),
    infinite_size_oc_directly_follows_graph()
).subscribe()

# GUI setup
def on_button_click():
    get_next_event(None)


root = tk.Tk()
root.title("OCDFG Event Trigger")
button = tk.Button(root, text="Click For Next Event", command=on_button_click)
button.pack(padx=20, pady=20)

root.mainloop()