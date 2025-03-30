import pandas as pd
from datetime import datetime
from pm4py.objects.ocel.obj import OCEL
from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery
from pm4py.algo.discovery.ocel.ocpn import algorithm as ocpn_discovery
from pm4py.visualization.ocel.ocpn import visualizer as ocpn_visualizer

# Step 1: Define simple events with omap
events_df = pd.DataFrame([
    {
        "ocel:eid": "e1",
        "ocel:activity": "Create Order",
        "ocel:timestamp": datetime.now(),
        "ocel:omap": [
            {"ocel:oid": "o1", "ocel:type": "Order"},
            {"ocel:oid": "c1", "ocel:type": "Customer"}
        ]
    },
    {
        "ocel:eid": "e2",
        "ocel:activity": "Approve Order",
        "ocel:timestamp": datetime.now(),
        "ocel:omap": [
            {"ocel:oid": "o1", "ocel:type": "Order"}
        ]
    }
])
print(events_df)
# = events_df.set_index("ocel:eid", drop=False)
# = events_df.set_index("ocel:eid", drop=True)#

# Step 2: Create object table
objects_df = pd.DataFrame([
    {"ocel:oid": "o1", "ocel:type": "Order"},
    {"ocel:oid": "c1", "ocel:type": "Customer"}
])

# Step 3: Create relation table from ocel:omap
def extract_relations_table(events_df):
    rows = []
    for _, row in events_df.iterrows():
        for obj in row["ocel:omap"]:
            rows.append({
                "ocel:eid": row["ocel:eid"],
                "ocel:activity": row["ocel:activity"],
                "ocel:timestamp": row["ocel:timestamp"],
                "ocel:oid": obj["ocel:oid"],
                "ocel:type": obj["ocel:type"]
            })
    return pd.DataFrame(rows)

relations_df = extract_relations_table(events_df)

# Step 4: Build OCEL
ocel = OCEL(events=events_df, objects=objects_df, relations=relations_df)

print(ocel.get_extended_table())
# Step 5: Discover and print OC-DFG
ocdfg = ocdfg_discovery.apply(ocel)
print("🔍 OC-DFG Edges:")
for edge, count in ocdfg["edges"].items():
    print(f"  {edge[0]} -> {edge[1]} ({count})")

# Step 6: Discover and visualize OC-Petri Net
ocpn = ocpn_discovery.apply(ocel)
gviz = ocpn_visualizer.apply(ocpn)
ocpn_visualizer.view(gviz)
