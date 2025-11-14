from pybeamline.stream.base_map import BaseMap
from pybeamline.utils.dfg_to_graphviz import dfg_to_graphviz


class dfg_str_to_graphviz(BaseMap):
	
	def transform(self, value):
		return [dfg_to_graphviz(value[1])]
	