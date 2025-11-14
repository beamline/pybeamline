from pybeamline.stream.base_map import BaseMap

class to_directly_follow_relations(BaseMap):
	def __init__(self):
		self.map_cases = dict()

	def transform(self, value):
		ret = None
		if value.get_trace_name() in self.map_cases.keys():
			ret = (self.map_cases[value.get_trace_name()], value.get_event_name())
		self.map_cases[value.get_trace_name()] = value.get_event_name()
		return [ret] if ret is not None else None