from pybeamline.stream.base_map import BaseMap

class skip_events(BaseMap):
	def __init__(self, events_to_skip):
		self.count = 0
		self.events_to_skip = events_to_skip

	def transform(self, value):
		self.count += 1
		if self.count % self.events_to_skip == 0:
			self.count = 0
			return [value]
		return None