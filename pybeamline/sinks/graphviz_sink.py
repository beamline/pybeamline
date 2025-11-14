from IPython.display import display, clear_output
from graphviz import Source

import imageio.v2 as imageio
from PIL import Image
from io import BytesIO
import numpy as np


class graphviz_sink:

	def __init__(self, gif_path=None, fps=5, mode="RGB", bg_color=(255, 255, 255), center=True):
		self.gif_path = gif_path
		self.fps = fps
		self.mode = mode
		self.bg_color = bg_color
		self.center = center

		self.frames = []
		self.max_w = 0
		self.max_h = 0


	def __call__(self, dot_string):
		clear_output(wait=True)

		# Display in notebook
		src = Source(dot_string)
		display(src)

		if self.gif_path is None:
			return

		# Render Graphviz DOT to PNG bytes
		png_data = src.pipe(format="png")

		# Open with PIL and normalize mode
		img = Image.open(BytesIO(png_data)).convert(self.mode)

		w, h = img.size

		# Track max width/height seen so far
		if w > self.max_w:
			self.max_w = w
		if h > self.max_h:
			self.max_h = h

		# Store original image (no resizing)
		self.frames.append(img)

		# Save/update GIF with padded frames
		self._save_gif()


	def _save_gif(self):
		# Build padded frames with identical size (max_w, max_h)
		padded_frames = []
		for img in self.frames:
			w, h = img.size

			# Create a new transparent/solid background canvas
			canvas = Image.new(self.mode, (self.max_w, self.max_h), self.bg_color)

			if self.center:
				offset = ((self.max_w - w) // 2, (self.max_h - h) // 2)
			else:
				offset = (0, 0)

			canvas.paste(img, offset)

			# Convert to numpy array for imageio
			frame_array = np.array(canvas, dtype=np.uint8)
			padded_frames.append(frame_array)

		imageio.mimsave(self.gif_path, padded_frames, fps=self.fps, loop=0)
