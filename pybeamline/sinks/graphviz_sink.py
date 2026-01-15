from typing import Optional
from IPython.display import display
from graphviz import Source

import imageio.v2 as imageio
from PIL import Image
from io import BytesIO
import numpy as np

from pybeamline.stream.base_sink import BaseSink

try:
	from IPython.display import clear_output as _clear_output
	from IPython.display import display as _display
except Exception:
	_clear_output = None
	_display = None


class graphviz_sink(BaseSink[str]):
	def __init__(
			self,
			gif_path: Optional[str] = None,
			fps: int = 5,
			mode: str = "RGB",
			bg_color=(255, 255, 255),
			center: bool = True,
			write_every: Optional[int] = None,  # e.g. 20; None = only on close
			display_in_notebook: bool = True,
	):
		self.gif_path = gif_path
		self.fps = fps
		self.mode = mode
		self.bg_color = bg_color
		self.center = center
		self.write_every = write_every
		self.display_in_notebook = display_in_notebook

		self.frames = []  # store PIL Images
		self.max_w = 0
		self.max_h = 0
		self._count = 0
		self._closed = False

	def consume(self, dot_string: str) -> None:
		src = Source(dot_string)

		if self.display_in_notebook and _clear_output is not None and _display is not None:
			_clear_output(wait=True)
			_display(src)

		if self.gif_path is None:
			return

		# Render DOT -> PNG bytes
		png_data = src.pipe(format="png")

		# PIL image, normalized mode
		img = Image.open(BytesIO(png_data)).convert(self.mode)

		w, h = img.size
		self.max_w = max(self.max_w, w)
		self.max_h = max(self.max_h, h)

		# Store original frame; we will pad at flush time
		self.frames.append(img)
		self._count += 1

		# Optional periodic flush
		if self.write_every and self._count % self.write_every == 0:
			self._flush()

	def close(self) -> None:
		if self._closed:
			return
		self._closed = True
		self._flush()

	def _flush(self) -> None:
		if self.gif_path is None or not self.frames:
			return

		# Pad all frames to identical size (max_w, max_h)
		padded = []
		for img in self.frames:
			w, h = img.size
			canvas = Image.new(self.mode, (self.max_w, self.max_h), self.bg_color)

			if self.center:
				offset = ((self.max_w - w) // 2, (self.max_h - h) // 2)
			else:
				offset = (0, 0)

			canvas.paste(img, offset)
			padded.append(np.asarray(canvas, dtype=np.uint8))

		imageio.mimsave(self.gif_path, padded, fps=self.fps, loop=0)
