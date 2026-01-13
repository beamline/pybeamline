from typing import Optional
from pybeamline.bevent import BEvent
import numpy as np
import matplotlib.pyplot as plt
from datetime import timedelta
from matplotlib.cm import get_cmap
import imageio.v2 as imageio
from io import BytesIO
from PIL import Image

from pybeamline.stream.base_sink import BaseSink

try:
	from IPython.display import clear_output as _clear_output
except Exception:
	_clear_output = None


class dotted_chart_sink(BaseSink[BEvent]):
	def __init__(
			self,
			title: str = "Dotted chart of events",
			max_events: Optional[int] = None,
			time_window_seconds: Optional[float] = None,
			point_size: float = 100,
			cmap_name: str = "tab20",
			show_legend: bool = False,
			gif_path: Optional[str] = None,
			fps: int = 5,
			write_every: Optional[int] = None,  # e.g. 20; None = only on close
			figsize=(10, 4),
			dpi: int = 120,
			bg_color=(255, 255, 255),
			center: bool = True,
			display_in_notebook: bool = True,
	):
		self.title = title
		self.max_events = max_events
		self.time_window_seconds = time_window_seconds
		self.point_size = point_size
		self.show_legend = show_legend

		self.cmap = get_cmap(cmap_name)
		self.act_to_color = {}
		self._next_color_idx = 0

		self.gif_path = gif_path
		self.fps = fps
		self.write_every = write_every

		self.figsize = figsize
		self.dpi = dpi
		self.bg_color = bg_color
		self.center = center
		self.display_in_notebook = display_in_notebook

		self.events = []  # rolling window / max buffer of BEvent
		self.frames = []  # PIL images
		self.max_w = 0
		self.max_h = 0

		self._count = 0
		self._closed = False

	def consume(self, event: BEvent) -> None:
		if self._closed:
			raise RuntimeError("DottedChartSink.consume() called after close().")

		self.events.append(event)

		# stable activity -> color mapping
		act = event.get_event_name()
		if act not in self.act_to_color:
			n_colors = self.cmap.N
			norm_idx = (self._next_color_idx % n_colors) / max(1, n_colors - 1)
			self.act_to_color[act] = self.cmap(norm_idx)
			self._next_color_idx += 1

		# apply time window
		if self.time_window_seconds is not None:
			cutoff = event.get_event_time() - timedelta(seconds=self.time_window_seconds)
			self.events = [e for e in self.events if e.get_event_time() >= cutoff]

		# apply max events
		if self.max_events is not None and len(self.events) > self.max_events:
			self.events = self.events[-self.max_events:]

		# render either to notebook or to frame buffer (or both)
		self._draw_and_maybe_capture()

		self._count += 1
		if self.gif_path and self.write_every and self._count % self.write_every == 0:
			self._flush()

	def close(self) -> None:
		if self._closed:
			return
		self._closed = True
		self._flush()

	def _draw_and_maybe_capture(self) -> None:
		if not self.events:
			return

		if self.display_in_notebook and _clear_output is not None:
			_clear_output(wait=True)

		events = self.events

		case_ids = np.array([e.get_trace_name() for e in events])
		times = np.array([e.get_event_time() for e in events])
		activities = np.array([e.get_event_name() for e in events])

		t0 = times[0]
		x = np.array([(t - t0).total_seconds() for t in times], dtype=float)

		unique_cases = list(dict.fromkeys(case_ids))
		y_positions = {cid: i for i, cid in enumerate(unique_cases)}
		y = np.array([y_positions[cid] for cid in case_ids], dtype=float)

		colors = np.array([self.act_to_color[a] for a in activities])

		# build figure (fixed dpi; height depends on cases unless you choose to fix it too)
		height = max(self.figsize[1], max(3.0, len(unique_cases) * 0.4))
		fig, ax = plt.subplots(figsize=(self.figsize[0], height), dpi=self.dpi)

		# horizontal lines per case
		for cid in unique_cases:
			idx = np.where(case_ids == cid)[0]
			xs_sorted = np.sort(x[idx])
			y_val = y_positions[cid]
			ax.plot(xs_sorted, [y_val] * len(xs_sorted), color="black", linewidth=1, zorder=1)

		ax.scatter(x, y, s=self.point_size, c=colors, edgecolors="black")

		ax.set_yticks(list(y_positions.values()))
		ax.set_yticklabels(list(y_positions.keys()))
		ax.set_xlabel("Time since first event (seconds)")
		ax.set_ylabel("Case ID")
		ax.set_title(self.title)
		ax.grid(True, linestyle=":", linewidth=0.5)

		if self.show_legend:
			visible_acts = sorted(set(activities))
			handles = [
				plt.Line2D(
					[0], [0],
					marker="o",
					color="w",
					label=act,
					markerfacecolor=self.act_to_color[act],
					markersize=8,
				)
				for act in visible_acts
			]
			ax.legend(
				handles=handles,
				title="Activities",
				loc="upper center",
				bbox_to_anchor=(0.5, -0.3),
				ncol=5,
				frameon=False,
			)

		fig.tight_layout()

		if self.gif_path:
			# Capture a frame (NO bbox_inches="tight" to avoid size jitter)
			buf = BytesIO()
			fig.savefig(buf, format="png")
			buf.seek(0)

			img = Image.open(buf).convert("RGB")
			w, h = img.size
			self.max_w = max(self.max_w, w)
			self.max_h = max(self.max_h, h)
			self.frames.append(img)

		# show in notebook if desired
		if self.display_in_notebook and self.gif_path is None:
			plt.show()

		plt.close(fig)

	def _flush(self) -> None:
		if not self.gif_path or not self.frames:
			return

		padded_frames = []
		for im in self.frames:
			iw, ih = im.size
			canvas = Image.new("RGB", (self.max_w, self.max_h), self.bg_color)

			if self.center:
				offset = ((self.max_w - iw) // 2, (self.max_h - ih) // 2)
			else:
				offset = (0, 0)

			canvas.paste(im, offset)
			padded_frames.append(np.asarray(canvas, dtype=np.uint8))

		imageio.mimsave(self.gif_path, padded_frames, fps=self.fps, loop=0)
