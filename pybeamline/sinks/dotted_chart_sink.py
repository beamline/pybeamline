from pybeamline.bevent import BEvent
import numpy as np
import matplotlib.pyplot as plt
from IPython.display import clear_output
from datetime import timedelta
from matplotlib.cm import get_cmap
import imageio.v2 as imageio
from io import BytesIO
from PIL import Image

class dotted_chart_sink:

    def __init__(
        self,
        title="Dotted chart of events",
        max_events=None,
        time_window_seconds=None,
        point_size=100,
        cmap_name="tab20",
        show_legend=False,
        gif_path=None,
        fps=5
    ):
        self.title = title
        self.max_events = max_events
        self.time_window_seconds = time_window_seconds
        self.point_size = point_size
        self.show_legend = show_legend
        self.events = []
        self.cmap = get_cmap(cmap_name)
        self.act_to_color = {}
        self._next_color_idx = 0
        self.gif_path = gif_path
        self.fps = fps
        self.frames = []
        self.max_w = 0
        self.max_h = 0


    def __call__(self, event: BEvent):
        self.events.append(event)
        
        act = event.get_event_name()
        if act not in self.act_to_color:
            n_colors = self.cmap.N  # for tab20, this is 20
            # spread indices over the available colors; reuse after n_colors
            norm_idx = (self._next_color_idx % n_colors) / max(1, n_colors - 1)
            self.act_to_color[act] = self.cmap(norm_idx)
            self._next_color_idx += 1
        
        if self.time_window_seconds is not None:
            cutoff = event.get_event_time() - timedelta(seconds=self.time_window_seconds)
            self.events = [e for e in self.events if e.get_event_time() >= cutoff]

        if self.max_events is not None and len(self.events) > self.max_events:
            self.events = self.events[-self.max_events:]
        
        self._draw()


    def _draw(self):
        if not self.events:
            return

        clear_output(wait=True)

        # Convert to numpy arrays
        events = self.events

        # Extract case ids, timestamps, and activities
        case_ids = np.array([e.get_trace_name() for e in events])
        times = np.array([e.get_event_time() for e in events])
        activities = np.array([e.get_event_name() for e in events])

        # Use first timestamp as zero
        t0 = times[0]
        x = np.array([(t - t0).total_seconds() for t in times], dtype=float)

        # Unique case IDs for Y-axis, but keep original order
        unique_cases = list(dict.fromkeys(case_ids))
        y_positions = {cid: i for i, cid in enumerate(unique_cases)}
        y = np.array([y_positions[cid] for cid in case_ids], dtype=float)

        # colors from persistent mapping
        colors = np.array([self.act_to_color[a] for a in activities])

        # Plot
        plt.figure(figsize=(10, max(3, len(unique_cases) * 0.4)))
        
        for cid in unique_cases:
            idx = np.where(case_ids == cid)[0]
            xs = x[idx]
            y_val = y_positions[cid]
            
            # Sort by x (timestamps)
            xs_sorted = np.sort(xs)
            # Horizontal line across all events of this case
            plt.plot(xs_sorted, [y_val] * len(xs_sorted), color="black", linewidth=1, zorder=1)


        plt.scatter(x, y, s=self.point_size, c=colors, edgecolors="black")

        # Label y-axis with actual case ids
        plt.yticks(list(y_positions.values()), list(y_positions.keys()))

        plt.xlabel("Time since first event (seconds)")
        plt.ylabel("Case ID")
        plt.title(self.title)
        plt.grid(True, linestyle=":", linewidth=0.5)

        # Build legend
        if self.show_legend:
            visible_acts = sorted(set(activities))  # alphabetically in legend
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
            plt.legend(
                handles=handles,
                title="Activities",
                loc="upper center",
                bbox_to_anchor=(0.5, -0.3),
                ncol=5,
                frameon=False,
            )

        plt.tight_layout()

        if self.gif_path:
            buf = BytesIO()
            plt.savefig(buf, format="png", bbox_inches="tight")
            buf.seek(0)

            # Open as PIL image, convert to RGB (no alpha)
            img = Image.open(buf).convert("RGB")
            w, h = img.size

            # Update global max size
            if w > self.max_w:
                self.max_w = w
            if h > self.max_h:
                self.max_h = h

            # Store original-size image
            self.frames.append(img)

            # Build padded frames with size (max_w, max_h)
            padded_frames = []
            for im in self.frames:
                iw, ih = im.size
                # white background canvas (or any color you prefer)
                canvas = Image.new("RGB", (self.max_w, self.max_h), (255, 255, 255))

                # center image on canvas
                offset = ((self.max_w - iw) // 2, (self.max_h - ih) // 2)
                canvas.paste(im, offset)

                padded_frames.append(np.array(canvas, dtype=np.uint8))

            imageio.mimsave(self.gif_path, padded_frames, fps=self.fps, loop=0)

        plt.show()
