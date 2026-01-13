import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import imageio.v2 as imageio
from io import BytesIO
from typing import Dict, Tuple, Optional, Any

from IPython.display import clear_output as _clear_output

from pybeamline.stream.base_sink import BaseSink

Counts = Dict[Tuple[Any, Any], float]

class heatmap_sink(BaseSink[Counts]):
    def __init__(
        self,
        title: str = "Distribution",
        value_label: str = "frequency",
        gif_path: Optional[str] = None,
        fps: int = 5,
        write_every: Optional[int] = None,   # e.g. 20; None = only on close
        figsize=(5, 4),
        dpi: int = 120,
    ):
        self.title = title
        self.value_label = value_label
        self.gif_path = gif_path
        self.fps = fps
        self.write_every = write_every
        self.figsize = figsize
        self.dpi = dpi

        self.frames = []
        self._count = 0
        self._closed = False

        if self.gif_path:
            matplotlib.use("Agg", force=True)

    def consume(self, counts: Counts) -> None:
        if self._closed:
            raise RuntimeError("HeatmapSink.consume() called after close().")

        nodes = sorted({k1 for (k1, k2) in counts} | {k2 for (k1, k2) in counts})
        n = len(nodes)
        index = {node: i for i, node in enumerate(nodes)}

        mat = np.zeros((n, n), dtype=float)
        for (a, b), freq in counts.items():
            mat[index[a], index[b]] = freq

        fig, ax = plt.subplots(figsize=self.figsize, dpi=self.dpi)
        im = ax.imshow(mat, cmap="Blues", interpolation="nearest")
        cbar = fig.colorbar(im, ax=ax)
        cbar.set_label(self.value_label)

        ax.set_xticks(range(n))
        ax.set_yticks(range(n))
        ax.set_xticklabels(nodes, rotation=45, ha="right")
        ax.set_yticklabels(nodes)
        ax.set_title(self.title)
        fig.tight_layout()

        if self.gif_path:
            buf = BytesIO()
            fig.savefig(buf, format="png")  # keep fixed size; avoid bbox_inches="tight"
            buf.seek(0)
            self.frames.append(imageio.imread(buf))
            plt.close(fig)

            self._count += 1
            if self.write_every and self._count % self.write_every == 0:
                self._flush()
        else:
            plt.show()
            plt.close(fig)
        print(".",end="")

    def close(self) -> None:
        print("done")
        if self._closed:
            return
        self._closed = True
        self._flush()

    def _flush(self) -> None:
        if self.gif_path and self.frames:
            imageio.mimsave(self.gif_path, self.frames, fps=self.fps, loop=0)
