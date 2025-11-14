import matplotlib.pyplot as plt
from IPython.display import clear_output
import numpy as np
import imageio.v2 as imageio
from io import BytesIO

class heatmap_sink:

    def __init__(self, title="Distribution", value_label="frequency", gif_path=None, fps=5):
        self.title = title
        self.value_label = value_label
        self.gif_path = gif_path
        self.fps = fps
        self.frames = []

    def __call__(self, counts):
        nodes = sorted({k1 for (k1, k2) in counts} | {k2 for (k1, k2) in counts})
        n = len(nodes)
        index = {node: i for i, node in enumerate(nodes)}

        mat = np.zeros((n, n))
        for (a, b), freq in counts.items():
            mat[index[a], index[b]] = freq

        clear_output(wait=True)
        plt.figure(figsize=(5, 4))
        plt.imshow(mat, cmap="Blues", interpolation="nearest")
        plt.colorbar(label=self.value_label)
        plt.xticks(range(n), nodes, rotation=45)
        plt.yticks(range(n), nodes)
        plt.title(self.title)
        plt.tight_layout()

        if self.gif_path:
            buf = BytesIO()
            plt.savefig(buf, format="png", bbox_inches="tight")
            buf.seek(0)
            self.frames.append(imageio.imread(buf))
            imageio.mimsave(self.gif_path, self.frames, fps=self.fps, loop=0)

        plt.show()
