from typing import Optional
from pybeamline.models.pdfa.pdfa import Pdfa

class WeightsNormalizer:

    @staticmethod
    def normalize(pdfa: Pdfa, alpha: float) -> Pdfa:

        new_pdfa = pdfa.deepcopy()
        new_pdfa.weight_factor = alpha

        nodes = list(new_pdfa.nodes)
        num_nodes = len(nodes)
        if num_nodes == 0:
            return new_pdfa

        ratio = 1.0 / num_nodes

        for source in nodes:
            for target, old_prob in new_pdfa.get_outgoing_edges(source).items():
                new_pdfa.edges[source][target] = alpha * old_prob + (1 - alpha) * ratio

        for source in nodes:
            for target in nodes:
                if target not in new_pdfa.edges[source]:
                    new_pdfa.add_edge(source, target, (1 - alpha) * ratio)

        return new_pdfa
