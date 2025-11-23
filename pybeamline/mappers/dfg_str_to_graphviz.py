from typing import Optional, List

from pybeamline.stream.base_map import BaseMap, T, R
from pybeamline.utils.dfg_to_graphviz import dfg_to_graphviz


def dfg_str_to_graphviz() -> BaseMap:
    return DfgStrToGraphviz()

class DfgStrToGraphviz(BaseMap):

    def transform(self, value: T) -> Optional[List[R]]:
        return [dfg_to_graphviz(value[1])]