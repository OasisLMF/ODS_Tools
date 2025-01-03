import logging
from typing import (
    Dict,
    Iterable,
    List,
    NamedTuple,
    Optional,
    Reversible,
    Set,
    Union,
)

import networkx as nx
from lark import Tree, Token

from ..config import TransformationConfig
from .errors import NoConversionPathError
from ..transformers.transform import parse


logger = logging.getLogger(__name__)


class TransformationEntry:
    def __init__(
        self,
        transformation: str,
        transformation_tree: Union[Tree, None] = None,
        when: str = "True",
        when_tree: Union[Tree, None] = None,
    ):
        self.transformation = transformation
        self.transformation_tree = transformation_tree
        self.when = when
        self.when_tree = when_tree

    def __eq__(self, other):
        return (
            self.transformation == other.transformation
            and self.when == other.when
        )

    def parse(self):
        self.when_tree = parse(self.when)
        self.transformation_tree = parse(self.transformation)


TransformationSet = Dict[str, List[TransformationEntry]]


class ColumnConversion(NamedTuple):
    type: str
    nullable: bool = True
    null_values: Set = set()


ColumnConversions = Dict[str, ColumnConversion]


class MappingFormat(NamedTuple):
    name: str
    version: str


class DirectionalMapping(NamedTuple):
    input_format: MappingFormat
    output_format: MappingFormat
    transformation_set: TransformationSet
    types: Dict[str, ColumnConversion] = dict()
    null_values: Set = set()


class MappingSpec:
    """
    Class holding information about a given mapping
    """

    def __init__(
        self,
        file_type: str,
        input_format: MappingFormat,
        output_format: MappingFormat,
        forward: DirectionalMapping = None,
        reverse: DirectionalMapping = None,
        metadata: Dict = None,
    ):
        self.file_type = file_type
        self.input_format = input_format
        self.output_format = output_format
        self.forward = forward
        self.reverse = reverse
        self.metadata = metadata or {}

    @property
    def can_run_forwards(self):
        """
        Flag whether the mapping file can be applied forwards.

        :return: True is the mapping can be applied forwards, False otherwise
        """
        return (
            self.forward is not None
            and len(self.forward.transformation_set) > 0
        )

    @property
    def can_run_in_reverse(self):
        """
        Flag whether the mapping file can be applied in reverse.

        :return: True is the mapping can be applied in reverse, False otherwise
        """
        return (
            self.reverse is not None
            and len(self.reverse.transformation_set) > 0
        )


class BaseMapping:
    """
    Class describing the mapping from the input to the
    output formats.

    :param config: The global config for the system
    :param input: The start of the conversion path
    :param output: The end of the conversion path
    """

    def __init__(
        self,
        config: TransformationConfig,
        file_type: str,
        **options,
    ):
        self._mapping_graph: Optional[Dict[str, nx.DiGraph]] = None
        self._path = None

        self.config = config
        default_format_dict = {"name": None, "version": None}
        self.input_format = MappingFormat(
            **self.config.get("input_format", default_format_dict)
        )
        self.output_format = MappingFormat(
            **self.config.get("output_format", default_format_dict)
        )

        self._options = {
            "file_type": file_type,
            "input_format": self.input_format,
            "output_format": self.output_format,
            **options,
        }

        self.file_type = file_type

    @property
    def mapping_specs(self) -> Reversible[MappingSpec]:
        """
        Returns a list of ``MappingSpec`` objects described by the mapping
        """
        raise NotImplementedError()

    def _build_mapping_graph(self) -> nx.DiGraph:
        """
        Creates a networkx graph to represent the relationships between
        formats in the system.

        :return: The built graph
        """
        g = nx.DiGraph()

        # the mapping config is in order from first search path to last
        # if we build it in reverse order we will store the most preferable
        # mapping on each edge
        if self.file_type:
            specs: Iterable[MappingSpec] = filter(
                lambda s: s.file_type.lower() == self.file_type.lower(),
                reversed(self.mapping_specs),
            )
        else:
            specs = reversed(self.mapping_specs)

        for mapping in specs:
            if mapping.can_run_forwards:
                g.add_edge(
                    mapping.input_format,
                    mapping.output_format,
                    transform=mapping.forward,
                    spec=mapping,
                )

            if mapping.can_run_in_reverse:
                g.add_edge(
                    mapping.output_format,
                    mapping.input_format,
                    transform=mapping.reverse,
                    spec=mapping,
                )

        return g

    @property
    def mapping_graph(self) -> nx.DiGraph:
        """
        Creates the graph to represent the relationships between formats in
        the system. It it has not already been generated it is generated here.
        """
        if self._mapping_graph is None:
            self._mapping_graph = self._build_mapping_graph()

        return self._mapping_graph

    @property
    def path(self):
        if self._path:
            return self._path

        try:
            self._path = nx.shortest_path(
                self.mapping_graph,
                self.input_format,
                self.output_format,
            )
            return self._path
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            raise NoConversionPathError(self.input_format, self.output_format)

    @property
    def path_edges(self):
        return list(
            map(
                lambda in_out: self.mapping_graph[in_out[0]][in_out[1]],
                zip(self.path[:-1], self.path[1:]),
            )
        )

    @staticmethod
    def has_missing_columns(node: Union[Tree, Token, int, None], missing_columns: List[str]) -> bool:
        """
        Recursively checks if a node in the transformation tree contains any missing columns.

        Args:
            node (Union[Tree, Token, int, None]): The node to check for missing columns.
            missing_columns (List[str]): A list of missing column names.

        Returns:
            bool: True if the node contains any missing columns, False otherwise.
        """
        if node is None or isinstance(node, int):
            return False
        if isinstance(node, Token):
            return node.type == 'IDENT' and node.value in missing_columns
        if node.data == 'lookup' and BaseMapping.has_missing_columns(node.children[0], missing_columns):
            return True
        return any(BaseMapping.has_missing_columns(child, missing_columns) for child in node.children)

    def get_transformations(self, available_columns: List[str]) -> List[DirectionalMapping]:
        """
        Gets a column transformations and full transformation set for the
        provided input and output paths, filtered based on the available columns.

        :param available_columns: List of available columns in the input data
        :return: The filtered mappings along the conversion path.
        """
        path = self.path
        logger.info(
            f"Path found {' -> '.join(f'{n.name} v{n.version}' for n in path)}"
        )
        transformations = [edge["transform"] for edge in self.path_edges]

        missing_columns = list(set(transformations[0].types.keys()) - set(available_columns))

        doable_transformations = []
        for mapping in transformations:
            doable_transformation_set = {}
            for col, transform_list in mapping.transformation_set.items():
                valid_transforms = []
                for transform in transform_list:
                    transform.parse()
                    if ((transform.transformation_tree is None or
                         not self.has_missing_columns(transform.transformation_tree, missing_columns)) and
                        (transform.when_tree is None or
                         not self.has_missing_columns(transform.when_tree, missing_columns))):
                        valid_transforms.append(transform)
                if valid_transforms:
                    doable_transformation_set[col] = valid_transforms
            if doable_transformation_set:
                doable_transformations.append(
                    DirectionalMapping(
                        input_format=mapping.input_format,
                        output_format=mapping.output_format,
                        transformation_set=doable_transformation_set,
                        types=mapping.types,
                        null_values=mapping.null_values,
                    )
                )

        return doable_transformations
