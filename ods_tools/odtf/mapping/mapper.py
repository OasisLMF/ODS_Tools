import yaml
from ..transformers.transform import parse
from lark import Token
from typing import NamedTuple
from typing import Dict, Set


class Mapper:
    def __init__(self, filepath):
        with open(filepath, 'r') as file:
            self.file = yaml.safe_load(file)
        self.make_transforms(self.file['transform'])
        self.null_values = self.file['null_values']
        self.make_types(self.file['types'], self.null_values)

    def get_transform(self, available_columns):
        missing_columns = list(set(self.types.keys()) - set(available_columns))
        transformation_set = {}
        for transformation in self.transformations:
            col_name, col_transformation = transformation
            valid_transformations = []
            for individual_transform in col_transformation:
                individual_transform.parse()
                if (
                    (
                        individual_transform.transformation_tree is None or
                        not self.has_missing_columns(
                            individual_transform.transformation_tree,
                            missing_columns
                        )
                    ) and (
                        individual_transform.when_tree is None or
                        not self.has_missing_columns(
                            individual_transform.when_tree,
                            missing_columns
                        )
                    )
                ):
                    valid_transformations.append(individual_transform)
            if valid_transformations:
                transformation_set[col_name] = valid_transformations

        return Mapping(transformation_set, self.types, self.null_values)

    def make_transforms(self, transforms):
        self.transformations = []
        for transform in transforms.items():
            name, individual_transforms = transform
            individual_transform_objects = []
            for individual_transform in individual_transforms:
                individual_transform_objects.append(TransformationEntry(
                    transformation=individual_transform['transformation'],
                    when=individual_transform.get('when', 'True')))
            self.transformations.append((name, individual_transform_objects))

    def make_types(self, types, null_values):
        self.types = {}
        for name, options in types.items():
            self.types[name] = ColumnConversion(options['type'], options.get('nullable', True), null_values)

    @staticmethod
    def has_missing_columns(node, missing_columns):
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
        if node.data == 'lookup' and Mapper.has_missing_columns(node.children[0], missing_columns):
            return True
        return any(Mapper.has_missing_columns(child, missing_columns) for child in node.children)


class TransformationEntry:
    def __init__(self, transformation, transformation_tree=None, when="True", when_tree=None):
        self.transformation = transformation
        self.transformation_tree = transformation_tree
        self.when = when
        self.when_tree = when_tree

    def __eq__(self, other):
        return self.transformation == other.transformation and self.when == other.when

    def parse(self):
        self.when_tree = parse(self.when)


class ColumnConversion(NamedTuple):
    type: str
    nullable: bool = True
    null_values: Set = set()


class Mapping(NamedTuple):
    transformation_set: Set
    types: Dict[str, ColumnConversion]
    null_values: Set = set()
