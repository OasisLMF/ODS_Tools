import yaml
from ods_tools.odtf.transformers.transform import parse
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
        self.validation = self.file.get('validation', None)

    def get_transform(self, available_columns):
        """Returns the valid transform from the mapping file
        Args:
            available_columns (List): List of columns in data set

        Returns:
            Mapping: Named tuple of transformation set, data types and null values
        """
        missing_columns = list(set(self.types.keys()) - set(available_columns))
        transformation_set = {}
        for transformation in self.transformations:
            col_name, col_transformation = transformation
            valid_transformations = []
            for individual_transform in col_transformation:
                individual_transform.parse()

                no_trans_tree = individual_transform.transformation_tree is None
                missing_trans_columns = self.has_missing_columns(individual_transform.transformation_tree, missing_columns)
                no_when_tree = individual_transform.when_tree is None
                missing_when_columns = self.has_missing_columns(individual_transform.when_tree, missing_columns)
                if (no_trans_tree or not missing_trans_columns):
                    if (no_when_tree or not missing_when_columns):
                        valid_transformations.append(individual_transform)
                    else:
                        raise ValueError(f"Missing dependency: {individual_transform.when} clause for {individual_transform.transformation}")

            if valid_transformations:
                transformation_set[col_name] = valid_transformations

        return Mapping(transformation_set, self.types, self.null_values)

    def make_transforms(self, transforms):
        """Converts given file's transform section into transformation entry objects

        Args:
            transforms (dict): Dictionary of transformations for all items
        """
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
        """Converts given file's types section into Column Conversion objects

        Args:
            types (dict): Dict of variables with their type
            null_values (set): All possible null values in data
        """
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
        self.transformation_tree = parse(self.transformation)
        self.when_tree = parse(self.when)


class ColumnConversion(NamedTuple):
    type: str
    nullable: bool = True
    null_values: Set = set()


class Mapping(NamedTuple):
    transformation_set: Set
    types: Dict[str, ColumnConversion]
    null_values: Set = set()
