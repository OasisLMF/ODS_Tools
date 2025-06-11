import logging
import pandas as pd
logger = logging.getLogger(__name__)


class Validator:
    def __init__(self, validation):
        if validation is None:
            self.validation = None
        else:
            self.validation = [validation['input'], validation['output']]

    def run(self, data, stage=0):
        """Runs the validation

        Args:
            stage (int, optional): Which stage of the validation: 0 for input 1 for output. Defaults to 0.
            enable_logging (bool, optional): Whether to log info. Defaults to False.
        """
        if self.validation is None:
            return True
        run_dict = self.validation[stage]
        result = Results(stage)  # So it prints nicely
        for name, mapping in run_dict.items():
            result.results[name] = self.run_entry(data, ValidationEntry(mapping))

        return result

    def run_entry(self, data, item):
        """Runs a single entry from the validation spec

        Args:
            data (Dataframe): The data to be ran on
            item (ValidationEntry): The validation to be applied

        Raises:
            ValueError: When given an unsupported operator

        Returns:
            Dataframe: result of operation
        """
        valid_operators = {'sum', 'count', 'count_rows', 'count_unique'}
        if item.operator not in valid_operators:
            raise ValueError(f"Unsupported operator: {item.operator}")

        if item.operator == 'count_rows':
            return pd.DataFrame([{'Rows': len(data)}])
        if item.group_by:
            grouped = data.groupby(item.group_by)

            if item.operator == 'sum':
                result = grouped[item.fields].sum().reset_index()
            elif item.operator == 'count':
                result = grouped[item.fields].count().reset_index()
            elif item.operator == 'count_unique':
                result = grouped[item.fields].nunique().reset_index()
        else:
            if item.operator == 'sum':
                result = pd.DataFrame([data[item.fields].sum()], columns=item.fields)
            elif item.operator == 'count':
                result = pd.DataFrame([data[item.fields].count()], columns=item.fields)
            elif item.operator == 'count_unique':
                result = pd.DataFrame([data[item.fields].nunique()], columns=item.fields)

        return result


class ValidationEntry:
    def __init__(self, config):
        self.fields = config.get("fields", [])
        self.operator = config.get("operator", "sum")
        self.group_by = config.get("group_by", None)


class Results:
    def __init__(self, stage):
        self.results = {}
        self.stage = ["Input", "Output"][stage]

    def __eq__(self, other):
        if self.results.keys() != other.results.keys():
            return False
        for name in self.results:
            if not self.results[name].equals(other.results[name]):
                return False
        return True

    def __str__(self):

        res = [f"{self.stage} Validation Results:\n"]
        for name, result in self.results.items():
            res.append(f"  {name}:")
            result_string = result.to_string(index=False, float_format='{:,.2f}'.format)
            res.extend(['    ' + line for line in result_string.split('\n')])

        return '\n'.join(res) + '\n'
