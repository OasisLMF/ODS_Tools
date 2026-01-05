from pathlib import Path
import tempfile
import json

from ods_tools.combine.combine import combine


example_path = Path(Path(__file__).parent.parent, "ods_tools", "combine", "examples")


def test_combine_as_expected():

    input_dir = example_path / "inputs"

    with tempfile.TemporaryDirectory() as tmp_dir:
        config_path = Path(tmp_dir, "config.json")

        with open(config_path, "w") as f:
            json.dump({
                "analysis_dirs": [str(child) for child in input_dir.iterdir()]
            }, f)

        with open(config_path, "r") as f:
            config = json.load(f)

        print(config)

    combine_result = combine(str(config_path))
