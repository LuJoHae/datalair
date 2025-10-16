# <img src="docs/assets/datalair-icon.ico" alt="logo" height="24em" style="vertical-align:middle;"> Datalair

**Datalair** is a lightweight Python library designed to streamline
the management, processing, and retrieval of data for research or projects.
It provides utilities for efficient downloading, handling, and storage
of datasets.

## Features

- **Data Downloading**: 
  - Utilities for downloading files or datasets from popular repositories such as GEO and ArrayExpress.
  - Robust handling for managing large or supplementary files with easy integration into workflows.

- **Developer-Friendly**:
  - Type safety and linting support for robust development with tools like `mypy` and `ruff`.
  - Built-in testing capabilities powered by `pytest`.

---

## Installation

Ensure you have Python 3.12 or newer before installing.

```shell script
poetry install https://github.com/LuJoHae/datalair.git
```

---

## Quickstart

Here’s a quick example of how to use Datalair to download a dataset:

```python
from datalair import Lair, Dataset

lair = Lair("./lair")
lair.create_if_not_exist()
lair.assert_ok_satus()

class MyDataset(Dataset):

    def derive(self, lair: "Lair") -> None:
        output_dir = lair.get_path(self)
        
        with open(output_dir.joinpath("myfile.txt"), "w") as f:
            f.write("Hello World")

dataset = MyDataset()
lair.safe_derive(dataset)
filepaths = lair.get_dataset_filepaths(dataset)
```

Check the module-specific functions like `download_supplementary_from_geo` or
`download_files_from_arrayexpress` for dataset-related tasks.
