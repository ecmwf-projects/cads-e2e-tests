# cads-e2e-tests

CADS E2E Tests

## Quick Start

### Explore all available options:

```
cads-e2e-tests --help
```

### Test one random request per dataset:

```
cads-e2e-tests --reports-path random_reports.jsonl
```

### Specify which requests to test:

```yaml
# requests.yaml

# Example 1:
- collection_id: reanalysis-era5-single-levels
  parameters:
    # optional parameters (random request if no parameters are provided)
    variable: "2t"
    product_type: "reanalysis"
    date: "2012-12-01"
    time: "12:00"
  checks:
    # optional checks (remove any check to disable)
    checksum: 01683b3d69dec4c7221e524e3f6697dd  # file md5 hash
    extension: .grib  # file extension
    size: 2076588  # file size in Bytes
    # checks that do not require downloading the results
    time: 60  # maximum running time to generate results in seconds
    content_length: 2076588  # file size in Bytes inferred from metadata
    content_type: application/x-grib  # file type inferred from metadata

# Example 2:
- collection_id: test-adaptor-dummy
  parameters:
    size: 0
  checks:
    size: 1  # wrong file size
```

```
cads-e2e-tests --requests-path requests.yaml --reports-path example_reports.jsonl
```

## Workflow for developers/contributors

For best experience create a new conda environment (e.g. DEVELOP) with Python 3.11:

```
conda create -n DEVELOP -c conda-forge python=3.11
conda activate DEVELOP
```

Before pushing to GitHub, run the following commands:

1. Update conda environment: `make conda-env-update`
1. Install this package: `pip install -e .`
1. Sync with the latest [template](https://github.com/ecmwf-projects/cookiecutter-conda-package) (optional): `make template-update`
1. Run quality assurance checks: `make qa`
1. Run tests: `make unit-tests`
1. Run the static type checker: `make type-check`
1. Build the documentation (see [Sphinx tutorial](https://www.sphinx-doc.org/en/master/tutorial/)): `make docs-build`

## License

```
Copyright 2024, European Union.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
