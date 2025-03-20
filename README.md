# The DuckDB ReadStat Extension

Use this extension to read data sets from SAS, Stata, and SPSS from [DuckDB](<https://duckdb.org/>) with [ReadStat](<https://github.com/WizardMac/ReadStat?tab=readme-ov-file#readstat-read-and-write-data-sets-from-sas-stata-and-spss>).

## Installation & Loading

Installation is simple through the DuckDB Community Extension repository, just type

```
INSTALL read_stat FROM community;
LOAD read_stat;
```

in a DuckDB instance near you.

### The `read_stat` Function
The extension adds a single DuckDB table function, `read_stat`, which you use as follows:

```SQL
-- Read a SAS `.sas7bdat` or `.xpt` file
FROM read_stat('sas_data.sas7bdat');
FROM read_stat('sas_data.xpt');
-- Read an SPSS `.sav`, `.zsav`, or `.por` file
FROM read_stat('spss_data.sav');
FROM read_stat('compressed_spss_data.zsav');
FROM read_stat('portable_spss_data.por');
-- Read a Stata .dta file
FROM read_stat('stata_data.dta');
```

If the file extension is not `.sas7bdat`, `.xpt`, `.sav`, `.zsav`, `.por`, or `.dta`,
use the `read_stat` function for the right file type with the `format` parameter:

```SQL
FROM read_stat('sas_data.other_extension', format = 'sas7bdat');
FROM read_stat('sas_data.other_extension', format = 'xpt');
-- SPSS `.sav` and `.zsav` can both be read through the format `'sav'`
FROM read_stat(
    'spss_data_possibly_compressed.other_extension',
    format = 'sav'
);
FROM read_stat('portable_spss_data.other_extension', format = 'por');
FROM read_stat('stata_data.other_extension', format = 'dta');
```

Override the file character `encoding` inferred from the file with an `iconv` encoding name, see <https://www.gnu.org/software/libiconv/>:

```SQL
FROM read_stat('latin1_encoded.sas7bdat', encoding = 'iso-8859-1');
```

If your files have the proper file extensions and you do not need to override their character encodings, a [replacement scan](<https://duckdb.org/docs/stable/guides/glossary.html#replacement-scan>) is also available:

```SQL
-- Read a SAS `.sas7bdat` or `.xpt` file
FROM 'sas_data.sas7bdat';
FROM 'sas_data.xpt';
-- Read an SPSS `.sav`, `.zsav`, or `.por` file
FROM 'spss_data.sav';
FROM 'compressed_spss_data.zsav';
FROM 'portable_spss_data.por';
-- Read a Stata .dta file
FROM 'stata_data.dta';
```

## Contributing

### Cloning
Clone the repo with submodules

```shell
git clone --recurse-submodules <repo>
```

### Dependencies

In principle, compiling this template only requires a C/C++ toolchain. However, this template relies on some additional
tooling to make life a little easier and to be able to share CI/CD infrastructure with extension templates for other languages:

- Python3
- Python3-venv
- [Make](https://www.gnu.org/software/make)
- CMake
- Git
- (Optional) Ninja + ccache
- vcpkg

Installing these dependencies will vary per platform:
- For Linux, these come generally pre-installed or are available through the distro-specific package manager.
- For MacOS, [homebrew](https://formulae.brew.sh/).
- For Windows, [chocolatey](https://community.chocolatey.org/).

### Building
After installing the dependencies, building is a two-step process. Firstly run:
```shell
make configure
```
This will ensure a Python venv is set up with DuckDB and DuckDB's test runner installed. Additionally, depending on configuration,
DuckDB will be used to determine the correct platform for which you are compiling.

Then, to build the extension run:
```shell
make debug
```
This delegates the build process to cargo, which will produce a shared library in `target/debug/<shared_lib_name>`. After this step, 
a script is run to transform the shared library into a loadable extension by appending a binary footer. The resulting extension is written
to the `build/debug` directory.

To create optimized release binaries, simply run `make release` instead.

#### Faster builds
We recommend to install Ninja and Ccache for building as this can have a significant speed boost during development. After installing, ninja can be used 
by running:
```shell
make clean
GEN=ninja make debug
```

### Testing
This extension uses the DuckDB Python client for testing. This should be automatically installed in the `make configure` step.
The tests themselves are written in the SQLLogicTest format, just like most of DuckDB's tests. A sample test can be found in
`test/sql/<extension_name>.test`. To run the tests using the *debug* build:

```shell
make test_debug
```

or for the *release* build:
```shell
make test_release
```

#### Version switching
Testing with different DuckDB versions is really simple:

First, run 
```shell
make clean_all
```
to ensure the previous `make configure` step is deleted.

Then, run

```shell
DUCKDB_TEST_VERSION=v1.1.2 make configure
```
to select a different duckdb version to test with

Finally, build and test with

```shell
make debug
make test_debug
```

#### Using unstable Extension C API functionality

The DuckDB Extension C API has a stable part and an unstable part. By default, this template only allows usage of the stable
part of the API. To switch it to allow using the unstable part, take the following steps:

Firstly, set your `TARGET_DUCKDB_VERSION` to your desired in `./Makefile`. Then, run `make update_duckdb_headers` to ensure 
the headers in `./duckdb_capi` are set to the correct version. (FIXME: this is not yet working properly). 

Finally, set `USE_UNSTABLE_C_API` to 1 in `./Makefile`. That's all!