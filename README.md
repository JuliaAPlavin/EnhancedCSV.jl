# EnhancedCSV.jl

Julia reader for the [ECSV (Enhanced CSV)](https://github.com/astropy/astropy-APEs/blob/main/APE6.rst) format – a CSV variant with YAML-encoded metadata for column types, units, and descriptions.

ECSV was developed by the Astropy project and is gaining adoption in the astronomical community, including use by ESA's Gaia archive.

## Usage

```julia
using EnhancedCSV
using StructArrays

# Read an ECSV file into a StructArray:
tbl = EnhancedCSV.read(StructArray, "data.ecsv")
```

Any Tables.jl-compatible sink can be used (e.g., `StructArray`, or `columntable`/`rowtable` from `Tables.jl`).

## Status

Builds on [QuackIO.jl](https://github.com/JuliaAPlavin/QuackIO.jl) ([DuckDB](https://duckdb.org/)) and [YAML.jl](https://github.com/JuliaData/YAML.jl) for csv and yaml, [VOUnits.jl](https://github.com/JuliaAPlavin/VOUnits.jl) for unit-string conversion, [StructArrays.jl](https://github.com/JuliaArrays/StructArrays.jl) for performant Julia-native tables.

Functionality:
- ✅ Reading
  - ✅ Decompress on the fly
  - 🚧 Lazy, filter, ...
- ✅ Writing
  - 🚧 Compress on the fly

Format features supported:
- ✅ Scalar columns
- ✅ Arrays: fixed or variable-length, 1- and n-dimensional
- ✅ Physical units via Unitful.jl
- 🚧 Table/column metadata