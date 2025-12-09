# EnhancedCSV.jl

Julia reader for the [ECSV (Enhanced CSV)](https://github.com/astropy/astropy-APEs/blob/main/APE6.rst) format – a CSV variant with YAML-encoded metadata for column types, units, and descriptions.

ECSV was developed by the Astropy project and is gaining adoption in the astronomical community, including use by ESA's Gaia archive.

**Status**

Functionality:
- ✅ Reading
- 🚧 Writing

Format features supported:
- ✅ Scalar columns
- ✅ Variable-length 1D arrays
- 🚧 Higher-dimensional arrays
- ✅ Physical units (via Unitful.jl)
- 🚧 Table/column metadata

## Usage

```julia
using EnhancedCSV
using StructArrays

# Read an ECSV file into a StructArray:
tbl = EnhancedCSV.read(StructArray, "data.ecsv")
```

Any Tables.jl-compatible sink can be used (e.g., `StructArray`, or `columntable`/`rowtable` from `Tables.jl`).
