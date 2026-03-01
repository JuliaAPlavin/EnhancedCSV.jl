module EnhancedCSV

using QuackIO
using YAML
import JSON
using Unitful
import VOUnits
using Tables: columntable

export read, write

# Mapping from ECSV datatype names to Julia types
const DATATYPE_MAP = Dict{String,Type}(
    "bool" => Bool,
    "int8" => Int8,
    "int16" => Int16,
    "int32" => Int32,
    "int64" => Int64,
    "uint8" => UInt8,
    "uint16" => UInt16,
    "uint32" => UInt32,
    "uint64" => UInt64,
    "float16" => Float16,
    "float32" => Float32,
    "float64" => Float64,
    "string" => String,
)

const REVERSE_DATATYPE_MAP = Dict{Type,String}(v => k for (k, v) in DATATYPE_MAP)

"""
    read(sink, source; kw...)

Read an ECSV (Enhanced CSV) file and return data in the specified sink format.

The ECSV format consists of:
- A YAML header with column metadata (lines starting with `# `)
- A CSV data section

# Arguments
- `sink`: The type to materialize the data into (e.g., `StructArray`)
- `source`: File path or IO object
- `kw...`: Additional keyword arguments passed to `QuackIO.read_csv`
"""
function read end

function read(sink, source::AbstractString; kw...)
    header = parse_ecsv_header(source)
    _read_body(sink, source, header; kw...)
end

function _read_body(sink, source, header; kw...)
    colspecs = NamedTuple(Symbol(d["name"]) => ColumnSpec(d) for d in header["datatype"])

    delim = string(get(header, "delimiter", " "))
    tbl = read_csv(columntable, source; comment="#", delim, kw...)

    @assert propertynames(tbl) == values(map(c -> c.name, colspecs))

    tbl = map(tbl, colspecs) do col, spec
        convert_column(col, spec)
    end
    return sink(tbl)
end

"""
    parse_ecsv_header(source)

Parse the ECSV header from a file.
Returns the parsed YAML header as a Dict.
"""
function parse_ecsv_header(source::AbstractString)
    open(source, "r") do io
        parse_ecsv_header(io)
    end
end

function parse_ecsv_header(io::IO)
    buf = IOBuffer()

    for line in eachline(io)
        if startswith(line, "# %ECSV")
            # Version line - skip
            continue
        elseif startswith(line, "# ")
            # YAML content - remove "# " prefix
            println(buf, @view line[3:end])
        elseif startswith(line, "##")
            # Comment line - skip
            continue
        else
            # First non-header line - stop reading header
            break
        end
    end

    seek(buf, 0)
    return YAML.load(buf, _YAML_CONSTRUCTORS)
end

# YAML.jl doesn't support !!omap (throws "not yet implemented"); provide a custom constructor for it
# since ECSV files use !!omap in their metadata headers
_construct_omap(constructor, node) = reduce(merge, YAML.construct_sequence(constructor, node))
const _YAML_CONSTRUCTORS = Dict{String,Function}("tag:yaml.org,2002:omap" => _construct_omap)

struct ColumnSpec
    name::Symbol
    datatype::Type
    subtype::Union{NamedTuple,Nothing}
    unit
end

ColumnSpec(d::Dict) = ColumnSpec(
    Symbol(d["name"]),
    DATATYPE_MAP[d["datatype"]],
    parse_subtype(get(d, "subtype", nothing)),
    parse_unit(get(d, "unit", nothing)),
)
parse_subtype(::Nothing) = nothing
function parse_subtype(subtype_str::String)
    m = match(r"^(\w+)(\[(.+)\])?$", subtype_str)
    @assert !isnothing(m)
    return (type=DATATYPE_MAP[m.captures[1]], dims=m.captures[3])
end

parse_unit(::Nothing) = nothing
function parse_unit(unit_str::String)
    (; unit, valuefn) = VOUnits.parse_unit(unit_str)
    @assert valuefn === identity "non-trivial valuefn=$valuefn for unit '$unit_str' is not supported"
    return unit
end

convert_column(col::AbstractVector, spec::ColumnSpec) = _convert_column_u(col, spec, spec.unit)

_convert_column_u(col, spec, u::Union{Nothing, typeof(NoUnits)}) = _convert_column(col, spec.datatype, spec.subtype)
_convert_column_u(col, spec, u) = _convert_column(col, spec.datatype, spec.subtype) * u

function _convert_column(col, datatype::Type{T}, subtype::Nothing) where {T}
    T == String && return col
    
    # Handle missing values
    if any(ismissing, col)
        convert(Vector{Union{Missing,T}}, col)
    else
        convert(Vector{T}, col)
    end
end

_convert_column(col, datatype, subtype::NamedTuple) = _convert_column(col, datatype, subtype.type, subtype.dims)
function _convert_column(col, datatype::Type{String}, subtype::Type{T}, subdims::AbstractString) where {T}
    @assert T != Bool
    @assert subdims == "null"

    map(col) do x
        ismissing(x) && return missing
        JSON.parse(x, Vector{Union{Missing,T}}; allownan=true)
    end
end

JSON.@nonstruct struct MyBool
    val::Bool
end
JSON.lift(::Type{MyBool}, x::Bool) = MyBool(x)
function JSON.lift(::Type{MyBool}, s::AbstractString)
    MyBool(
        first(s) in ('T', 't', '1') ? true :
        first(s) in ('F', 'f', '0') ? false :
        parse(Bool, s)
    )
end

using StructArrays

function _convert_column(col, datatype::Type{String}, subtype::Type{Bool}, subdims::AbstractString)
    @assert subdims == "null"

    symb_to_bool = Dict(
        :false => false,
        :False => false,
        :true => true,
        :True => true,
    )

    map(col) do x
        ismissing(x) && return missing
        JSON.parse(x, StructVector{MyBool}).val
    end
end

## Writing ECSV files

function write(dest::AbstractString, table; kw...)
    open(dest, "w") do io
        write(io, table; kw...)
    end
    return dest
end

function write(io::IO, table; delim=',', kw...)
    cols = columntable(table)

    # Build header
    header = Dict{String,Any}()
    if delim != ' '
        header["delimiter"] = string(delim)
    end
    header["datatype"] = [column_to_spec(name, col) for (name, col) in pairs(cols)]

    # Write ECSV header
    println(io, "# %ECSV 1.0")
    println(io, "# ---")
    yaml_str = YAML.write(header)
    for line in split(yaml_str, "\n")
        isempty(line) && continue
        println(io, "# ", line)
    end

    # Prepare data columns
    csv_cols = NamedTuple(name => prepare_column_for_csv(col) for (name, col) in pairs(cols))

    # Write CSV data via temp file (QuackIO writes to files, not IO)
    csvtmp = tempname() * ".csv"
    try
        write_table(csvtmp, csv_cols; format=:csv, delim=string(delim), kw...)
        Base.write(io, Base.read(csvtmp))
    finally
        rm(csvtmp, force=true)
    end
end

function column_to_spec(name::Symbol, col::AbstractVector)
    spec = Dict{String,Any}("name" => string(name))
    T = eltype(col)
    NMT = Base.nonmissingtype(T)

    if NMT <: Unitful.Quantity
        valtype = Unitful.numtype(NMT)
        spec["unit"] = VOUnits.unit_string(Unitful.unit(NMT))
        spec["datatype"] = REVERSE_DATATYPE_MAP[valtype]
    elseif NMT <: Unitful.LogScaled
        valtype = NMT.parameters[3]  # numeric type is 3rd parameter of Gain
        spec["unit"] = string(Unitful.logunit(NMT))
        spec["datatype"] = REVERSE_DATATYPE_MAP[valtype]
    elseif NMT <: AbstractVector
        _set_array_spec!(spec, col)
    else
        spec["datatype"] = REVERSE_DATATYPE_MAP[NMT]
    end

    return spec
end

function _set_array_spec!(spec, col)
    # Infer element type from actual data when static type is too broad (e.g. Vector{Any})
    elemT = _infer_array_eltype(col)
    NME = Base.nonmissingtype(elemT)
    if NME <: Unitful.Quantity
        inner_valtype = Unitful.numtype(NME)
        spec["unit"] = VOUnits.unit_string(Unitful.unit(NME))
        spec["subtype"] = "$(REVERSE_DATATYPE_MAP[inner_valtype])[null]"
    elseif NME <: Unitful.LogScaled
        inner_valtype = NME.parameters[3]
        spec["unit"] = string(Unitful.logunit(NME))
        spec["subtype"] = "$(REVERSE_DATATYPE_MAP[inner_valtype])[null]"
    else
        spec["subtype"] = "$(REVERSE_DATATYPE_MAP[NME])[null]"
    end
    spec["datatype"] = "string"
end

function _infer_array_eltype(col)
    ET = eltype(Base.nonmissingtype(eltype(col)))
    ET != Any && return ET
    # Fallback: inspect first non-missing element
    for x in col
        ismissing(x) && continue
        return eltype(x)
    end
    error("cannot infer element type for empty array column")
end


prepare_column_for_csv(col::AbstractVector) = _prepare_col(col, Base.nonmissingtype(eltype(col)))

_prepare_col(col, ::Type{T}) where {T<:Union{Number,String}} = col
_prepare_col(col::AbstractVector{>:Missing}, ::Type{<:Union{Bool,Unitful.Quantity,Unitful.LogScaled,AbstractVector}}) = map(x -> ismissing(x) ? missing : _prepare_val(x), col)
_prepare_col(col, ::Type{<:Union{Bool,Unitful.Quantity,Unitful.LogScaled,AbstractVector}}) = _prepare_val.(col)

_prepare_val(x::Bool) = x ? "True" : "False"
_prepare_val(x::Union{Unitful.Quantity, Unitful.LogScaled}) = Unitful.ustrip(x)
_prepare_val(x::AbstractVector) = json_encode_array(x)

function json_encode_array(arr::AbstractVector)
    cleaned = if eltype(arr) >: Missing || eltype(arr) <: Union{Unitful.Quantity,Unitful.LogScaled}
        map(arr) do x
            ismissing(x) && return nothing
            x isa Unitful.Quantity || x isa Unitful.LogScaled ? Unitful.ustrip(x) : x
        end
    else
        arr
    end
    JSON.json(cleaned; allownan=true)
end

end