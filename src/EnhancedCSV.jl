module EnhancedCSV

using CSV
using YAML
import JSON
using Unitful
using Tables: columntable
using DataPipes

export read

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

"""
    read(sink, source; kw...)

Read an ECSV (Enhanced CSV) file and return data in the specified sink format.

The ECSV format consists of:
- A YAML header with column metadata (lines starting with `# `)
- A CSV data section

# Arguments
- `sink`: The type to materialize the data into (e.g., `StructArray`)
- `source`: File path or IO object
- `kw...`: Additional keyword arguments passed to `CSV.read`
"""
function read end

function read(sink, source::AbstractString; kw...)
    header = parse_ecsv_header(source)
    colspecs = NamedTuple(Symbol(d["name"]) => ColumnSpec(d) for d in header["datatype"])

    delim = only(get(header, "delimiter", " "))
    tbl = CSV.read(source, columntable; comment="#", delim, ntasks=1, kw...)
    
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
    yaml_lines = String[]
    
    for line in eachline(io)
        if startswith(line, "# %ECSV")
            # Version line - skip
            continue
        elseif startswith(line, "# ")
            # YAML content - remove "# " prefix
            push!(yaml_lines, line[3:end])
        elseif startswith(line, "##")
            # Comment line - skip
            continue
        else
            # First non-header line - stop reading header
            break
        end
    end

    return YAML.load(join(yaml_lines, "\n"))
end

struct ColumnSpec
    name::Symbol
    datatype::Type
    subtype::Union{NamedTuple,Nothing}
    unit::Union{Unitful.FreeUnits,Unitful.MixedUnits,Nothing}
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
    try
        @p let
            unit_str
            replace(__,
                r"'?\b(/beam|/pix|electron)\b'?" => (s -> (@warn "ignoring the unsupported '$s' unit" unit_str; "")),
                # "'" => "",  # XXX: shouldn't have arcminutes described this way?
            )
            replace(__,
            #     r"^/" => "1/",
            #     r"/$" => "",
                r"^\." => "",
                r"\.$" => "",
                r"([^*])\*\*([^*])" => s"\1^\2",
            )
            
            # # handle eg "mas.yr-1":
            # replace(__, r"(\w)\." => s"\1*")
            # replace(__, r"(\w)(-?\d)" => s"\1^\2")

            # replace(__,
            #     r"\bdeg\b" => "°",
            #     r"\barcsec\b" => "arcsecond",
            #     r"\barcmin\b" => "arcminute",
            #     r"\bum\b" => "μm",
            #     r"\bAngstrom\b" => "angstrom")
            uparse(unit_context=[Unitful; Unitful.unitmodules], __)
        end
    catch exception
        if exception isa ArgumentError && occursin("could not be found in unit modules", exception.msg)
            @warn "cannot parse unit '$unit_str', ignoring it"
        else
            @warn "cannot parse unit '$unit_str', ignoring it" exception
        end
        return nothing
    end
end

convert_column(col::AbstractVector, spec::ColumnSpec) = _convert_column_u(col, spec, spec.unit)

_convert_column_u(col, spec, u::Nothing) = _convert_column(col, spec.datatype, spec.subtype)
_convert_column_u(col, spec, u::Union{Unitful.FreeUnits,Unitful.MixedUnits}) = _convert_column(col, spec.datatype, spec.subtype) * u

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

__precompile__(false)
@eval YAML function construct_yaml_omap(constructor::Constructor, node::Node)
    reduce(merge, construct_sequence(constructor, node))
end

end