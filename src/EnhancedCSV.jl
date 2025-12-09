module EnhancedCSV

using CSV
using YAML

export read

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
    display(header)
    
    delimiter = get(header, "delimiter", " ")
    delim_char = only(delimiter)
    
    CSV.read(source, sink; comment="#", delim=delim_char, kw...)
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
    
    # Parse YAML
    # Preprocess to handle unsupported !!omap tag - just remove the tag, keep the structure
    yaml_content = join(yaml_lines, "\n")
    isempty(yaml_content) ? Dict{String,Any}() : YAML.load(yaml_content)
end



__precompile__(false)
@eval YAML function construct_yaml_omap(constructor::Constructor, node::Node)
    reduce(merge, construct_sequence(constructor, node))
end


end
