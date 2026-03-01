module TranscodingStreamsExt

using EnhancedCSV
using TranscodingStreams: TranscodingStreams, TranscodingStream, Codec

function _compression_name(codec::Codec)
    name = lowercase(string(nameof(typeof(codec))))
    # e.g. "gzipdecompressor" -> "gzip", "zstddecompressor" -> "zstd"
    replace(name, r"(de)?compress(or|ion)$" => "")
end

function EnhancedCSV.read(sink, source::AbstractString, codec::Codec; kw...)
    header = open(source, "r") do file_io
        EnhancedCSV.parse_ecsv_header(TranscodingStream(codec, file_io))
    end
    EnhancedCSV._read_body(sink, source, header; compression=_compression_name(codec), kw...)
end

end
