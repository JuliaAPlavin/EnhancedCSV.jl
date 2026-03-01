using TestItems
using TestItemRunner
@run_package_tests

@testitem "example gaia" begin
    using StructArrays
    using Unitful, UnitfulAstro

    run(`gunzip --keep --force data/gaia.csv.gz`)
    tbl = EnhancedCSV.read(StructArray, "data/gaia.csv")
    @test length(tbl) == 5
    @test propertynames(tbl)[1] == :solution_id
    @test all(==(375316653866487564), tbl.solution_id)
    @test tbl.n_transits == [25, 21, 23, 26, 25]
    @test tbl.g_transit_flux[2][5] == 788.8084742392512u"s^-1"
    r = tbl[2]
    @test all(==((21,)), map(size, filter(x -> x isa AbstractArray, values(r))))

    using CodecZlib
    tbl_gz = EnhancedCSV.read(StructArray, "data/gaia.csv.gz", GzipDecompressor())
    @test length(tbl_gz) == 5
    @test isequal(tbl_gz, tbl)
end

@testitem "write scalars" begin
    using StructArrays

    tbl = (a=[1, 2, 3], b=[4.0, 5.0, 6.0], c=["x", "y", "z"])
    dest = tempname() * ".ecsv"
    EnhancedCSV.write(dest, tbl)
    tbl2 = EnhancedCSV.read(StructArray, dest)
    @test tbl2.a == [1, 2, 3]
    @test tbl2.b == [4.0, 5.0, 6.0]
    @test tbl2.c == ["x", "y", "z"]
end

@testitem "write missing" begin
    using StructArrays

    tbl = (a=Union{Int,Missing}[1, missing, 3], b=[4.0, 5.0, 6.0])
    dest = tempname() * ".ecsv"
    EnhancedCSV.write(dest, tbl)
    tbl2 = EnhancedCSV.read(StructArray, dest)
    @test tbl2.a[1] == 1
    @test ismissing(tbl2.a[2])
    @test tbl2.a[3] == 3
end

@testitem "write booleans" begin
    using StructArrays

    tbl = (flag=[true, false, true],)
    dest = tempname() * ".ecsv"
    EnhancedCSV.write(dest, tbl)
    content = Base.read(dest, String)
    @test occursin("True", content)
    @test occursin("False", content)
    tbl2 = EnhancedCSV.read(StructArray, dest)
    @test tbl2.flag == [true, false, true]
end

@testitem "write units" begin
    using StructArrays, Unitful

    tbl = (dist=[1.0, 2.0, 3.0]u"km", speed=[10.0, 20.0, 30.0]u"m/s")
    dest = tempname() * ".ecsv"
    EnhancedCSV.write(dest, tbl)
    tbl2 = EnhancedCSV.read(StructArray, dest)
    @test tbl2.dist == [1.0, 2.0, 3.0]u"km"
    @test tbl2.speed == [10.0, 20.0, 30.0]u"m*s^-1"
end

@testitem "write arrays" begin
    using StructArrays

    tbl = (id=[1, 2, 3], values=[[1.0, 2.0], [3.0, 4.0, 5.0], [6.0]])
    dest = tempname() * ".ecsv"
    EnhancedCSV.write(dest, tbl)
    tbl2 = EnhancedCSV.read(StructArray, dest)
    @test tbl2.id == [1, 2, 3]
    @test isequal(tbl2.values[1], [1.0, 2.0])
    @test isequal(tbl2.values[2], [3.0, 4.0, 5.0])
    @test isequal(tbl2.values[3], [6.0])
end

@testitem "write gaia roundtrip" begin
    using StructArrays
    using Unitful, UnitfulAstro

    run(`gunzip --keep --force data/gaia.csv.gz`)
    tbl = EnhancedCSV.read(StructArray, "data/gaia.csv")
    dest = tempname() * ".ecsv"
    EnhancedCSV.write(dest, tbl)
    tbl2 = EnhancedCSV.read(StructArray, dest)

    @test tbl2.solution_id == tbl.solution_id
    @test tbl2.n_transits == tbl.n_transits
    @test isequal(tbl2.transit_id[1], tbl.transit_id[1])
    @test tbl2.g_transit_time[2] == tbl.g_transit_time[2]
    @test tbl2.g_transit_flux[2][5] == tbl.g_transit_flux[2][5]
    @test tbl2.photometry_flag_noisy_data[1] == tbl.photometry_flag_noisy_data[1]
end

@testitem "compressed roundtrip" begin
    using StructArrays
    using CodecZlib, CodecZstd

    tbl_orig = StructArray(a=[1, 2, 3], b=[4.0, 5.0, 6.0], c=["x", "y", "z"])
    dest = tempname() * ".ecsv"
    EnhancedCSV.write(dest, tbl_orig)

    for (ext, cmd, codec) in [
        (".gz", `gzip -c`, GzipDecompressor()),
        (".zst", `zstd -c`, ZstdDecompressor()),
    ]
        dest_c = dest * ext
        run(pipeline(`$cmd $dest`, stdout=dest_c))
        tbl_c = EnhancedCSV.read(StructArray, dest_c, codec)
        @test isequal(tbl_c, tbl_orig)

        # codec works regardless of file extension
        dest_noext = tempname()
        cp(dest_c, dest_noext)
        tbl_noext = EnhancedCSV.read(StructArray, dest_noext, codec)
        @test isequal(tbl_noext, tbl_orig)
    end
end

@testitem "astropy subtypes" begin
    using StructArrays

    tbl = EnhancedCSV.read(StructArray, "data/astropy/subtypes.ecsv")
    @test length(tbl) == 16

    # Scalar types
    @test tbl.i_index[1] == 0
    @test tbl.s_byte[1] === Int8(0)
    @test tbl.s_short[1] === Int16(0)
    @test tbl.s_int[1] === Int32(0)
    @test tbl.s_long[1] === Int64(0)
    @test tbl.s_float[1] === Float32(0.0)
    @test tbl.s_double[1] === Float64(0.0)
    @test tbl.s_string[1] == "zero"
    @test tbl.s_boolean[1] === false

    # Missing scalars
    @test ismissing(tbl.s_byte[2])
    @test ismissing(tbl.s_short[3])
    @test ismissing(tbl.s_int[4])
    @test ismissing(tbl.s_long[5])

    # NaN
    @test isnan(tbl.s_float[6])
    @test isnan(tbl.s_double[7])

    # Variable-length arrays
    @test tbl.v_int[1] == [0, 1, 2]
    @test tbl.v_int[3] == [2]
    @test ismissing(tbl.v_int[4])
    @test tbl.v_byte[4] == []

    # Null values within arrays
    @test isequal(tbl.v_float[1], Union{Missing,Float32}[0.0f0, missing, 2.5f0])
    @test isequal(tbl.f_float[1], Union{Missing,Float32}[0.0f0, missing, 2.5f0])
    @test isequal(tbl.v_string[1], Union{Missing,String}["foo", missing, "zero"])

    # Fixed-size arrays
    @test tbl.f_int[1] == [0, 1, 2]
    @test tbl.f_short[2] == [1, 2, 3]

    # Fixed-size boolean arrays
    @test tbl.f_boolean[1] == [false, false, false]
    @test tbl.f_boolean[2] == [true, false, false]

    # Variable-length boolean arrays
    @test tbl.v_boolean[1] == [false, false, false]
    @test tbl.v_boolean[3] == [false]

    # Multidimensional arrays parse as nested vectors
    @test tbl.m_int[1][1] == [1000, 1001]
    @test tbl.m_double[1][2] == [-0.25, -0.5, -0.75]

    # Missing entire array cells
    @test ismissing(tbl.f_byte[2])
    @test ismissing(tbl.v_byte[2])
    @test ismissing(tbl.m_int[2])
end

@testitem "astropy Planck18" begin
    using StructArrays
    using Unitful

    tbl = EnhancedCSV.read(StructArray, "data/astropy/Planck18.ecsv")
    @test length(tbl) == 1

    # Scalar values
    @test tbl.name[1] == "Planck18"
    @test tbl.Om0[1] == 0.30966
    @test tbl.Neff[1] == 3.046
    @test tbl.Ob0[1] == 0.04897

    # Units
    @test tbl.Tcmb0[1] == 2.7255u"K"

    # Array column with units
    @test tbl.m_nu[1] == [0.0, 0.0, 0.06]u"eV"
end

@testitem "astropy cosmo_flat" begin
    using StructArrays
    using Unitful, UnitfulAstro

    tbl = EnhancedCSV.read(StructArray, "data/astropy/cosmo_flat.ecsv")
    @test length(tbl) == 29

    # Mpc unit on distance columns
    @test unit(tbl.dm[2]) == u"Mpc"
    @test tbl.dm[2] ≈ 669.77536u"Mpc"
    @test tbl.da[2] ≈ 576.15085u"Mpc"

    # redshift unit is not parseable — column should still have numeric values
    @test tbl.redshift[2] ≈ 0.1625
end

@testitem "astropy rv" begin
    using StructArrays
    using Unitful

    tbl = EnhancedCSV.read(StructArray, "data/astropy/rv.ecsv")
    @test length(tbl) == 50

    # deg unit on coordinate columns
    @test unit(tbl.var"target.ra"[1]) == u"°"
    @test tbl.var"target.ra"[1] ≈ 334.66179341417325u"°"
    @test tbl.var"target.dec"[1] ≈ -37.4746243276451u"°"

    # Columns without units
    @test tbl.heliocent[1] ≈ 23.14
end

@testitem "astropy inline: masked bool" begin
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """# %ECSV 1.0
# ---
# datatype:
# - {name: col0, datatype: bool}
# schema: astropy-2.0
col0
1
0
True
""
False
""")
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 5
    @test tbl.col0[1] === true
    @test tbl.col0[2] === false
    @test tbl.col0[3] === true
    @test ismissing(tbl.col0[4])
    @test tbl.col0[5] === false
end

@testitem "astropy inline: meta as map" begin
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """
# %ECSV 1.0
# ---
# datatype:
# - {name: fake, datatype: string}
# meta:
#   hr:  65    # Home runs
#   avg: 0.278 # Batting average
#   rbi: 147   # Runs Batted In
# schema: astropy-2.0
fake
0""")
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 1
    @test tbl.fake[1] == "0"
end

@testitem "astropy inline: meta as list" begin
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """
# %ECSV 1.0
# ---
# meta:
# - keywords:
#   - {z_key1: val1}
#   - {a_key2: val2}
# - comments: [Comment 1, Comment 2, Comment 3]
# datatype:
# - name: fake
#   datatype: string
fake
0""")
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 1
    @test tbl.fake[1] == "0"
end

@testitem "astropy inline: simple" begin
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """# %ECSV 1.0
# ---
# datatype:
# - {name: a, datatype: int64}
# - {name: b, datatype: float64}
# - {name: c, datatype: string}
# schema: astropy-2.0
a b c
1 1.0 c
2 2.0 d
3 3.0 e
""")
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 3
    @test tbl.a == [1, 2, 3]
    @test tbl.b == [1.0, 2.0, 3.0]
    @test tbl.c == ["c", "d", "e"]
end

@testitem "astropy inline: write_full" begin
    # Verbatim from astropy test_ecsv.py test_write_full
    using StructArrays, Unitful

    f = tempname() * ".ecsv"
    Base.write(f, """# %ECSV 1.0
# ---
# datatype:
# - name: bool
#   unit: m / s
#   datatype: bool
#   description: descr_bool
#   meta: !!omap
#   - {meta bool: 1}
#   - {a: 2}
# - name: int64
#   unit: m / s
#   datatype: int64
#   description: descr_int64
#   meta: !!omap
#   - {meta int64: 1}
#   - {a: 2}
# - name: float64
#   unit: m / s
#   datatype: float64
#   description: descr_float64
#   meta: !!omap
#   - {meta float64: 1}
#   - {a: 2}
# - name: str
#   unit: m / s
#   datatype: string
#   description: descr_str
#   meta: !!omap
#   - {meta str: 1}
#   - {a: 2}
# meta: !!omap
# - comments: [comment1, comment2]
# - {a: 3}
# schema: astropy-2.0
bool int64 float64 str
False 0 0.0 "ab 0"
True 1 1.0 "ab, 1"
False 2 2.0 ab2
""")
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 3
    @test tbl.bool == [false, true, false]u"m/s"
    @test tbl.int64 == [0, 1, 2]u"m/s"
    @test tbl.float64 == [0.0, 1.0, 2.0]u"m/s"
    # unit on string column is metadata-only (like AstroPy), not applied
    @test tbl.str == ["ab 0", "ab, 1", "ab2"]
end

@testitem "astropy inline: multidim_only_masked" begin
    # Verbatim from astropy test_ecsv.py test_multidim_only_masked
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """
# %ECSV 1.0
# ---
# datatype:
# - {name: array3x2, datatype: string, subtype: 'float64[3,2]'}
array3x2
""
""")
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 1
    @test ismissing(tbl.array3x2[1])
end

@testitem "astropy inline: multidim_unknown_subtype" begin
    # Verbatim from astropy test_ecsv.py test_multidim_unknown_subtype (subtype="complex")
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """# %ECSV 1.0
# ---
# datatype:
# - name: a
#   datatype: string
#   subtype: complex
# schema: astropy-2.0
a
[1,2]
[3,4]""")
    # unknown subtype "complex" — should fall back to plain string
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 2
    @test tbl.a[1] == "[1,2]"
    @test tbl.a[2] == "[3,4]"
end

@testitem "astropy inline: read_bad_datatype" begin
    # Verbatim from astropy test_ecsv.py test_read_bad_datatype
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """# %ECSV 1.0
# ---
# datatype:
# - {name: a, datatype: object}
# schema: astropy-2.0
a
{"x":1}
[3,4]""")
    # unknown datatype "object" — falls back to String
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 2
    @test tbl.a[1] == "{\"x\":1}"
    @test tbl.a[2] == "[3,4]"
end

@testitem "astropy inline: read_complex" begin
    # Verbatim from astropy test_ecsv.py test_read_complex
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """# %ECSV 1.0
# ---
# datatype:
# - {name: a, datatype: complex}
# schema: astropy-2.0
a
1+1j
2+2j""")
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 2
    @test_broken tbl.a[1] == 1+1im
    @test_broken tbl.a[2] == 2+2im
end

@testitem "astropy inline: read_str" begin
    # Verbatim from astropy test_ecsv.py test_read_str
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """# %ECSV 1.0
# ---
# datatype:
# - {name: a, datatype: str}
# schema: astropy-2.0
a
sometext
S""")
    # unknown datatype "str" — falls back to String
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 2
    @test tbl.a[1] == "sometext"
    @test tbl.a[2] == "S"
end

@testitem "astropy inline: masked_empty_subtypes" begin
    # Verbatim from astropy test_ecsv.py test_masked_empty_subtypes
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """
# %ECSV 1.0
# ---
# datatype:
# - {name: o, datatype: string, subtype: json}
# - {name: f, datatype: string, subtype: 'int64[2]'}
# - {name: v, datatype: string, subtype: 'int64[null]'}
# schema: astropy-2.0
o f v
null [0,1] [1]
"" "" ""
[1,2] [2,3] [2,3]
""")
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 3
    # "json" subtype is unknown — treated as plain string
    @test tbl.o[1] == "null"
    @test ismissing(tbl.o[2])
    @test tbl.o[3] == "[1,2]"
    # fixed-size array
    @test tbl.f[1] == [0, 1]
    @test ismissing(tbl.f[2])
    @test tbl.f[3] == [2, 3]
    # variable-length array
    @test tbl.v[1] == [1]
    @test ismissing(tbl.v[2])
    @test tbl.v[3] == [2, 3]
end

@testitem "astropy inline: masked_vals_in_array_subtypes" begin
    # Derived from astropy test_ecsv.py test_masked_vals_in_array_subtypes
    # AstroPy writes a table and reads back; we use the expected ECSV output as input
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """
# %ECSV 1.0
# ---
# datatype:
# - {name: f, datatype: string, subtype: 'int64[2]'}
# - {name: v, datatype: string, subtype: 'int64[null]'}
# schema: astropy-2.0
f v
[1,null] [1,null]
[null,4] [null,4,5]
""")
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 2
    @test isequal(tbl.f[1], [1, missing])
    @test isequal(tbl.f[2], [missing, 4])
    @test isequal(tbl.v[1], [1, missing])
    @test isequal(tbl.v[2], [missing, 4, 5])
end

@testitem "astropy inline: round_trip_empty_table" begin
    # Derived from astropy test_ecsv.py test_round_trip_empty_table
    # AstroPy writes an empty table and reads back; we use equivalent ECSV as input
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """# %ECSV 1.0
# ---
# datatype:
# - {name: a, datatype: bool}
# - {name: b, datatype: int32}
# - {name: c, datatype: float64}
# schema: astropy-2.0
a b c
""")
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 0
end

@testitem "astropy inline: read_not_json_serializable" begin
    # Verbatim from astropy test_ecsv.py test_read_not_json_serializable
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """# %ECSV 1.0
# ---
# datatype:
# - {name: a, datatype: string, subtype: json}
# schema: astropy-2.0
a
fail
[3,4]""")
    # "json" subtype with invalid JSON "fail" — treated as plain string since json is unknown subtype
    tbl = EnhancedCSV.read(StructArray, f)
    @test length(tbl) == 2
    @test tbl.a[1] == "fail"
    @test tbl.a[2] == "[3,4]"
end

@testitem "astropy inline: read_bad_datatype_for_object_subtype" begin
    # Verbatim from astropy test_ecsv.py test_read_bad_datatype_for_object_subtype
    using StructArrays

    f = tempname() * ".ecsv"
    Base.write(f, """# %ECSV 1.0
# ---
# datatype:
# - {name: a, datatype: int64, subtype: json}
# schema: astropy-2.0
a
fail
[3,4]""")
    # datatype int64 with subtype json is invalid — AstroPy also raises an error
    @test_throws Exception EnhancedCSV.read(StructArray, f)
end

@testitem "_" begin
    # import Aqua
    # Aqua.test_all(EnhancedCSV)

    import CompatHelperLocal as CHL
    CHL.@check()
end
