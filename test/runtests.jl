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

@testitem "_" begin
    # import Aqua
    # Aqua.test_all(EnhancedCSV)

    import CompatHelperLocal as CHL
    CHL.@check()
end
