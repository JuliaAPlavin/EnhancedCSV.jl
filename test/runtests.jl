using TestItems
using TestItemRunner
@run_package_tests

@testitem "example gaia" begin
    using StructArrays
    using Unitful

    tbl = EnhancedCSV.read(StructArray, "data/gaia.csv")
    @test length(tbl) == 5
    @test propertynames(tbl)[1] == :solution_id
    @test all(==(375316653866487564), tbl.solution_id)
    @test tbl.n_transits == [25, 21, 23, 26, 25]
    @test tbl.g_transit_flux[2][5] == 788.8084742392512u"s^-1"
end

@testitem "_" begin
    # import Aqua
    # Aqua.test_all(EnhancedCSV)

    import CompatHelperLocal as CHL
    CHL.@check()
end
