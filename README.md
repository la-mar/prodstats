# prodstats

<div style="text-align:center;">
  <table >
    <tr>
      <a href="https://codecov.io/gh/la-mar/prodstats">
        <img src="https://codecov.io/gh/la-mar/prodstats/branch/master/graph/badge.svg" />
      </a>
      <a href="(https://circleci.com/gh/la-mar/prodstats">
        <img src="https://circleci.com/gh/la-mar/prodstats.svg?style=svg" />
      </a>
            <a href="https://hub.docker.com/r/driftwood/prodstats">
        <img src="https://img.shields.io/docker/pulls/driftwood/prodstats.svg" />
      </a>
    </tr>
  </table>
</div>

## Roadmap

<!-- 0. Terraform and deployment scripts -->
<!-- 1. Container to run db migrations -->

1. Entity Master
2. Formation grid sourcing (cli upload, s3, dropbox)
3. Formation Assignment
4. Well Spacing
5. Implement WellStats

   - wellbore_crow_length
   - wellbore_direction
   - wellbore_bearing
   - wellbore_dls_roc
   - lateral_dls_roc
   - wellbore_dls_mc
   - lateral_dls_mc
   - nearest_prospect
   - dist_to_prospect_mi
   - nearest_api10
   - dist_to_company_well_mi

6. Implement WellLinks (to RRC)
7. Capture material changes in allocation well count month over month
8. Incorporate DrillingInfo well data

## TODO

1. Run task endpoint
2. calculate frac_parameters -> gen, gen_name
3. RequestRouter for provider requests
   input: const.DataType, const.Provider, const.HoleDirection
   output: PathComponent
4. Set httpx timeouts
5. Refactor jsontools and ext.orjson to jsonlib
6. Standardize log messages
7. Normalize Area/Provider related tables when incorporating drillinginfo. Change Area to ProviderArea and add providers.IDMaster.
8. Datadog metrics
9. Request/Response logging middleware
10. unit tests for config values
11. Chord GeomExecutor -> ProdExecutor -> WellExecutor(use local geoms and prod_headers)
12. parse wells.sub_basin

## Issues

- wells.county_code losing leading zeroes

### Development

#### Installation

#### Testing

- note about custom pytest markers
