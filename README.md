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

1. Formation grid sourcing (cli upload, s3, dropbox)
2. Formation Assignment
3. Well Spacing
4. Implement WellStats

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

5. Implement WellLinks (to RRC)
6. Capture material changes in allocation well count month over month
7. Incorporate DrillingInfo well data

## TODO

1. Chord GeomExecutor -> ProdExecutor -> WellExecutor(use local geoms and prod_headers)
2. RequestRouter for provider requests
   input: const.DataType, const.Provider, const.HoleDirection
   output: PathComponent
3. Set httpx timeouts
4. Refactor jsontools and ext.orjson to jsonlib
5. Standardize log messages
6. Normalize Area/Provider related tables when incorporating drillinginfo. Change Area to ProviderArea and add providers.IDMaster.
