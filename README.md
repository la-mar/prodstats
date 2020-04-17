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

# TODO:

1. Chord GeomExecutor -> ProdExecutor -> WellExecutor(use local geoms and prod_headers)
1. Formation grid sourcing (cli upload, s3, dropbox)
1. Formation Assignment
1. Implement wellstats
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
1. Capture comments on production calculations and prodstats

   - material changes in allocation well count

1. Generate well links to relevant RRC filings
1. Production links needed?
1. Well Spacing
1. RequestRouter for provider requests:
   - input: const.DataType, const.Provider, const.HoleDirection
   - output: PathComponent
1. Standardize log messages
1. Refactor executors.py functionality into base class
1. Populate lat/lon on well_location
1. Add geom_webmercator to well_location, survey_points, surveys
1. Change prodstat aggregate_type stored in database from 'mean' to 'avg' for consistency
1. Set httpx timeouts
1. refactor jsontools and ext.orjson to jsonlib module with api
1. Normalize Area/Provider related tables when incorporating drillinginfo. Change Area to ProviderArea and add providers.IDMaster.
1. Schema provider fields to Enum
