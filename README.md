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

1. Formation grid sourcing (cli upload, s3, dropbox)
2. Formation Assignment
3. Implement wellstats
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
4. Capture comments on production calculations and prodstats

   - material changes in allocation well count

5. Generate well links to relevant RRC filings
6. Production links needed?
7. Well Spacing
8. RequestRouter for provider requests:
   - input: const.DataType, const.Provider, const.HoleDirection
   - output: PathComponent
9. Standardize log messages
10. Refactor executors.py functionality into base class
11. Populate lat/lon on well_location
12. Add geom_webmercator to well_location, survey_points, surveys
13. Change prodstat aggregate_type stored in database from 'mean' to 'avg' for consistency
14. Set httpx timeouts
