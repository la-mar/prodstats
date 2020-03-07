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

# prodstat methodology

## header inputs

- api10
- perf_upper_min
- perf_lower_max
- frac_lb
- frac_bbl

# Add comments to prodstat records

- indicate if a sizable change occured in well_count

## monthly inputs

- api14
- api10
- first_date (year, month) -> prod_date
- primary_product
- oil (liquid)
- total_gas (gas)
- wells (well_count or oil_well_count)

## steps

1. group by api10 & prod_date ->
   return: - oil_min - oil_max - oil_avg - gas_min - gas_max - gas_avg
   - add comment for number of duplicates removed
