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
  <!-- - frac_lb -->
  <!-- - frac_bbl -->

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

### preprocess monthly prod

1. group by api10 & prod_date ->
   return:

   - prod_header:

     - api10
     - prod_date_first (prod_date_min)
     - prod_date_last (prod_date_max)
     - oil_min
     - oil_max
     - oil_sum
     - gas_min
     - gas_max
     - gas_sum
     - prod_month_max
     - api14s (array of api14s used in this calculation)
     - comments (capture number of duplicates removed)

   - prod_monthly:

     - api10
     - prod_date
     - prod_month (ordinal: 1, 2, 3, ..., n)
     - primary_product
     - oil
     - gas
     - water

### calculate

#### peak30

1.  calculate peak30: calculate the peak 30 day oil value within the first 12 months
    global: PEAK_NORM_LIMIT_MONTHS = 12

    inputs: prod_monthly

    outputs:

    - api10
    - peak30_date
    - peak30_prod_month
    - oil_peak30
    - gas_peak30

2.  calculate peak_norm_month: ordinal like prod_month, but 1 begins at peak_prod_month. (prod_date - peak30_prod_date).months (perhaps +1)

#### peak30_sum_window

1. calculate peak30_sum over a given number of months
   QUESTION: should the window be formed on peak_norm_month or prod_month?
   global: PEAK_NORM_LIMIT_MONTHS

   inputs:

   - oil
   - gas
   - peak_norm_month
   - (param) windows_size_months: int
   - max_by: str (e.g. "oil", "gas")
   - limit: int (global.PEAK_NORM_LIMIT_MONTHS)

   outputs:

   - frame:
     - oil_sum_pknorm
     - oil_gas_pknorm
     - prod_month_start
     - prod_month_end
   - series:
     - record with maximum max_by parameter

2. add peak_norm max for each window to prod_header:
   - oil_sum_pknorm_1mo
   - oil_sum_pknorm_3mo
   - oil_sum_pknorm_6mo
   - gas_sum_pknorm_1mo
   - gas_sum_pknorm_3mo
   - gas_sum_pknorm_6mo

#### prod aggregates

- oil_sum_first_1mo
- oil_sum_first_3mo
- oil_sum_first_6mo
- oil_sum_first_9mo
- oil_sum_first_12mo
- oil_sum_first_18mo
- oil_sum_first_24mo
- oil_sum_last_1mo
- oil_sum_last_3mo
- oil_sum_last_6mo
- oil_total
