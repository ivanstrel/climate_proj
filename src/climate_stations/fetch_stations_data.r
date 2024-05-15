library(tidyverse)
library(zoo)
library(lubridate)
library(sf)
library(progress)
library(httr)
library(jsonlite)

START_DATE = dmy("01-01-1993")
END_DATE = dmy("31-12-2024")
DATES = seq(START_DATE, END_DATE, by = "10 day")

## Read bounding box
bbox <- st_read("data/bbox/Selected_S2_tiles.geojson") |>
    st_transform(crs = 4326) |>
    # Add about 10 km buffer
    st_buffer(dist = 10000) |>
    st_bbox()

# =========================================================================== #
# Meteostat data download   https://dev.meteostat.net  ####
# =========================================================================== #
# Read stations
url <- "https://bulk.meteostat.net/v2/stations/full.json.gz"
response <- GET(url)

temp_file <- tempfile(fileext = ".gz")
writeBin(content(response, "raw"), temp_file)

stations <- fromJSON(temp_file) |>
    as_tibble() |>
    unnest_wider(col = c("name", "location", "identifiers", "inventory"), names_sep = "_") |>
    dplyr::select(id, name_en, location_longitude, location_latitude,
                  location_elevation,
                  identifiers_wmo, identifiers_national, identifiers_icao,
                  ) |>
    rename(meteostat_id = id, 
           name = name_en,
           lon = location_longitude,
           lat = location_latitude,
           alt = location_elevation,
           wmo_id = identifiers_wmo,
           national_id = identifiers_national,
           icao_id = identifiers_icao
           ) |>
    # Convert to spatial and filter by bbox
    st_as_sf(coords = c("lon", "lat"), crs = 4326) |>
    st_crop(bbox)

# Export as geojson
st_write(stations, "output/meteostat_data/meteostat_stations.geojson",
         delete_dsn = TRUE)

# --------------------------------------------------------------------------- #
# Download data for each station                                           ####
# --------------------------------------------------------------------------- #
stat_data <- apply(stations, 1, function(x) {
    stationid = x["meteostat_id"]
    print(stationid)
    # Download bulk data csv by url template:
    # https://bulk.meteostat.net/v2/daily/{station}.csv.gz as temp file
    url <- sprintf("https://bulk.meteostat.net/v2/daily/%s.csv.gz", stationid)
    response <- GET(url)
    temp_file <- tempfile(fileext = ".gz")
    writeBin(content(response, "raw"), temp_file)
    # Read data
    colnames = params <- c(
        "date", "tavg", "tmin", "tmax", "prcp", "snow", "wdir", "wspd", "wpgt", "pres", "tsun"
    )
    # More about colnames
    df <- read_csv(temp_file, col_names = FALSE) |>
        as_tibble()
    # Check if the data frame is not empty
    if (nrow(df) == 0 | ncol(df) == 1) {
        return(NULL)
    }
    # Add station id
    df <- df |>
        set_names(colnames) |>
        mutate(meteostat_id = as.character(stationid)) |>
        # Drop wdir wspd wpgt pres tsun
        dplyr::select(-c(wdir, wspd, wpgt, pres, tsun))
    return(df)
    }
) |>
    bind_rows() %>%
    # Filter by date
    filter(date >= ymd("1994-01-01")) %>%
    # Drop rows where all observation == NA
    dplyr::filter(!if_all(2:6, ~ is.na(.)))
# Join tables
meteostat_data_full <- stations |>
    right_join(stat_data, by = "meteostat_id")

# --------------------------------------------------------------------------- #
# Test plots (data coverage)    ####
# --------------------------------------------------------------------------- #
p <- ggplot(meteostat_data_full, aes(x = date)) +
    geom_point(aes(y = tavg, color = meteostat_id), size = 0.1) +
    facet_grid(rows = vars(meteostat_id))
ggsave("pics/tests/meteostat_data_full_tavg.png", p, dpi = 300)
p <- ggplot(meteostat_data_full, aes(x = date)) +
    geom_point(aes(y = prcp, color = meteostat_id), size = 0.1) +
    facet_grid(rows = vars(meteostat_id))
ggsave("pics/tests/meteostat_data_full_prcp.png", p, dpi = 300)


# Export as geojson
st_write(meteostat_data_full,
        "output/meteostat_data/meteostat_data_full.geojson",
         delete_dsn = TRUE)
# Export to csv
write_csv(
    meteostat_data_full |>
        as_tibble() |>
        select(-geometry) |>
        bind_cols(
            do.call(rbind, st_geometry(meteostat_data_full)) |> 
                as_tibble() |>
                setNames(c("lon","lat"))
        ),
    "output/meteostat_data/meteostat_data_full.csv"
)

# =========================================================================== #
# Prepare daily normals                                                    ####
# =========================================================================== #
meteostat_daily_norm <- meteostat_data_full |>
    mutate(doy = yday(date)) |>
    group_by(meteostat_id, doy, name, alt, wmo_id, national_id, icao_id, geometry) |>
    summarise(tavg = mean(tavg, na.rm = TRUE),
              tmin = mean(tmin, na.rm = TRUE),
              tmax = mean(tmax, na.rm = TRUE),
              prcp = mean(prcp, na.rm = TRUE),
              snow = mean(snow, na.rm = TRUE),
              n_obs = n()
              ) |>
    ungroup() |>
    # Replace Inf with NA
    mutate(across(c(tavg, tmin, tmax, prcp, snow), ~ ifelse(.x == Inf, NA, .x)))

# Export geojson and csv
st_write(meteostat_daily_norm, "output/meteostat_data/meteostat_daily_norm.geojson",
         delete_dsn = TRUE)
write_csv(meteostat_daily_norm |>
            as_tibble() |>
            select(-geometry) |>
            bind_cols(
                do.call(rbind, st_geometry(meteostat_daily_norm)) |> 
                    as_tibble() |>
                    setNames(c("lon","lat"))
            ),
        "output/meteostat_data/meteostat_daily_norm.csv")

# =========================================================================== #
# Prepare monthly normals                                                  ####
# =========================================================================== #
meteostat_monthly_norm <- meteostat_data_full |>
    mutate(month = month(date)) |>
    group_by(meteostat_id, month, name, alt, wmo_id, national_id, icao_id, geometry) |>
    summarise(tavg = mean(tavg, na.rm = TRUE),
              tmin = mean(tmin, na.rm = TRUE),
              tmax = mean(tmax, na.rm = TRUE),
              prcp = mean(prcp, na.rm = TRUE),
              snow = mean(snow, na.rm = TRUE),
              n_obs = n()
              ) |>
    ungroup() |>
    # Replace Inf with NA
    mutate(across(c(tavg, tmin, tmax, prcp, snow), ~ ifelse(.x == Inf, NA, .x)))
# Export geojson and csv
st_write(meteostat_monthly_norm, "output/meteostat_data/meteostat_monthly_norm.geojson",
         delete_dsn = TRUE)
write_csv(meteostat_monthly_norm |>
            as_tibble() |>
            select(-geometry) |>
            bind_cols(
                do.call(rbind, st_geometry(meteostat_monthly_norm)) |> 
                    as_tibble() |>
                    setNames(c("lon","lat"))
            ),
        "output/meteostat_data/meteostat_monthly_norm.csv")

# Test graph
p <- ggplot(meteostat_monthly_norm, aes(x = month)) +
    geom_line(aes(y = tavg, color = meteostat_id))
ggsave("pics/tests/meteostat_monthly_norm_tavg.png", p, dpi = 300)