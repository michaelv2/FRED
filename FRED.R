# FRED API R Implementation
# R equivalent of the Python fred.py class

# Required libraries
library(httr)
library(xml2)
library(lubridate)
library(dplyr)

# Constants
EARLIEST_REALTIME_START <- '1776-07-04'
LATEST_REALTIME_END <- '9999-12-31'
NAN_CHAR <- '.'
MAX_RESULTS_PER_REQUEST <- 1000

# Initialize FRED API connection
fred_init <- function(api_key = NULL, api_key_file = NULL) {
  # Initialize the FRED API connection
  # You need to specify a valid API key in one of 3 ways:
  # 1. Pass the string via api_key
  # 2. Set api_key_file to a file with the api key in the first line
  # 3. Set the environment variable 'FRED_API_KEY'
  
  if (!is.null(api_key)) {
    key <- api_key
  } else if (!is.null(api_key_file)) {
    key <- trimws(readLines(api_key_file, n = 1))
  } else {
    key <- Sys.getenv('FRED_API_KEY', unset = NA)
  }
  
  if (is.na(key) || key == "") {
    stop("You need to set a valid API key. You can set it in 3 ways: pass the string with api_key, ",
         "or set api_key_file to a file with the api key in the first line, or set the environment ",
         "variable 'FRED_API_KEY' to the value of your api key. You can sign up for a free api key ",
         "on the Fred website at http://research.stlouisfed.org/fred2/")
  }
  
  # Store API key in global environment for functions to use
  assign("FRED_API_KEY", key, envir = .GlobalEnv)
  return(key)
}

# Helper function to fetch data from URL
fetch_fred_data <- function(url) {
  tryCatch({
    response <- GET(url)
    if (status_code(response) != 200) {
      error_content <- content(response, "text")
      root <- read_xml(error_content)
      error_msg <- xml_attr(root, "message")
      stop(error_msg)
    }
    content(response, "text") %>% read_xml()
  }, error = function(e) {
    stop("Error fetching data: ", e$message)
  })
}

# Helper function to parse dates
parse_fred_date <- function(date_str, format = '%Y-%m-%d') {
  as.Date(date_str, format = format)
}

# Get information about a series
fred_get_series_info <- function(series_id, api_key = NULL) {
  if (is.null(api_key)) {
    api_key <- get("FRED_API_KEY", envir = .GlobalEnv)
  }
  
  url <- sprintf("http://api.stlouisfed.org/fred/series?series_id=%s&api_key=%s", 
                 series_id, api_key)
  
  root <- fetch_fred_data(url)
  
  if (is.null(root) || length(xml_children(root)) == 0) {
    stop('No info exists for series id: ', series_id)
  }
  
  # Extract attributes from the first child element
  series_node <- xml_children(root)[[1]]
  info <- xml_attrs(series_node)
  
  return(as.data.frame(t(info), stringsAsFactors = FALSE))
}

# Get data for a Fred series (latest release)
fred_get_series <- function(series_id, observation_start = NULL, observation_end = NULL, 
                            api_key = NULL, ...) {
  if (is.null(api_key)) {
    api_key <- get("FRED_API_KEY", envir = .GlobalEnv)
  }
  
  url <- sprintf("http://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s", 
                 series_id, api_key)
  
  if (!is.null(observation_start)) {
    observation_start <- as.Date(observation_start)
    url <- paste0(url, '&observation_start=', format(observation_start, '%Y-%m-%d'))
  }
  
  if (!is.null(observation_end)) {
    observation_end <- as.Date(observation_end)
    url <- paste0(url, '&observation_end=', format(observation_end, '%Y-%m-%d'))
  }
  
  # Add additional parameters
  extra_params <- list(...)
  if (length(extra_params) > 0) {
    param_string <- paste(names(extra_params), extra_params, sep = "=", collapse = "&")
    url <- paste0(url, '&', param_string)
  }
  
  root <- fetch_fred_data(url)
  
  if (is.null(root) || length(xml_children(root)) == 0) {
    stop('No data exists for series id: ', series_id)
  }
  
  # Extract data
  observations <- xml_children(root)
  dates <- character(length(observations))
  values <- numeric(length(observations))
  
  for (i in seq_along(observations)) {
    obs <- observations[[i]]
    dates[i] <- xml_attr(obs, "date")
    val <- xml_attr(obs, "value")
    
    if (val == NAN_CHAR) {
      values[i] <- NA
    } else {
      values[i] <- as.numeric(val)
    }
  }
  
  data.frame(
    date = parse_fred_date(dates),
    value = values,
    stringsAsFactors = FALSE
  )
}

# Get latest release data (alias for fred_get_series)
fred_get_series_latest_release <- function(series_id, api_key = NULL) {
  fred_get_series(series_id, api_key = api_key)
}

# Get all releases data for a series
fred_get_series_all_releases <- function(series_id, api_key = NULL) {
  if (is.null(api_key)) {
    api_key <- get("FRED_API_KEY", envir = .GlobalEnv)
  }
  
  url <- sprintf("http://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&realtime_start=%s&realtime_end=%s",
                 series_id, api_key, EARLIEST_REALTIME_START, LATEST_REALTIME_END)
  
  root <- fetch_fred_data(url)
  
  if (is.null(root) || length(xml_children(root)) == 0) {
    stop('No data exists for series id: ', series_id)
  }
  
  observations <- xml_children(root)
  
  # Pre-allocate vectors
  n_obs <- length(observations)
  dates <- character(n_obs)
  realtime_starts <- character(n_obs)
  values <- numeric(n_obs)
  
  for (i in seq_along(observations)) {
    obs <- observations[[i]]
    dates[i] <- xml_attr(obs, "date")
    realtime_starts[i] <- xml_attr(obs, "realtime_start")
    val <- xml_attr(obs, "value")
    
    if (val == NAN_CHAR) {
      values[i] <- NA
    } else {
      values[i] <- as.numeric(val)
    }
  }
  
  data.frame(
    date = parse_fred_date(dates),
    realtime_start = parse_fred_date(realtime_starts),
    value = values,
    stringsAsFactors = FALSE
  )
}

# Get first release data
fred_get_series_first_release <- function(series_id, api_key = NULL) {
  df <- fred_get_series_all_releases(series_id, api_key)
  
  # Group by date and take the first release (earliest realtime_start)
  df %>%
    group_by(date) %>%
    slice_min(realtime_start, n = 1) %>%
    ungroup() %>%
    select(date, value)
}

# Get series data as of a specific date
fred_get_series_as_of_date <- function(series_id, as_of_date, api_key = NULL) {
  as_of_date <- as.Date(as_of_date)
  df <- fred_get_series_all_releases(series_id, api_key)
  
  # Filter data up to as_of_date
  df %>%
    filter(realtime_start <= as_of_date) %>%
    group_by(date) %>%
    slice_max(realtime_start, n = 1) %>%
    ungroup()
}

# Get vintage dates for a series
fred_get_series_vintage_dates <- function(series_id, api_key = NULL) {
  if (is.null(api_key)) {
    api_key <- get("FRED_API_KEY", envir = .GlobalEnv)
  }
  
  url <- sprintf("http://api.stlouisfed.org/fred/series/vintagedates?series_id=%s&api_key=%s", 
                 series_id, api_key)
  
  root <- fetch_fred_data(url)
  
  if (is.null(root) || length(xml_children(root)) == 0) {
    stop('No vintage date exists for series id: ', series_id)
  }
  
  vintage_nodes <- xml_children(root)
  dates <- character(length(vintage_nodes))
  
  for (i in seq_along(vintage_nodes)) {
    dates[i] <- xml_text(vintage_nodes[[i]])
  }
  
  parse_fred_date(dates)
}

# Helper function for series search
do_series_search <- function(url) {
  root <- fetch_fred_data(url)
  
  num_results_total <- as.numeric(xml_attr(root, "count"))
  
  series_nodes <- xml_children(root)
  num_results_returned <- length(series_nodes)
  
  if (num_results_returned == 0) {
    return(list(data = NULL, total = num_results_total))
  }
  
  # Extract series information
  series_data <- data.frame(
    id = character(num_results_returned),
    realtime_start = character(num_results_returned),
    realtime_end = character(num_results_returned),
    title = character(num_results_returned),
    observation_start = character(num_results_returned),
    observation_end = character(num_results_returned),
    frequency = character(num_results_returned),
    frequency_short = character(num_results_returned),
    units = character(num_results_returned),
    units_short = character(num_results_returned),
    seasonal_adjustment = character(num_results_returned),
    seasonal_adjustment_short = character(num_results_returned),
    last_updated = character(num_results_returned),
    popularity = character(num_results_returned),
    notes = character(num_results_returned),
    stringsAsFactors = FALSE
  )
  
  for (i in seq_along(series_nodes)) {
    attrs <- xml_attrs(series_nodes[[i]])
    series_data[i, "id"] <- attrs["id"]
    series_data[i, "realtime_start"] <- attrs["realtime_start"]
    series_data[i, "realtime_end"] <- attrs["realtime_end"]
    series_data[i, "title"] <- attrs["title"]
    series_data[i, "observation_start"] <- attrs["observation_start"]
    series_data[i, "observation_end"] <- attrs["observation_end"]
    series_data[i, "frequency"] <- attrs["frequency"]
    series_data[i, "frequency_short"] <- attrs["frequency_short"]
    series_data[i, "units"] <- attrs["units"]
    series_data[i, "units_short"] <- attrs["units_short"]
    series_data[i, "seasonal_adjustment"] <- attrs["seasonal_adjustment"]
    series_data[i, "seasonal_adjustment_short"] <- attrs["seasonal_adjustment_short"]
    series_data[i, "last_updated"] <- attrs["last_updated"]
    series_data[i, "popularity"] <- attrs["popularity"]
    series_data[i, "notes"] <- attrs["notes"]
  }
  
  # Parse date columns
  date_cols <- c("realtime_start", "realtime_end", "observation_start", "observation_end", "last_updated")
  for (col in date_cols) {
    series_data[[col]] <- parse_fred_date(series_data[[col]])
  }
  
  rownames(series_data) <- series_data$id
  
  list(data = series_data, total = num_results_total)
}

# Helper function for getting search results with pagination
get_search_results <- function(url, limit, order_by, sort_order) {
  order_by_options <- c('search_rank', 'series_id', 'title', 'units', 'frequency',
                        'seasonal_adjustment', 'realtime_start', 'realtime_end', 'last_updated',
                        'observation_start', 'observation_end', 'popularity')
  
  if (!is.null(order_by)) {
    if (order_by %in% order_by_options) {
      url <- paste0(url, '&order_by=', order_by)
    } else {
      stop(sprintf('%s is not in the valid list of order_by options: %s', 
                   order_by, paste(order_by_options, collapse = ", ")))
    }
  }
  
  sort_order_options <- c('asc', 'desc')
  if (!is.null(sort_order)) {
    if (sort_order %in% sort_order_options) {
      url <- paste0(url, '&sort_order=', sort_order)
    } else {
      stop(sprintf('%s is not in the valid list of sort_order options: %s', 
                   sort_order, paste(sort_order_options, collapse = ", ")))
    }
  }
  
  result <- do_series_search(url)
  data <- result$data
  num_results_total <- result$total
  
  if (is.null(data)) {
    return(data)
  }
  
  if (limit == 0) {
    max_results_needed <- num_results_total
  } else {
    max_results_needed <- limit
  }
  
  # Handle pagination
  if (max_results_needed > MAX_RESULTS_PER_REQUEST) {
    n_requests <- ceiling(max_results_needed / MAX_RESULTS_PER_REQUEST)
    
    for (i in 2:n_requests) {
      offset <- (i - 1) * MAX_RESULTS_PER_REQUEST
      next_result <- do_series_search(paste0(url, '&offset=', offset))
      next_data <- next_result$data
      
      if (!is.null(next_data)) {
        data <- rbind(data, next_data)
      }
    }
  }
  
  # Return only the requested number of results
  if (nrow(data) > max_results_needed) {
    data <- data[1:max_results_needed, ]
  }
  
  return(data)
}

# Search for series by text
fred_search <- function(text, limit = 1000, order_by = NULL, sort_order = NULL, api_key = NULL) {
  if (is.null(api_key)) {
    api_key <- get("FRED_API_KEY", envir = .GlobalEnv)
  }
  
  encoded_text <- URLencode(text, reserved = TRUE)
  url <- sprintf("http://api.stlouisfed.org/fred/series/search?search_text=%s&api_key=%s", 
                 encoded_text, api_key)
  
  get_search_results(url, limit, order_by, sort_order)
}

# Search for series by release
fred_search_by_release <- function(release_id, limit = 0, order_by = NULL, sort_order = NULL, api_key = NULL) {
  if (is.null(api_key)) {
    api_key <- get("FRED_API_KEY", envir = .GlobalEnv)
  }
  
  url <- sprintf("http://api.stlouisfed.org/fred/release/series?release_id=%d&api_key=%s", 
                 release_id, api_key)
  
  info <- get_search_results(url, limit, order_by, sort_order)
  
  if (is.null(info)) {
    stop('No series exists for release id: ', release_id)
  }
  
  return(info)
}

# Search for series by category
fred_search_by_category <- function(category_id, limit = 0, order_by = NULL, sort_order = NULL, api_key = NULL) {
  if (is.null(api_key)) {
    api_key <- get("FRED_API_KEY", envir = .GlobalEnv)
  }
  
  url <- sprintf("http://api.stlouisfed.org/fred/category/series?category_id=%d&api_key=%s", 
                 category_id, api_key)
  
  info <- get_search_results(url, limit, order_by, sort_order)
  
  if (is.null(info)) {
    stop('No series exists for category id: ', category_id)
  }
  
  return(info)
}
