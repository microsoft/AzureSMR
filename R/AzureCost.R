#' Get data consumption of an Azure subscription for a time period.

#' Aggregation method can be either daily based or hourly based.
#' 
#' Formats of start time point and end time point follow ISO 8601 standard. For example, if you want to calculate data consumption between Feb 21, 2017 to Feb 25, 2017, with an aggregation granularity of "daily based", the inputs should be "2017-02-21 00:00:00" and "2017-02-25 00:00:00", for start time point and end time point, respectively.
#' If the aggregation granularity is hourly based, the inputs can be "2017-02-21 01:00:00" and "2017-02-21 02:00:00", for start and end time point, respectively. 
#' NOTE by default the Azure data consumption API does not allow an aggregation granularity that is finer than an hour. In the case of "hourly based" granularity, if the time difference between start and end time point is less than an hour, data consumption will still be calculated hourly based with end time postponed.
#' For example, if the start time point and end time point are "2017-02-21 00:00:00" and "2017-02-21 00:45:00", the actual returned results are data consumption in the interval of "2017-02-21 00:00:00" and "2017-02-21 01:00:00". However this calculation is merely for retrieving the information of an existing instance instance (e.g. `meterId`) with which the pricing rate is multiplied by to obtain the overall expense. 
#' Time zone of all time inputs are synchronized to UTC.
#' 
#' @inheritParams setAzureContext
#' 
#' @param instance Instance name that one would like to check expense. It is by default empty, which returns data consumption for all instances under subscription.
#' 
#' @param timeStart Start time.
#' @param timeEnd End time.
#' @param granularity Aggregation granularity. Can be either "Daily" or "Hourly".
#'
#' @family Cost functions
#' @export
azureDataConsumption <- function(azureActiveContext,
                                 instance="",
                                 timeStart,
                                 timeEnd,
                                 granularity="Hourly",
                                 verbose=FALSE) {
  
  # check the validity of credentials.
  
  assert_that(is.azureActiveContext(azureActiveContext))
  
  # renew token if it expires.
  
  azureCheckToken(azureActiveContext)
  
  # preconditions here...
  
  if(missing(timeStart))
    stop("Please specify a starting time point in YYYY-MM-DD HH:MM:SS format.")
  
  if(missing(timeEnd))
    stop("Please specify an ending time point in YYYY-MM-DD HH:MM:SS format.")
  
  ds <- try(as.POSIXlt(timeStart, format= "%Y-%m-%d %H:%M:%S", tz="UTC"))
  de <- try(as.POSIXlt(timeEnd, format= "%Y-%m-%d %H:%M:%S", tz="UTC"))
  
  if (class(ds) == "try-error" ||
      is.na(ds) ||
      class(de) == "try-error" ||
      is.na(de))
    stop("Input date format should be YYYY-MM-DD HH:MM:SS.")
  
  timeStart <- ds
  timeEnd <- de
  
  if (timeStart >= timeEnd)
    stop("End time is no later than start time!")
  
  lubridate::minute(timeStart) <- 0
  lubridate::second(timeStart) <- 0
  lubridate::minute(timeEnd)   <- 0
  lubridate::second(timeEnd)   <- 0
  
  if (granularity == "Daily") {
    
    # timeStart and timeEnd should be some day at midnight.
    
    lubridate::hour(timeStart) <- 0
    lubridate::hour(timeEnd) <- 0
    
  }
  
  # If the computation time is less than a hour, timeEnd will be incremented by 
  # an hour to get the total cost within an hour aggregated from timeStart. 
  # However, only the consumption on computation is considered in the returned 
  # data, and the computation consumption will then be replaced with the actual 
  # timeEnd - timeStart.
  
  # NOTE: estimation of cost in this case is rough though, it captures the major
  # component of total cost, which originates from running an Azure instance. 
  # Other than computation cost, there are also cost on activities such as data
  # transfer, software library license, etc. This is not included in the 
  # approximation here until a solid method for capturing those consumption data
  # is found. Data ingress does not generate cost, but data egress does. Usually
  # the occurrence of data transfer is not that frequent as computation, and 
  # pricing rates for data transfer is also less than computation (e.g., price 
  # rate of "data transfer in" is ~ 40% of that of computation on an A3 virtual
  # machine).
  
  # TODO: inlude other types of cost for jobs that take less than an hour.
  
  if (as.numeric(timeEnd - timeStart) == 0) {
    writeLines("Difference between timeStart and timeEnd is less than the 
               aggregation granularity. Cost is estimated solely on computation
               running time.")
    
    # increment timeEnd by one hour.
    
    timeEnd <- timeEnd + 3600
  }
  
  # reformat time variables to make them compatible with API call.
  
  start <- URLencode(paste(as.Date(timeStart), 
                           "T",
                           sprintf("%02d", lubridate::hour(timeStart)), 
                           ":", 
                           sprintf("%02d", lubridate::minute(timeStart)), 
                           ":", 
                           sprintf("%02d", lubridate::second(timeStart)), 
                           "+",
                           "00:00",
                           sep=""),
                     reserved=TRUE)
  
  end <- URLencode(paste(as.Date(timeEnd), 
                         "T",
                         sprintf("%02d", lubridate::hour(timeEnd)), 
                         ":", 
                         sprintf("%02d", lubridate::minute(timeEnd)), 
                         ":", 
                         sprintf("%02d", lubridate::second(timeEnd)), 
                         "+",
                         "00:00",
                         sep=""),
                   reserved=TRUE)
  
  url <-
    paste0("https://management.azure.com/subscriptions/",
           azureActiveContext$subscriptionID,
           "/providers/",
           "Microsoft.Commerce/UsageAggregates?api-version=",
           "2015-06-01-preview",
           "&reportedStartTime=",
           start,
           "&reportedEndTime=",
           end,
           "&aggregationgranularity=",
           granularity,
           "&showDetails=",
           "false"
    )
  
  r <- call_azure_sm(azureActiveContext,
                     uri=url,
                     verb="GET",
                     verbose=verbose)
  
  stopWithAzureError(r)
  
  rl <- content(r, "text", encoding="UTF-8")
  
  df <- fromJSON(rl)
  
  df_use <- df$value$properties
  
  inst_data <- lapply(df$value$properties$instanceData, fromJSON)
  
  # retrieve results that match instance name.
  
  if (instance != "") {
    instance_detect <- function(inst_data) {
      return(basename(inst_data$Microsoft.Resources$resourceUri) == instance)
    }
    
    index_instance <- which(unlist(lapply(inst_data, instance_detect)))
    
    if(!missing(instance)) {
      if(length(index_instance) == 0)
        stop("No data consumption records found for the instance during the 
             given period.")
      df_use <- df_use[index_instance, ]
    } else if(missing(instance)) {
      if(length(index_resource) == 0)
        stop("No data consumption records found for the resource group during 
             the given period.")
      df_use <- df_use[index_resource, ]
    }
  }
  
  # if time difference is less than one hour. Only return one row of computation
  # consumption whose value is the time difference.
  
  # timeEnd <- timeEnd - 3600
  
  if(as.numeric(timeEnd - timeStart) == 0) {
    
    time_diff <- as.numeric(de - ds) / 3600
    
    df_use <- df_use[which(df_use$meterName == "Compute Hours"), ]
    df_use <- df_use[1, ]
    
    df_use$quantity <- df_use$time_diff
    
  } else {
    
    # NOTE the maximum number of records returned from API is limited to 1000.
    
    if (nrow(df_use) == 1000 && 
        max(as.POSIXct(df_use$usageEndTime)) < as.POSIXct(end)) {
      warning(sprintf("The number of records in the specified time period %s 
                      to %s exceeds the limit that can be returned from API call.
                      Consumption information is truncated. Please use a small 
                      period instead.", timeStart, timeEnd))
    }
  }
  
  df_use <- df_use[, c("usageStartTime",
                       "usageEndTime",
                       "meterName",
                       "meterCategory",
                       "meterSubCategory",
                       "unit",
                       "meterId",
                       "quantity",
                       "meterRegion")]
  
  df_use$usageStartTime <- as.POSIXct(df_use$usageStartTime)
  df_use$usageEndTime   <- as.POSIXct(df_use$usageEndTime)
  
  writeLines(sprintf("The data consumption for %s between %s and %s is",
                     instance,
                     as.character(timeStart),
                     as.character(timeEnd)))
  
  return(df_use)
}


#' Get pricing details of resources under a subscription.
#' 
#' The pricing rates function wraps API calls to Azure RateCard and currently the API supports only the Pay-As-You-Go offer scheme. 
#'
#' @inheritParams setAzureContext
#' 
#' @param currency Currency in which price rating is measured.
#' @param locale Locality information of subscription.
#' @param offerId Offer ID of the subscription. For more information see https://azure.microsoft.com/en-us/support/legal/offer-details/
#' 
#' @param region region information about the subscription.
#' 
#' @family Cost functions
#' @export
azurePricingRates <- function(azureActiveContext,
                              currency,
                              locale,
                              offerId,
                              region,
                              verbose=FALSE
) {
  # renew token if it expires.
  
  azureCheckToken(azureActiveContext)
  
  # preconditions.
  
  if(missing(currency))
    stop("Error: please provide currency information.")
  
  if(missing(locale))
    stop("Error: please provide locale information.")
  
  if(missing(offerId))
    stop("Error: please provide offer ID.")
  
  if(missing(region))
    stop("Error: please provide region information.")
  
  url <- paste(
    "https://management.azure.com/subscriptions/", 
    azureActiveContext$subscriptionID,
    "/providers/Microsoft.Commerce/RateCard?api-version=2016-08-31-preview&
    $filter=",
    "OfferDurableId eq '", offerId, "'",
    " and Currency eq '", currency, "'",
    " and Locale eq '", locale, "'",
    " and RegionInfo eq '", region, "'",
    sep="")
  
  url <- URLencode(url)
  
  r <- call_azure_sm(azureActiveContext,
                     uri=url,
                     verb="GET",
                     verbose=verbose)
  
  stopWithAzureError(r)
  
  rl <- fromJSON(content(r, "text", encoding="UTF-8"), simplifyDataFrame=TRUE)
  
  df_meter <- rl$Meters
  df_meter$MeterRate <- rl$Meters$MeterRates$`0`
  
  # NOTE: an irresponsible drop of MeterRates and MeterTags. Will add them back 
  # after having a better handle of them.
  
  df_meter <- subset(df_meter, select=-MeterRates)
  df_meter <- subset(df_meter, select=-MeterTags)
  
  names(df_meter) <- paste0(tolower(substring(names(df_meter), 
                                              1, 
                                              1)), 
                            substring(names(df_meter), 2))
  
  df_meter
}


#' Calculate cost of using a specific instance of Azure for certain period.
#' 
#'Note if difference between \code{timeStart} and \code{timeEnd} is less than the finest granularity, e.g., "Hourly" (we notice this is a usual case when one needs to be aware of the charges of a job that takes less than an hour), the expense will be estimated based solely on computation hour. That is, the total expense is the multiplication of computation hour and pricing rate of the requested instance.
#'
#' @inheritParams setAzureContext
#' @inheritParams azureDataConsumption
#' @inheritParams azurePricingRates
#' 
#' @return Total cost measured in the given currency of the specified Azure instance in the period.
#' 
#' @family Cost functions
#' @export
azureExpenseCalculator <- function(azureActiveContext,
                                   instance="",
                                   timeStart,
                                   timeEnd,
                                   granularity,
                                   currency,
                                   locale,
                                   offerId,
                                   region,
                                   verbose=FALSE) {
  df_use <- azureDataConsumption(azureActiveContext,
                                 instance=instance,
                                 timeStart=timeStart,
                                 timeEnd=timeEnd,
                                 granularity=granularity,
                                 verbose=verbose) 
  
  df_used_data <- df_use[, c("meterId",
                             "meterSubCategory",
                             "usageStartTime",
                             "usageEndTime",
                             "quantity")]
  
  # use meterId to find pricing rates and then calculate total cost.
  
  df_rates <- azurePricingRates(azureActiveContext,
                                currency=currency,
                                locale=locale,
                                region=region,
                                offerId=offerId,
                                verbose=verbose)
  
  meter_list <- unique(df_used_data$meterId)
  
  df_used_rates <- df_rates[which(df_rates$meterId %in% meter_list), ]
  df_used_rates$meterId <- df_used_rates$meterId
  
  # join data consumption and meter pricing rate.
  
  df_merged <- merge(x=df_used_data,
                     y=df_used_rates,
                     by="meterId",
                     all.x=TRUE)
  
  df_merged$meterSubCategory <- df_merged$meterSubCategory.y
  df_merged$cost <- df_merged$quantity * df_merged$meterRate
  
  df_cost <- df_merged[, c("meterName",
                           "meterCategory",
                           "meterSubCategory",
                           "quantity",
                           "unit",
                           "meterRate",
                           "cost")]
  
  names(df_cost) <- paste0(tolower(substring(names(df_cost), 
                                             1, 
                                             1)), 
                           substring(names(df_cost), 2))
  
  df_cost
}
