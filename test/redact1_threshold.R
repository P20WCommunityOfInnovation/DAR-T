#' Redact 1 with Threshold Option
#'
#' Redact all counts of 1 variable less than {{threshold}} and suppress if necessary.
#'
#' @param df A data frame of data, either raw data or aggregated. No NA values allowed.
#' @param var1 Variable of interest
#' @param pre_agg FALSE if this is the raw data frame (default),
#'     TRUE if data is already aggregated
#' @param counts character If pre_agg = T, this is the name of the column with the counts.
#' @param ignore_redact_col Do not touch this unless you know what you are doing.
#'     If gives the ability to redact values at the outset
#'     that might not otherwise be redacted, indicated by TRUE. If TRUE there must be a column
#'     called redact with only 0 and 1.
#'
#' @param threshold gives the minimum number allowed to be shown. KYSTATS default is 10.
#'
#' @return Aggregated data frame with var1 values, column of counts of var1 called size,
#'     and a redact indicator column of 0 and 1.
#' @export
#'
#' @examples
#' df <- data.frame(first_col = sample(0:5, 300, rep = TRUE),
#'      letters = sample(LETTERS[1:25], 300, rep=TRUE))
#'
#' redact1_threshold(df, letters, threshold = 8)

redact1_threshold <- function(df,
                        var1,
                        pre_agg = F,
                        counts = NA,
                        ignore_redact_col = T,
                        threshold = 10){


# Warnings ----------------------------------------------------------------


  if({{pre_agg}} & is.na({{counts}})){
    return("You must provide a counts column for a pre-aggregated data frame.")
    
  }

  #Because of the output, we want no NA values for the grouping variable.
  if({{df}} %>% filter(is.na({{var1}})) %>% nrow() !=0){
    return("Variable values should not include NA.")
    
  }

  #If it says to use an already populated redact column, check that it exists and only has 0/1.
  if(!{{ignore_redact_col}}){

    if(!("redact" %in% colnames({{df}}))){
    return("Beware of ignore_redact_col! If ignore_redact_col = F,\nthen a column called redact with 0s and 1s must be included.")
  }

    else if(nrow({{df}} %>% filter(redact %in% c(0,1))) != nrow({{df}})){
      return("Beware of ignore_redact_col! If ignore_redact_col = F,\nthen the redact column must only contain 0s and 1s, no other values or NAs.")
  }
}



  # Aggregate the raw data frame input --------------------------------------

  #If raw data frame that does not have a redact column already, then aggregate it.
  if(!{{pre_agg}} & {{ignore_redact_col}}){
    df<- {{df}} %>%
      group_by({{var1}}) %>%
      summarise(.groups="drop",
                size    = n())}

  #If a raw data frame where we want to use a previously populated redact column,
  #aggregate and flag as redact if there is a redact flag for any value in the group.
  else if(!{{pre_agg}} & !{{ignore_redact_col}}){
    df<- {{df}} %>%
      group_by({{var1}}) %>%
      summarise(.groups="drop",
                size    = n(),
                redact  = max(redact, na.rm = T))}


  #Be sure no duplicated combinations in already aggregated data frames.
  #If it is preaggregated, make sure there are no duplicate combinations of grouping variables.
  else if (({{df}} %>% pull({{var1}}) %>% unique() %>% length()) !=
           ({{df}} %>% pull({{var1}}) %>% length())){

    return("Your pre-aggregated data frame has duplicate combinations. Either set pre_agg = F if this is really the raw data frame or remove duplicate combinations.")
    
  }

  #Make sure you have the counts column renamed to size if input was pre-agged.
  #Remove rows with counts of 0. This may change in the future.
  #else{df <- {{df}} %>% rename(size = {{counts}}) %>% filter(size != 0)}
  else{df <- {{df}} %>% rename(size = {{counts}}) %>% filter(!is.na(size))}


  if (!{{ignore_redact_col}}){
    df <- df %>% select({{var1}}, size, redact) %>%
      mutate(redact = ifelse(is.na(redact), 0, redact))
    }

  else{df <- df %>%
    select({{var1}}, size) %>% mutate(redact = 0)
  }

  #Every thing up to this point was to set up the aggregated data frame.
  #Should have an aggregated data frame at this point with three columns:
  #{{var1}}, size, redact


# Actual redaction starts here --------------------------------------------


# Direct Redaction --------------------------------------------------------

  #Any value less than {{threshold}} must be redacted.
 #The counts of zero have been filtered out. If one day you want to change that
 #you should rethink this step.
  df_round1_redact <- df %>%
    mutate(redact            = ifelse(size   < {{threshold}},  1, redact),
           size_not_redacted = ifelse(redact == 1, NA, size))



# Suppressive Redaction ---------------------------------------------------

  #If all the entries or none of the entries are redacted, then move on.
  #Otherwise check if suppressive redaction needs to be done.
  if(sum(df_round1_redact$redact, na.rm=T) %in% c(0, nrow(df_round1_redact))){
    df_final_redact <- df_round1_redact}

  if(between(sum(df_round1_redact$redact, na.rm=T), 1, nrow(df_round1_redact)-1)){

    df_final_redact <- df_round1_redact %>%
      mutate(var1_num_redacted  = sum(redact, na.rm = T),
             var1_size_redacted = sum(redact*size, na.rm = T)) %>%
      mutate(redact = case_when((var1_num_redacted == 1 |
                                   (var1_size_redacted < {{threshold}} & var1_num_redacted >0)) &
                                  size == min(size_not_redacted, na.rm=T) ~ 1,
                                TRUE ~ redact))
  }


# Write out. Redaction completed ------------------------------------------

  #All redaction is done. This goes back to the original aggregated data frame.
  #Joins the redaction flags.

  df %>%
    select({{var1}}, size) %>%
    left_join(df_final_redact %>%
                select({{var1}}, redact)) %>%
    mutate(redact = ifelse(is.na(redact), 0, redact))

  }  %>%  suppress_messages("Joining") %>% suppress_warnings("returning Inf")
