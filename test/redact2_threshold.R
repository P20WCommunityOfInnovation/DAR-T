#' Redact 2 with Threshold Options
#'
#' Redact all counts of combinations of variables less than {{threshold}} and suppress if necessary.
#'
#' @param df A data frame of data, either raw data or aggregated. No NA values allowed.
#' @param var1 First variable of interest
#' @param var2 Second variable of interest. The order of the variables matters
#' @param pre_agg FALSE if this is the raw data frame (default),
#'     TRUE if data is already aggregated
#' @param counts character If pre_agg = T, this is the name of the column with the counts.
#' @param var1_redact_zero FALSE if zeros should NOT be redacted for var1,
#'     TRUE if zeros should be redacted for var1. Defaults to FALSE.
#' @param var2_redact_zero FALSE if zeros should NOT be redacted for var2,
#'     TRUE if zeros should be redacted for var2. Defaults to FALSE.
#' @param ignore_redact_col Do not touch this unless you know what you are doing.
#'     If gives the ability to redact values at the outset
#'     that might not otherwise be redacted, indicated by TRUE. If TRUE there must be a column
#'     called redact with only 0 and 1.
#'     Only works for combos of 2 variables. Does not redact up to 1 variable.
#' @param threshold gives the minimum number allowed to be shown. KYSTATS default is 10.
#'
#'
#' @return Aggregated data frame with var1 values, var2 values,
#'     column of counts of combinations called size,
#'     and a redact indicator column of 0 and 1.
#'     Counts columns only includes 0s if var1_redact_zero = T or var2_redact_zero = T
#'     Also gives results of redact2 and redact1 for var1 and var2.
#'     NA appear in the remaining column.
#' @export
#'
#' @examples
#' df <- data.frame(first_col = sample(0:5, 1000, rep = TRUE),
#'      letters = sample(LETTERS[1:25], 1000, rep=TRUE))
#'
#' redact2_threshold(df, first_col, letters, threshold = 5)
#'
#' df_agg <- df %>% group_by(first_col, letters) %>% summarise(counts = n(), .groups = "drop")
#'
#' redact2_threshold(df_agg, first_col, letters, pre_agg = T, counts = "counts", threshold = 3)



redact2_threshold <- function(df,
                        var1,
                        var2,
                        pre_agg = F,
                        counts = NA,
                        var1_redact_zero = F,
                        var2_redact_zero = F,
                        ignore_redact_col = T,
                        threshold = 10){


# Warnings ----------------------------------------------------------------


  if({{pre_agg}} & is.na({{counts}})){
    return("You must provide a counts column for a pre-aggregated data frame.")
    
  }


  if({{df}} %>% filter(is.na({{var1}}) | is.na({{var2}})) %>% nrow() !=0){
    return("Variable values should not include NA.")
    
  }

  #If it says to use an already populated redact column, check that it exists and only has 0/1.
  if(!{{ignore_redact_col}}){

    if(!("redact" %in% colnames({{df}}) ) ){
      return("Beware of ignore_redact_col! If ignore_redact_col = F,\nthen a column called redact with 0s and 1s must be included.")}

    else if(nrow({{df}} %>% filter(redact %in% c(0,1))) != nrow({{df}})){
      return("Beware of ignore_redact_col! If ignore_redact_col = F,\nthen the redact column must only contain 0s and 1s, no other values or NAs.")
    }
  }


# Aggregate the raw data frame input --------------------------------------


  if(!{{pre_agg}} & {{ignore_redact_col}}){
    df<- {{df}} %>%
      group_by({{var1}}, {{var2}}) %>%
      summarise(.groups="drop",
                size    = n())}

  else if(!{{pre_agg}} & !{{ignore_redact_col}}){
    df<- {{df}} %>%
      group_by({{var1}}, {{var2}}) %>%
      summarise(.groups="drop",
                size    = n(),
                redact = max(redact, na.rm = T))}

  #Be sure no duplicated combinations in already aggregated data frames.

  else if(({{df}}  %>% select({{var1}}, {{var2}}) %>% distinct() %>% nrow()) !=
           ({{df}} %>% select({{var1}}, {{var2}}) %>% nrow())){

    return("Your pre-aggregated data frame has duplicate combinations. Either set pre_agg = F if this is really the raw data frame or remove duplicate combinations.")
    
  }


  #Make sure you have the counts column renamed to size.

  else{df <- {{df}} %>% rename(size = {{counts}}) %>% filter(!is.na(size))}



  if (!{{ignore_redact_col}}){
    if(!("redact" %in% colnames(df))){
      return("If ignore_redact_col = F, there must be a column called redact with 0 and 1.")
    }
    else{df<-df %>% select({{var1}}, {{var2}}, size, redact)}
    }


  else{df<-df %>%
    select({{var1}}, {{var2}}, size) %>% mutate(redact = 0)
  }


# # Include Zeros for Redaction ---------------------------------------------

  if({{var1_redact_zero}} & {{var2_redact_zero}}){
    df <- complete(df, {{var1}}, {{var2}}, fill = list(size=0, redact = 0))}

  else if({{var1_redact_zero}} | {{var2_redact_zero}}){

    dont_redact_zeros <- c(!{{var1_redact_zero}}, !{{var2_redact_zero}}, F, F)
    redact_zeros      <- c( {{var1_redact_zero}},  {{var2_redact_zero}}, F, F)

    df <-complete(df,
                  nesting(df[dont_redact_zeros]),
                  df[redact_zeros],
                  fill = list(size=0, redact = 0))
  }



  #Should have an aggregated data frame at this point with four columns:
  #{{var1}}, {{var2}}, size, redact



# First Level Redaction ---------------------------------------------------

#Redact1 on var1----
  df1 <- df %>%
  group_by({{var1}}) %>%
  summarise(.groups = "drop",
            size    = sum(size))


red1_var1 <- redact1_threshold(df1,
                               {{var1}},
                               pre_agg = T,
                               counts = "size",
                               ignore_redact_col = T,
                               threshold = {{threshold}})

#Prepping to make sure that everything that was required on 1 variable to redact also is redacted on 2.
red1_var1_for_trickle_down <- red1_var1 %>% select({{var1}}, redact) %>% rename(redact1 = redact)

#Redact1 on var2----
df2 <- df %>%
  group_by({{var2}}) %>%
  summarise(.groups = "drop",
            size    = sum(size))


red1_var2 <- redact1_threshold(df2,
                               {{var2}},
                               pre_agg = T,
                               counts = "size",
                               ignore_redact_col = T,
                               threshold = {{threshold}})

#Prepping to make sure that everything that was required on 1 variable to redact also is redacted on 2.
red1_var2_for_trickle_down <- red1_var2 %>% select({{var2}}, redact) %>% rename(redact2 = redact)

# Join the first level to agg df ------------------------------------------

df <- df %>%
  left_join(red1_var1_for_trickle_down) %>%
  left_join(red1_var2_for_trickle_down)  %>%
  rowwise() %>%
  mutate(redact = max(redact, redact1, redact2, na.rm=T)) %>%
  select(-redact1, -redact2)

# Second Level Redaction --------------------------------------------------

og <- df %>%
  mutate(redact            = ifelse(size   < {{threshold}},  1, redact)) %>%
  mutate(size_not_redacted = ifelse(redact == 1, NA, size))

# Suppressive Redaction ---------------------------------------------------

#Make sure that (1) any redaction needs to be done and if so,
# (2) there is another combo to redact for suppressive redaction.
# if (sum(og$redact) == 0){return(og %>% select(-size_not_redacted))}

if (sum(og$redact) %in% c(0, nrow(og))){
  return(data.table::rbindlist(
  list(og %>%
         select(-size_not_redacted),
         red1_var1,
       red1_var2), fill = T))
}

og<- og %>%
    group_by({{var1}}) %>% mutate(var1_row_has_redaction = ifelse(sum(redact, na.rm=T) == 0, 0, 1)) %>% ungroup() %>% #Flag any row of variable 1 if ANY entry in the row is redacted
    group_by({{var2}}) %>% mutate(var2_row_has_redaction = ifelse(sum(redact, na.rm=T) == 0, 0, 1))


   # What are the places where they are in both a row and column have redaction?
  # These are the only places we consider for suppressive redaction
  # unless there is an issue, which checked for in the next step.

  potentials_for_suppression <- og %>%
    filter((var1_row_has_redaction == 1 & var2_row_has_redaction == 1)) %>%
    select({{var1}}, {{var2}}, size, redact)

  # Make sure that among the redacted and further possibilities to choose from for more redaction,
  # there more than 1 in each row/column and at least {{threshold}} total for redaction.
  # Otherwise we need to add more to our "potentials" list.
  #In fact, whatever we add, go ahead and redact it, don't just mark it as potential.

  check_for_more_suppression_needed <- potentials_for_suppression %>%
    #All entries where at least one entry in its row and one entry in its column already flagged to be redacted.
    group_by({{var1}}) %>%
    mutate(number_var1         = n(),       #How many rows actually have a redaction. Need this to be at least 2.
           size_redacted_var1  = sum(size), #Are the entries chosen as potential have enough to have a sum more than {{threshold}}? This can cause warnings if all entries in the row are redacted.
           more_redaction_needed_var1 = ifelse((size_redacted_var1 < {{threshold}} & number_var1 > 0) |
                                                number_var1 == 1,
                                        1, 0)) %>% #Step 5: If either of above are true we need more potential entries for redaction.----
  ungroup() %>%
    group_by({{var2}}) %>% #Repeat above for var2.
    mutate(number_var2         = n(),
           size_redacted_var2  = sum(size),
           more_redaction_needed_var2 = ifelse((size_redacted_var2 < {{threshold}} & number_var2 > 0) |
                                                number_var2 == 1,
                                        1, 0 )) %>% #Step 5: If either of above are true we need more potential entries for redaction.----
  ungroup()

  #Is more redaction actually needed due to only one entry being redacted in a row or
  #less than {{threshold}} being redacted total?

  #No more needed, go on to suppression
  if(nrow(check_for_more_suppression_needed %>% filter(more_redaction_needed_var1 == 1)) == 0 &
     nrow(check_for_more_suppression_needed %>% filter(more_redaction_needed_var2 == 1)) == 0){
    ready_for_suppression <- potentials_for_suppression
  }

  #Yes more is needed. Redact for sure one more in the row or column that needs more.
  #Then will have to do another cross to find all rows and columns that need extra redaction
  #and their potential entries.
  #To find one  more entry to redact in a row so requirements are satisfied,
  #choose the one with the smallest column sum.

  else{
    var1_row_more_redact <- og %>%
      left_join(check_for_more_suppression_needed %>%
                  filter(more_redaction_needed_var1 == 1) %>%
                  select({{var1}}, more_redaction_needed_var1)) %>%
      distinct() %>%
      group_by({{var2}}) %>%
      mutate(total_size = sum(size)) %>%
      ungroup() %>%
      filter(var2_row_has_redaction != 1 & more_redaction_needed_var1 == 1) %>%
      group_by({{var1}}) %>%
      filter(total_size == min(total_size)) %>%
      ungroup() %>%
      mutate(redact1 =  1) %>%
      select({{var1}}, {{var2}}, redact1)

    var2_row_more_redact <- og %>%
      left_join(check_for_more_suppression_needed %>% filter(more_redaction_needed_var2 == 1) %>%
                  select({{var2}}, more_redaction_needed_var2)) %>%
      distinct() %>%
      group_by({{var1}}) %>%
      mutate(total_size = sum(size)) %>%
      ungroup() %>%
      filter(var1_row_has_redaction != 1 & more_redaction_needed_var2 == 1) %>%
      group_by({{var2}}) %>%
      filter(total_size == min(total_size)) %>%
      ungroup() %>%
      mutate(redact2 =  1) %>%
      select({{var1}}, {{var2}}, redact2)


    og <- og %>% left_join(var1_row_more_redact) %>% left_join(var2_row_more_redact) %>%
      rowwise() %>% mutate(redact = max(redact, redact1, redact2, na.rm = T)) %>%
      select(-redact1, -redact2)


#Now that we have added in all the corner cases that we need,
    #we will rederive the potential places for suppression.
    og<- og %>%
      group_by({{var1}}) %>% mutate(var1_row_has_redaction = ifelse(sum(redact, na.rm = T) == 0, 0, 1)) %>% ungroup() %>% #Flag any row of variable 1 if ANY entry in the row is redacted
      group_by({{var2}}) %>% mutate(var2_row_has_redaction = ifelse(sum(redact, na.rm = T) == 0, 0, 1)) %>% ungroup() #Same for variable 2.

    # What are the places where they are in both a row and column have redaction?
    # These are the only places we consider for suppressive redaction


    ready_for_suppression <- og  %>%
      filter(var1_row_has_redaction == 1, var2_row_has_redaction == 1) %>%
      select({{var1}}, {{var2}}, size, redact)


  }

  final <- ready_for_suppression %>%
    group_by({{var1}}) %>%
    mutate(num_var1_redacted     = sum(redact),
           size_var1_redacted    = sum(redact*size),
           min_not_redacted_var1 = min(size[redact==0])) %>%
    ungroup() %>%
    mutate(redact = ifelse(num_var1_redacted == 1 & redact == 0 & size == min_not_redacted_var1,
                           1,
                           redact)) %>%
    mutate(redact = ifelse(size_var1_redacted < {{threshold}} & redact == 0 & size == min_not_redacted_var1,
                           1,
                           redact)) %>%
    select(-contains("_var1")) %>%
    group_by({{var2}}) %>%
    mutate(num_var2_redacted     = sum(redact),
           size_var2_redacted    = sum(redact*size),
           min_not_redacted_var2 = min(size[redact==0])) %>%
    ungroup() %>%
    mutate(redact = ifelse(num_var2_redacted == 1 & redact == 0 & size == min_not_redacted_var2,
                           1,
                           redact)) %>%
    mutate(redact = ifelse(size_var2_redacted < {{threshold}} & redact == 0 & size == min_not_redacted_var2,
                           1,
                           redact)) %>%
    select(-contains("_var2"))



  data.table::rbindlist(
  list(og %>%
    select({{var1}}, {{var2}}, size) %>%
    left_join(final %>% select({{var1}}, {{var2}}, redact)) %>%
    mutate(redact = ifelse(is.na(redact), 0, redact)),
    red1_var1,
    red1_var2), fill = T)


  } %>%  suppress_messages("Joining") %>%  suppress_warnings("returning Inf")




