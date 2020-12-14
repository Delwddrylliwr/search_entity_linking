#author - Tom Wilkinson
#goal - Linking delivery partner names to establish entities, 
#       scraping search engine results to make use of their reconciliation of acronyms and common mispellings
#resources: rvest - https://www.analyticsvidhya.com/blog/2017/03/beginners-guide-on-web-scraping-in-r-using-rvest-with-hands-on-knowledge/
#         object oriented R - http://adv-r.had.co.nz/OO-essentials.html - but note that method definition within the setRefClass call seems to have been deprecated, you have to use the separate $methods() function instead


library(methods)
library(rvest)
library(httr)
library(stringr)
library(gdata)


# @TODO move these global parameters to a config file
setwd("C:\\Users\\t-wilkinson\\Downloads")
partner_list_filename <- "partner_names.csv"
#The number of URLs to compare
num.comparison.urls <- 3
#How similar do the partners have to be?
min.similarity <- 1
#How will URLs be compared?
#string_comparator <- function(a,b){return(a == b)}
string_comparator <- function(a,b){
  a.domain <- str_match(a, "://(.*?)/")[[2]]
  b.domain <- str_match(b, "://(.*?)/")[[2]]
  return(a.domain == b.domain)
} #compares domains only
#We'll include some extra terms in every search to specify the context of internationa development
context.terms <- "development+"
search.domain <- "https://www.google.co.uk/search?q=" # Bing: "http://www.bing.com/search?q="
search.xpath <- "//h3[contains(@class, 'r')]//a" # Bing: "//body//div//ol//li//h2//a"
extract_search_result <- function(search.result) {
  result.url <- html_attr(search.result, name = "href")
  #Exclude google's referrals to other search cateories like photos
  if(length(grep("search?", result.url)) > 0) return(NULL)
  return(result.url)
}


#Create a simple class for each partner, containing the URLs retrieved from search, and the entities we've assigned them to
Partner <- setRefClass("Partner", 
                       fields = list(source.id = "character", name = "character", urls = "vector", entities = "vector")
)
Partner$methods(
  add_url = function(new.url) {
    
    urls <<- append(urls, new.url)
  },
  entify = function(entity) {
    entities <<- append(entities, entity)
  },
  shared_entities = function(other.partner){
    shared.entities <- list()
    for(entity in entities){
      if(entity$has_member(other.partner)){
        shared.entities <- append(shared.entities, entity)
      }
    }
    return(shared.entities)
  }
)


#Create a function for checking how many urls are shared by a pair of Partner objects
compare_partners <- function(partner1, partner2, string.comparator) {
  match.count <- 0
  p1.urls <- partner1$urls
  p2.urls <- partner2$urls
  
  #Compare all urls
  for(p1.url in p1.urls){
    for(p2.url in p2.urls){
      if(string_comparator(p1.url, p2.url)){
        match.count <- match.count + 1
        #p1.urls <- p1.urls[!(p1.urls == p1.url)] #only count whether this matches ANY rather than ALL
        p2.urls <- p2.urls[!(p2.urls == p2.url)] #only count whether this matches ANY rather than ALL
        break
      }
    }
  }
  
  return(match.count)
}


#Create a simple class for an entity, containing a UID, the member name UIDs and a minimum match strength
Entity <- setRefClass("Entity",
                      fields = list(members = "vector", min.match.score = "numeric")
)
Entity$methods(
  add_member = function(new.member, match.score){
    # @TODO check that the offered new.member is of type Partner
    members[[new.member$name]] <<- new.member
    
    #Is this a worse match than any so far?
    if(match.score < min.match.score){
      min.match.score <<- match.score
    }
  },
  has_member = function(possible.member){
    return(possible.member$source.id %in% names(members))
  }
)


#Create a function for adding a partner to another partner's entities
# @TODO intitially we'll make this transitive, but should build non-transitive too
# @TODO this isn't great OOP: it shouldn't be the primary way for us to add a partner to an entity 
join_entities <- function(partner, other.partner, entities, similarity){
  #Make sure these haven't already been entified
  if(length(other.partner$shared_entities(partner)) == 0){
    #Let's add these partners to the entity
    new.entity.members <- list()
    new.entity.members[[partner$source.id]] <- partner
    new.entity.members[[other.partner$source.id]] <- other.partner
    new.entity <- Entity$new(members = new.entity.members, min.match.score = similarity)
    
    #Let's replace every entity that each belongs to with this single entity
    for(old.entity in partner$entities){
      for(old.partner in old.entity$members){
        if(old.partner$source.id %in% names(new.entity$members)){} else{
          new.entity$add_member(old.partner, old.entity$min.match.score)
          old.partner$entities <- list(new.entity)
        }
      }
      ##If this entity was of less certainty, then the new one will inherit that
      #if(old.entity$min.match.score < similarity){
      #  new.entity$min.match.score <- old.entity$min.match.score
      #}
    }
    entities <- entities[!(entities %in% partner$entities)]
    partner$entities <- list(new.entity)
    for(old.entity in other.partner$entities){
      for(old.partner in old.entity$members){
        if(old.partner$source.id %in% names(new.entity$members)){} else{
          new.entity$add_member(old.partner, old.entity$min.match.score)
          old.partner$entities <- list(new.entity)
        }
      }
    }
    entities <- entities[!(entities %in% other.partner$entities)]
    other.partner$entities <- list(new.entity)
    return(append(entities, new.entity))
  }
  return(entities)
}


construct_url <- function(partner.name){
  
  #If the name is uppercase, let's add context terms to disambiguate from other acronyms
  if( grepl("^[[:upper:]]+$", gsub("[[:punct:] ]", "", partner.name))){
    return(paste(search.domain, context.terms, gsub("[[:punct:] ]", "+", partner.name), sep = "+"))
  }
  #Otherwise, we won't risk pulling up more general results by the context terms
  return(paste(search.domain, gsub("[[:punct:] ]", "+", partner.name), sep = "+"))
}


#AND NOW THE WORK BEGINS

# @TODO can these be assigned as vectors of objects instead? That should be more efficient...
partners <- list()
entities <- list()

# @TODO Read in the list of different partner names from the delivery network data
#this will need input on the database details from Fraser
partner.names <- read.csv(partner_list_filename, colClasses = c("character", "character"))

#Cycle through the list of partner names - apparently for loops are bad form in R, but given that we're working on a mutable list of partners, we don't want the underlying C trying to parallelise this!
for(row.index in 1:nrow(partner.names)){
  row <- partner.names[row.index,]
  
  #Create a partner object
  partner <- Partner$new(name = row$Name, source.id = row$ID, urls = vector("character"), entities = list())
  
  #Creating a url to search for this partner name
  #We'll strip the punctuation from the partner name, to avoid dealing with
  search.url <- construct_url(partner$name)
  
  #Reading the HTML code from the website - establishing a proxy
  search.results.page <- read_html(html_session(search.url, use_proxy("ek-proxy.dfid.gov.uk:8080")))
  
  #Extract URLs for the top five matches
  search.results <- html_nodes(search.results.page, xpath = search.xpath) #for Google results
  for(result.idx in 1:min(num.comparison.urls, length(search.results))){
    partner$add_url(extract_search_result(search.results[result.idx]))
  }
  
  
  #Cycle through the partners searched so far, and see if this matches any of them well enough to constitute part
  #of the same entity
  # @TODO set up for non-transitive entities too
  for(other.partner in partners){
    #Compare these two partners
    similarity <- compare_partners(partner, other.partner, string_comparator)
    
    if(similarity >= min.similarity){
      entities <- join_entities(partner, other.partner, entities, similarity)
    }
  }
  
  #Add this partner to the list for future comparison
  partners[[partner$source.id]] <- partner
  
  #Let's wait a little, so that we don't spook Google with botlike behaviour
  show(row.index)
  Sys.sleep(3)
}


#Export the list of entities found, in terms of the source identifiers
output <- vector("character")
for(entity in entities){
  output.entity <- ""
  for(member in entity$members){
    output.entity <- paste(output.entity, member$source.id, sep = "~")
  }
  output <- append(output, output.entity)
}

# @TODO return the output to the database, for the visualisation tool to pick up
write_stream()
