#' Backup script to convert Viollier sample metadata to Harvester format (required by the sequencing centers)
#' 
#' - Only removes rows without sample number
#' - ETHID is set to sample number
#' - Does not fill in missing rows on the plate
viollier_to_harvester <- function(filename) {
  
    df  <- read.csv(filename, sep=";")
    res <- data.frame(ethid               = df$Sample.number, 
                      order_date          = df$Order.date, 
                      ct                  = df$CT.Wert, 
                      viollier_plate_name = tolower(df$PlateID),
                      #sequencing_center   = df$Sequencing.center,
                      well_position       = df$DeepWellLocation, 
                      well_letter         = substr(df$DeepWellLocation, 1, 1),
                      well_number         = as.numeric(substr(df$DeepWellLocation, 2, 3)),
                      id_and_well         = paste(df$Sample.number, df$DeepWellLocation, sep='_'))
    
    # Remove samples without sample number (apparently we don't care about non-numeric Ct values)
    remove <- which(is.na(res$ethid)) # | is.na(res$ct))
    cat(paste0(filename, ": Removed ", length(remove), " samples without sample number\n"))
    if (length(remove) > 0) {
        res <- res[-remove, ]
    }
    
    res <- res[with(res, order(viollier_plate_name, well_number, well_letter)), ]
    res$well_letter <- res$well_number <- NULL
    
    return(res)
}

# Example usage:
#
# write.csv(viollier_to_harvester("2021-08-09/FGCZ/2021-08-06--08-09 FGCZ SARS-CoV-2 samples.csv"), 
#           file = "2021-08-09/FGCZ/2021-08-06--08-09 FGCZ SARS-CoV-2 samples_corrected.csv", 
#           row.names=FALSE)
# 
# write.csv(viollier_to_harvester("2021-08-09/H2030/Health2030-2021-08-09.csv"), 
#           file = "2021-08-09/H2030/Health2030-2021-08-09_corrected.csv", 
#           row.names=FALSE)