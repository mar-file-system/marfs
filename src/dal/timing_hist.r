#! /usr/bin/env Rscript
getmode <- function(d){
  uniqd <- unique(d)
  uniqd[which.max(tabulate(match(d, uniqd)))]
}

args <- commandArgs(trailingOnly = TRUE)
filename <- args[1]
d<-scan(filename, quiet=TRUE)

png(paste(basename(args[1]), ".png", sep=""), width = 960, height = 480)
h <- hist(d, plot=TRUE, main = paste("total:", round(length(d), digits=6), ", min:", round(min(d), digits=6), "s, max:", round(max(d), digits=6), "s, median:", round(median(d), digits=6), "s, mode:", round(getmode(d), digits=6), "s, mean:", round(mean(d), digits=6), "s, variance:", round(var(d), digits=6), "s\n", sep=""), xlab = paste(basename(args[1]), "(s)"))
