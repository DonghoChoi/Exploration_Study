setwd("~/Documents/Work/Exploration_Study/Dissertation/Data")
ad<-read.csv("aggregate_data.csv")

ad$gender_d <-0
ad$gender_d[ad$gender=="M"]<-1

library(ggplot2)

# Location loyalty and Useful Coverage
scatter_location_loyalty <- ggplot(ad,aes(location_loyalty,UsefulCoverage,color=gender))
scatter_location_loyalty + geom_point() + labs(x="Location Loyalty",y="Useful Coverage",color="gender") + geom_smooth(method="lm",aes(fill=gender),alpha=0.1)

# dummy variable coding using riskB
ad$riskB_d1 <-0
ad$riskB_d0[ad$riskB==3] <- 1
ad$riskB_d2 <-0
ad$riskB_d0[ad$riskB==2] <- 1
ad$riskB_d3 <-0
ad$riskB_d0[ad$riskB==1] <- 1

library(plyr)
count(ad_m,riskB)
count(ad_m, c('riskB','timeB'))

ad$riskB_factor <- as.factor(ad$riskB)

riskB_with_location <- lm(UsefulCoverage ~ location_loyalty+riskB_factor,data=ad)
summary(riskB_with_location)

riskB <- lm(UsefulCoverage ~ riskB_factor,data=ad)
summary(riskB)

riskB_m <- lm(UsefulCoverage ~ riskB_factor,data=ad_m)
summary(riskB_m)

riskB_f <- lm(UsefulCoverage ~ riskB_factor,data=ad_f)
summary(riskB_f)

ad$selection <- ad$UsefulCoverage/ad$Coverage
ad_m$selection <- ad_m$UsefulCoverage/ad_m$Coverage
ad_f$selection <- ad_f$UsefulCoverage/ad_f$Coverage


