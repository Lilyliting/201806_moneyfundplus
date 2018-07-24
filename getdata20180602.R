# initial settings -----------------

setwd('C:\\Users\\Administrator\\Desktop\\Periodly_invest_20180502')

library(RMySQL)
library(quantmod)
library(rJava)
library(xlsx)
library(lubridate)
library(reshape2)
library(WindR)

w.start(showmenu = F)
conwind <- dbConnect(MySQL(), host='rm-2zey1z6px42nits51.mysql.rds.aliyuncs.com',
                     port=3306,dbname="wind", username="hulitingali", password="Hu.lt@2018")

# benchmark --------------------
# stkfund & bondfond index

# fundidx <- w.wsd("885001.WI,885005.WI","close","2003-12-31","2018-04-30","Fill=Previous")
# write.csv(fundidx$Data,file = '.\\Data\\fundindex20180430.csv',row.names = F)

# HS300

query <- 'select TRADE_DT, S_DQ_CLOSE from aindexeodprices
where S_INFO_WINDCODE =\'000300.SH\'
and TRADE_DT >= \'20031231\' order by TRADE_DT;'
hs300 <- dbGetQuery(conwind, query)
hs300[,1] <- as.Date(hs300[,1],format="%Y%m%d")

query <- 'select TRADE_DT, S_DQ_CLOSE from AIndexWindIndustriesEOD
where S_INFO_WINDCODE =\'885005.WI\'
and TRADE_DT >= \'20031231\' order by TRADE_DT;'
bondidx <- dbGetQuery(conwind, query)
bondidx[,1] <- as.Date(bondidx[,1],format="%Y%m%d")
benchmark <- merge(hs300,bondidx,by.x = 'TRADE_DT',by.y = 'TRADE_DT')

query <- 'select TRADE_DT, S_DQ_CLOSE from AIndexWindIndustriesEOD
where S_INFO_WINDCODE =\'885009.WI\'
and TRADE_DT >= \'20031231\' order by TRADE_DT;'
moneyidx <- dbGetQuery(conwind, query)
moneyidx[,1] <- as.Date(moneyidx[,1],format="%Y%m%d")
benchmark <- merge(benchmark,moneyidx,by.x = 'TRADE_DT',by.y = 'TRADE_DT')

colnames(benchmark) <- c('date','hs300','bondfund','moneyfund')
write.csv(benchmark,file = '.\\Data\\fundindex20180508.csv',row.names = F)

# money --------------------------
# query <- 'select TRADE_DT, S_DQ_CLOSE from aindexeodprices
# where S_INFO_WINDCODE =\'000300.SH\'
# and TRADE_DT >= \'20031231\' order by TRADE_DT;'
# hs300 <- dbGetQuery(conwind, query)
# hs300[,1] <- as.Date(hs300[,1],format="%Y%m%d")

query <- 'select PRICE_DATE,F_NAV_ADJUSTED from chinamutualfundnav
where F_INFO_WINDCODE = \'110030.OF\' order by PRICE_DATE'
stkfund <- dbGetQuery(conwind, query)
stkfund[,1] <- as.Date(stkfund[,1],format="%Y%m%d")

query <- 'select PRICE_DATE,F_NAV_ADJUSTED from chinamutualfundnav
where F_INFO_WINDCODE = \'110006.OF\' order by PRICE_DATE'
moneyidx <- dbGetQuery(conwind, query)
moneyidx[,1] <- as.Date(moneyidx[,1],format="%Y%m%d")
benchmark <- merge(stkfund,moneyidx,by.x = 'PRICE_DATE',by.y = 'PRICE_DATE')


colnames(benchmark) <- c('date','stkfund','moneyfund')
write.csv(benchmark,file = '.\\Data\\moneyandstk20180602.csv',row.names = F)
