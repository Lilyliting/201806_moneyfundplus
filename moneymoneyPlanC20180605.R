# initial settings -----------------

setwd('C:\\Users\\Administrator\\Desktop\\Moneyboost_20180601')

library(xts)
library(RMySQL)
library(quantmod)
library(rJava)
library(xlsx)
library(lubridate)
library(reshape2)
library(PerformanceAnalytics)
library(parallel)
library(RcppRoll)

# teststa <- '2009-03-01'
# testend <- '2018-05-29'

# teststa <- '2010-11-01'
# testend <- '2014-07-01'

teststa <- '2009-03-01'
testend <- '2018-05-29'

cmsn <- c(0.008,0.000) # commission
usernum <- 9

# conwind <- dbConnect(MySQL(), host='rm-2zey1z6px42nits51.mysql.rds.aliyuncs.com',
#                      port=3306,dbname="wind", username="hulitingali", password="Hu.lt@2018")

# read csv files -----------------
fundidx <- read.csv('.\\Data\\moneyandindex20180602.csv')
fundidx[,1] <- as.Date(fundidx[,1])
fundidx <- xts(fundidx[,-1],fundidx[,1])
fundidx <- fundidx[paste0((as.Date(teststa)-1),'/',testend)]
tradedays <- as.Date(.indexDate(fundidx[,1]))

# timing <- read.csv('.\\Data\\wei.csv')
# timing <- read.csv('.\\Data\\gftdtimimg.csv')
timing <- read.csv('.\\Data\\keep.csv')
timing <- timing[,-3]
timing[,1] <- as.Date(timing[,1])

timingxts <- xts(timing[,-1],timing[,1])


df <- data.frame('date' = tradedays)
df$year <- year(df$date)
df$month <- month(df$date)
df$day <- day(df$date)
df <- aggregate(df$date,by=list(df$year,df$month),FUN = max)
# poschangedate = sort(df[df$Group.2 %% 3==0,]$x) # 调仓时间点
poschangedate = sort(df$x) # 调仓时间点

fundpct <- diff(fundidx)/lag(fundidx)
fundpct[is.na(fundpct)] <- 0

# HS300pct <- diff(HS300xts)/lag(HS300xts)
# HS300pct <- HS300pct[paste(investstart,investend,'/')]
# HS300pct[is.na(HS300pct)] <- 0

hist(fundpct[,2],nclass=50)

# virtual users -----------------

userdbgen <- function(usernum,tradedaysf=tradedays,safep=3,yearn=1) {
    timelen <- length(tradedaysf)
    userdb <- as.data.frame(matrix(ncol = 9,nrow = usernum))
    # userdb <- as.data.frame(matrix(ncol = 6,nrow = 1))
    colnames(userdb) <- c('userid',
                          'starttime',
                          'endtime',
                          'datediff',
                          'investmoney',
                          'pstop', # profit limit (x months)
                          'lstop', # loss limit (x %)
                          'safeprofit',
                          'maxstkpos')
    
    # userdb[,1] <- seq(1, usernum)
    # sta <- sample(timelen-yearn*240,usernum,replace = T)
    # # userdb[,2] <- tradedaysf[2]
    # # userdb[,3] <- tradedaysf[length(tradedaysf)]
    # userdb[,2] <- tradedaysf[sta+1]
    # userdb[,3] <- tradedaysf[sapply((timelen-sta-yearn*238-1),sample,size=1)+sta+yearn*238]
    # userdb[,4] <- as.numeric(difftime(userdb$endtime,userdb$starttime,units = 'days'))
    # userdb[,5] <- sample(seq(1e5,5e5,by=1e5),usernum,replace = T)
    # userdb[,6] <- 100
    # userdb[,7] <- sample(seq(2,10,by=2),usernum,replace = T)
    # userdb[,8] <- sample(seq(0,3),usernum,replace = T)
    # userdb[,9] <- sample(seq(0.1,0.5,by=0.1),usernum,replace = T)
    
    userdb[,1] <- seq(1, usernum)
    # sta <- sample(timelen-yearn*240,usernum,replace = T)
    userdb[,2] <- tradedaysf[2]
    # userdb[,3] <- tradedaysf[sapply((timelen-sta-yearn*238-1),sample,size=1)+sta+yearn*238]
    userdb[,3] <- tradedaysf[length(tradedaysf)]
    userdb[,4] <- as.numeric(difftime(userdb$endtime,userdb$starttime,units = 'days'))
    userdb[,5] <- 5e5
    userdb[,6] <- 100
    userdb[,7] <- seq(5,15,by=5)
    userdb[,8] <- c(rep(0,3),rep(1,3),rep(2,3))
    userdb[,9] <- 1
    return(userdb)
}

userdb <- userdbgen(usernum)



# invest function ----------------------

# statime <- tradedays[1]
# endtime <- tradedays[1000]
# maxstkpos <- 0.5
# investp <- 'weeks'
# timingts=timingxts

periodlyinvest <- function(kk, #用户数据
                           userdbf=userdb,
                           # statime=statime, 
                           # endtime=endtime,  
                           # maxstkpos=maxstkpos,
                           # investp=investp
                           
                           timingts=timingxts, # 默认数据
                           fundpctf=fundpct,
                           poschangedatef=poschangedate,
                           cmsnf=cmsn) {
    # kk=1
    # userdbf=userdb
    # timingts=timingxts # 默认数据
    # fundpctf=fundpct
    # poschangedatef=poschangedate
    # cmsnf=cmsn
    
    library(xts)
    library(lubridate)
    library(PerformanceAnalytics)
    library(quantmod)
    # library(RMySQL)
    stkidx=1
    moneyidx=2
    
    # conwind <- dbConnect(MySQL(), host='rm-2zey1z6px42nits51.mysql.rds.aliyuncs.com',
    #                      port=3306,dbname="wind", username="hulitingali", password="Hu.lt@2018")
    
    statime <- userdbf[kk,2]
    endtime <- userdbf[kk,3]
    datediff <- userdbf[kk,4]
    investmoney <- userdbf[kk,5]
    pstop <- userdbf[kk,6]
    lstop <- userdbf[kk,7]
    safeprofit <- userdbf[kk,8]
    maxstkpos <- userdbf[kk,9]
    
    # 定投时间区间内数据
    fundpctts0 <- fundpctf[paste0(statime,'/',endtime)]
    timingts0 <- timingts[paste0(statime,'/',endtime)]
    # if (minstkpos==1) {timingts0[] <- 1}
    dateseq <- date(timingts0)
    
    safeline <- timingts0
    safeline[1,1] <- investmoney
    safeline[-1,1] <- investmoney*cumprod(1+as.numeric(diff(dateseq-dateseq[1]))/365.25*safeprofit/100)
    
    # 安全垫终止日
    # df <- data.frame('date' = date(timingts0))
    # df$x <- year(df$date)
    # df$y <- month(df$date)
    # df$z <- day(df$date)
    # safem <- df[1,3] + 6
    # safey <- df[1,2] + safem %/% 12
    # safem <- safem %% 12
    # safed <- min(df[1,4],26)
    # temp <- subset(df,df$x>=safey & df$y>=safem & df$z>=safed,select = 'date')
    # safedate <- as.Date(temp[1,1])
    
    
    # invest begin
    moneyonstk <- timingts0
    moneyonstk[] <- NaN
    moneyonmoney <- moneyonstk
    # moneyonbase <- moneyonstk
    
    # safe period
    timingts0[1] = 0
    
    pos11 <- timingts0[1,]
    moneyonstk[1] <- investmoney*pos11/(1+cmsnf[1])
    moneyonmoney[1] <- investmoney*(1-pos11)/(1+cmsnf[2])
    # moneyonbase[1] <- startmoney/(1+cmsnf[2])
    
    
    record <- data.frame()
    
    # costs <- moneyonstk 
    # costs[investidx,] <- seq(1,length.out = length(investidx))
    # costs <- na.locf(costs)
    # costs <- length(investidx)*cyclemoney
    
    poschangecnt <- 0
    pospp <- as.numeric(pos11)
    
    climax <- as.numeric(moneyonstk[1]) # 记录极值点
    
    for (ii in 2:length(timingts0)) { #length(timingts0)
        moneyonstk[ii] <- as.numeric(moneyonstk[ii-1])*(fundpctts0[ii,stkidx]+1)
        moneyonmoney[ii] <- as.numeric(moneyonmoney[ii-1])*(fundpctts0[ii,moneyidx]+1)
        # moneyonbase[ii] <- as.numeric(moneyonbase[ii-1])*(fundpctts0[ii,moneyidx]+1)
        
        totm <- moneyonstk[ii]+moneyonmoney[ii] #+moneyonbase[ii]
        
        # if (date(timingts0[ii])>safedate && as.numeric(moneyonbase[ii])!=0) {
        #     moneyonmoney[ii] <- moneyonmoney[ii]+moneyonbase[ii]
        #     moneyonbase[ii] <- 0
        # }
        if ((moneyonstk[ii] < climax*(1-lstop/100)||(totm-as.numeric(safeline[ii]))<0) #
            && as.numeric(moneyonstk[ii])>0) {
            # position reduced by loss stop
            timingts0[1:ii,] <- 1
            zeropos <- which(timingts0==0)[1]
            if (!is.na(zeropos)) {
                timingts0[1:(zeropos-1)] <- 0
            } else {
                timingts0[] <- 0
            }
            
            recordii <- data.frame(date(timingts0[ii]),kk,0,0,'loss stop')
            colnames(recordii) <- c('date','scenario','sig','pos','reason')
            record <- rbind(record,recordii)
            
            moslag <- moneyonstk[ii]
            moblag <- moneyonmoney[ii]
            stkposii <- timingts0[ii]
            moneyonstk[ii] <- stkposii*totm
            moneyonstk[ii] <- moneyonstk[ii]-max((moneyonstk[ii]-moslag),0)/(1+1/cmsnf[1])
            moneyonmoney[ii] <- ((1-stkposii)*totm)
            moneyonmoney[ii] <- moneyonmoney[ii]-max((moneyonmoney[ii]-moblag),0)/(1+1/cmsnf[2])
            poschangecnt <- poschangecnt+1
            pospp <- 0
            climax <- 0
        } else if (timingts0[ii]!=pospp && pospp == 1) {
            # position reduced by timing signals
            moneyonmoney[ii] <- moneyonstk[ii]+moneyonmoney[ii]
            moneyonstk[ii] <- 0
            poschangecnt <- poschangecnt+1
            pospp <- 1-pospp
            
            
            recordii <- data.frame(date(timingts0[ii]),kk,0,0,'timingsig')
            colnames(recordii) <- c('date','scenario','sig','pos','reason')
            record <- rbind(record,recordii)
            climax <- 0
            
        } else if (timingts0[ii]!=pospp && pospp == 0) {
            # position increased by timing signals
            
            # tempdate <- gsub('-','',as.character(dateseq[ii]))
            # query <- paste0('select F_INFO_YEARLYROE from cmoneymarketfincome 
            #         where ANN_DATE <= \'',tempdate,'\' and S_INFO_WINDCODE = \'110006.OF\' 
            #         order by ANN_DATE desc limit 0,10;')
            # tempdata <- dbGetQuery(conwind, query)
            # moneyyield <- mean(tempdata[,1])
            # moneyyield <- max(safeprofit,moneyyield)
            x <- totm*(3-safeprofit)/(lstop+3)
            if (min(x,totm*maxstkpos)<100 || (totm-as.numeric(safeline[ii])*(1+cmsnf[1]*1.5))<0) {next} # 
            moslag <- moneyonstk[ii]
            moblag <- moneyonmoney[ii]
            
            moneyonstk[ii] <- min(totm*(3-safeprofit)/(lstop+3),totm*maxstkpos)
            # print(moneyonstk[ii]/totm)
            # print(max((moneyonstk[ii]-moslag),0)/(1+1/cmsnf[1]))
            moneyonstk[ii] <- moneyonstk[ii]-max((moneyonstk[ii]-moslag),0)/(1+1/cmsnf[1])
            moneyonmoney[ii] <- totm-moneyonstk[ii]
            moneyonmoney[ii] <- moneyonmoney[ii]-max((moneyonmoney[ii]-moblag),0)/(1+1/cmsnf[2])
            
            poschangecnt <- poschangecnt+1
            pospp <- 1-pospp
            
            recordii <- data.frame(date(timingts0[ii]),kk,1,as.numeric(moneyonstk[ii]),'timingsig')
            colnames(recordii) <- c('date','scenario','sig','pos','reason')
            record <- rbind(record,recordii)
        }
        climax <- as.numeric(max(climax,moneyonstk[ii]))
    }
    totwealth <- moneyonstk+moneyonmoney #+moneyonbase
    
    colnames(totwealth) <- paste0('lstop',lstop,'_safe',safeprofit) #'boost_p',pstop,'_l',lstop,
    
    
    IRRmoney <- log(prod(1+fundpctts0[-1,2]))/datediff*365.25
    IRR <- as.numeric(log(totwealth[length(totwealth)]/investmoney)/datediff*365.25)
    
    mdd <- maxDrawdown(Delt(totwealth[,1]))
    sharpe <- SharpeRatio(Delt(totwealth[,1]),FUN = 'StdDev')
    
    result <- c(IRRmoney,IRR,mdd,sharpe,poschangecnt)
    print(result)
    # dbDisconnect(conwind)
    # return(result)
    # return(totwealth)
    return(record)
}

# poschange record

recorddf <- data.frame()
for (ii in 1:usernum) {
    print(ii)
    res <- periodlyinvest(ii)
    recorddf <- rbind(recorddf,res)
}
write.xlsx(recorddf,'moneyrecord.xlsx',row.names = F)



# safeline[1,1] <- 500000
# safeline[-1,1] <- 500000*cumprod(1+as.numeric(diff(dateseq-dateseq[1]))/365.25*2/100)
# resultdf <- cbind(resultdf,safeline)


resultdf <- fundidx[-1,2]
resultdf <- resultdf/as.numeric(resultdf[1])*500000

for (ii in 1:usernum) {
    print(ii)
    res <- periodlyinvest(ii)
    print(colnames(res))
    resultdf <- cbind(resultdf,res)
}
resultdf <- na.locf(resultdf)
write.xlsx(resultdf,'moneymoney.xlsx')



# ------------------------------- 
# cl <- makeCluster(4) # 初始化四核心集群
# jj <- 1:usernum
# wealthn <- parLapply(cl, jj, periodlyinvest,userdb,timingxts,fundpct,poschangedate,cmsn) #并行计算
# stopCluster(cl) # 关闭集群
# 
# 
# resultmtx <- matrix(unlist(wealthn),nrow = 5)
# userdb$IRRmoney <- resultmtx[1,]
# userdb$IRR <- resultmtx[2,]
# userdb$mdd <- resultmtx[3,]
# userdb$sharpe <- resultmtx[4,]
# userdb$poschangetime <- resultmtx[5,]
# userdb
# 
# 
# write.xlsx(userdb,'userdb20180604.xlsx',row.names = F)

# winr <- table(sign(userdb$totalwealth-userdb$targetmoney))
# winr
# winr[2]/usernum
# earnr <-table(sign(userdb$IRR))
# earnr
# earnr[2]/usernum
# write.csv(userdb,'userdb20180505.csv',row.names = F)




